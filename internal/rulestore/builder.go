package rulestore

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var (
	ErrApplicationCodeRequired = errors.New("application code is required")
	ErrConfigIDRequired        = errors.New("config id is required")
)

type PublishedConfig struct {
	ApplicationCode   string
	ConfigID          string
	RuleName          string
	RuleType          string
	Content           string
	ContentChecksum   string
	AggregateChecksum string
	RuleCount         int
}

type PublishedChange struct {
	Operation           string
	FromSnapshotVersion int64
	ToSnapshotVersion   int64
	ApplicationCode     string
	ConfigID            string
	PreviousChecksum    string
	CurrentChecksum     string
	Config              *PublishedConfig
}

func BuildSnapshot(version int64, checksum string, generatedAt time.Time, configs []PublishedConfig) (Snapshot, error) {
	snapshot := Snapshot{
		Version:     version,
		Checksum:    checksum,
		GeneratedAt: generatedAt.UTC(),
		Apps:        map[string]Application{},
	}
	for _, cfg := range configs {
		if err := applyConfig(&snapshot, cfg); err != nil {
			return Snapshot{}, err
		}
	}
	return snapshot, nil
}

func ApplyChanges(current Snapshot, toVersion int64, toChecksum string, generatedAt time.Time, changes []PublishedChange) (Snapshot, error) {
	if toVersion <= current.Version {
		return Snapshot{}, fmt.Errorf("target snapshot version must advance: current=%d target=%d", current.Version, toVersion)
	}
	if strings.TrimSpace(toChecksum) == "" {
		return Snapshot{}, errors.New("target snapshot checksum is required")
	}
	next := current.Clone()
	next.Version = toVersion
	next.Checksum = toChecksum
	next.GeneratedAt = generatedAt.UTC()
	for _, change := range changes {
		if change.FromSnapshotVersion != 0 && change.FromSnapshotVersion != current.Version {
			return Snapshot{}, fmt.Errorf("change %s/%s has unexpected from version %d, want %d", change.ApplicationCode, change.ConfigID, change.FromSnapshotVersion, current.Version)
		}
		if change.ToSnapshotVersion != 0 && change.ToSnapshotVersion != toVersion {
			return Snapshot{}, fmt.Errorf("change %s/%s has unexpected to version %d, want %d", change.ApplicationCode, change.ConfigID, change.ToSnapshotVersion, toVersion)
		}
		operation := strings.ToUpper(strings.TrimSpace(change.Operation))
		switch operation {
		case "DELETE", "REMOVE", "DELETED":
			if err := validatePreviousChecksum(next, change); err != nil {
				return Snapshot{}, err
			}
			deleteConfig(&next, change.ApplicationCode, change.ConfigID)
		case "UPSERT", "UPDATE", "CREATE", "CREATED", "UPDATED":
			if change.Config == nil {
				return Snapshot{}, fmt.Errorf("change %s/%s has no config", change.ApplicationCode, change.ConfigID)
			}
			if err := applyConfig(&next, *change.Config); err != nil {
				return Snapshot{}, err
			}
			if err := validateCurrentChecksum(next, change); err != nil {
				return Snapshot{}, err
			}
		default:
			return Snapshot{}, fmt.Errorf("unsupported change operation %q", change.Operation)
		}
	}
	return next, nil
}

func applyConfig(snapshot *Snapshot, cfg PublishedConfig) error {
	cfg.ApplicationCode = strings.TrimSpace(cfg.ApplicationCode)
	cfg.ConfigID = strings.TrimSpace(cfg.ConfigID)
	if cfg.ApplicationCode == "" {
		return ErrApplicationCodeRequired
	}
	if cfg.ConfigID == "" {
		return ErrConfigIDRequired
	}

	app := snapshot.Apps[cfg.ApplicationCode]
	if app.Code == "" {
		app = Application{
			Code:    cfg.ApplicationCode,
			Configs: map[string]Config{},
			Rules:   map[string]Rule{},
		}
	}
	app.Version = snapshot.Version
	app.Checksum = snapshot.Checksum
	deleteRulesForConfig(app.Rules, cfg.ConfigID)

	content := json.RawMessage(strings.TrimSpace(cfg.Content))
	if len(content) == 0 {
		content = json.RawMessage("{}")
	}
	app.Configs[cfg.ConfigID] = Config{
		ApplicationCode:   cfg.ApplicationCode,
		ConfigID:          cfg.ConfigID,
		RuleName:          cfg.RuleName,
		RuleType:          cfg.RuleType,
		Content:           append(json.RawMessage(nil), content...),
		ContentChecksum:   chooseString(cfg.ContentChecksum, cfg.AggregateChecksum),
		AggregateChecksum: cfg.AggregateChecksum,
		RuleCount:         cfg.RuleCount,
	}

	rules, err := parseRules(cfg, snapshot.Version, content)
	if err != nil {
		return err
	}
	for _, rule := range rules {
		if existing, ok := app.Rules[rule.RuleID]; ok && existing.ConfigID != cfg.ConfigID {
			return fmt.Errorf("duplicate rule id %q in application %q: configs %q and %q", rule.RuleID, cfg.ApplicationCode, existing.ConfigID, cfg.ConfigID)
		}
		app.Rules[rule.RuleID] = rule
	}

	snapshot.Apps[cfg.ApplicationCode] = app
	return nil
}

func deleteConfig(snapshot *Snapshot, applicationCode, configID string) {
	app, ok := snapshot.Apps[applicationCode]
	if !ok {
		return
	}
	delete(app.Configs, configID)
	deleteRulesForConfig(app.Rules, configID)
	if len(app.Configs) == 0 && len(app.Rules) == 0 {
		delete(snapshot.Apps, applicationCode)
		return
	}
	app.Version = snapshot.Version
	app.Checksum = snapshot.Checksum
	snapshot.Apps[applicationCode] = app
}

func deleteRulesForConfig(rules map[string]Rule, configID string) {
	for id, rule := range rules {
		if rule.ConfigID == configID {
			delete(rules, id)
		}
	}
}

func parseRules(cfg PublishedConfig, snapshotVersion int64, content json.RawMessage) ([]Rule, error) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(content, &root); err != nil {
		return nil, fmt.Errorf("parse %s content: %w", cfg.ConfigID, err)
	}

	schemaVersion := rawString(root, "schemaVersion", "schema_version")
	rawRules := rawArray(root, "rules")
	if len(rawRules) == 0 {
		rawRules = rawArray(root, "limit", "limits", "rateLimits", "rate_limits", "distributedRateLimits")
	}
	if len(rawRules) == 0 {
		if cfg.RuleCount > 0 {
			return nil, fmt.Errorf("config %s declares %d rules but content has no rule array", cfg.ConfigID, cfg.RuleCount)
		}
		return nil, fmt.Errorf("config %s has no distributed rate limit rules", cfg.ConfigID)
	}

	rules := make([]Rule, 0, len(rawRules))
	for index, raw := range rawRules {
		var item map[string]json.RawMessage
		if err := json.Unmarshal(raw, &item); err != nil {
			return nil, fmt.Errorf("parse %s rule %d: %w", cfg.ConfigID, index, err)
		}
		ruleID := rawString(item, "ruleId", "rule_id", "id", "configId", "config_id")
		if ruleID == "" {
			return nil, fmt.Errorf("parse %s rule %d: rule id is required", cfg.ConfigID, index)
		}
		name := rawString(item, "name", "ruleName", "rule_name")
		if name == "" {
			name = cfg.RuleName
		}
		ruleSchema := rawString(item, "schemaVersion", "schema_version")
		if ruleSchema == "" {
			ruleSchema = schemaVersion
		}

		quota := rawInt64(item, "quota", "limit", "permits")
		if quota <= 0 {
			return nil, fmt.Errorf("parse %s rule %s: quota must be positive", cfg.ConfigID, ruleID)
		}
		windowSeconds := rawInt64(item, "windowSeconds", "window_seconds", "window")
		if windowSeconds <= 0 {
			return nil, fmt.Errorf("parse %s rule %s: window seconds must be positive", cfg.ConfigID, ruleID)
		}

		rules = append(rules, Rule{
			ApplicationCode: cfg.ApplicationCode,
			ConfigID:        cfg.ConfigID,
			RuleID:          ruleID,
			Name:            name,
			SnapshotVersion: snapshotVersion,
			Checksum:        chooseString(cfg.ContentChecksum, cfg.AggregateChecksum),
			SchemaVersion:   ruleSchema,
			Algorithm:       chooseString(rawString(item, "algorithm"), "fixed_window"),
			Quota:           quota,
			WindowSeconds:   windowSeconds,
			Burst:           rawInt64(item, "burst", "burstCapacity", "burst_capacity"),
			Dimensions:      rawStringArray(item, "dimensions"),
			FailPolicy:      chooseString(rawString(item, "failPolicy", "fail_policy"), "fail_closed"),
			Attributes:      rawStringMap(item, "attributes", "metadata"),
			Raw:             append(json.RawMessage(nil), raw...),
		})
	}
	return rules, nil
}

func validatePreviousChecksum(snapshot Snapshot, change PublishedChange) error {
	expected := strings.TrimSpace(change.PreviousChecksum)
	if expected == "" {
		return nil
	}
	app, ok := snapshot.Apps[strings.TrimSpace(change.ApplicationCode)]
	if !ok {
		return fmt.Errorf("change %s/%s previous checksum %q cannot be validated because application is missing", change.ApplicationCode, change.ConfigID, expected)
	}
	cfg, ok := app.Configs[strings.TrimSpace(change.ConfigID)]
	if !ok {
		return fmt.Errorf("change %s/%s previous checksum %q cannot be validated because config is missing", change.ApplicationCode, change.ConfigID, expected)
	}
	actual := chooseString(cfg.ContentChecksum, cfg.AggregateChecksum)
	if actual != expected {
		return fmt.Errorf("change %s/%s previous checksum mismatch: got %q want %q", change.ApplicationCode, change.ConfigID, actual, expected)
	}
	return nil
}

func validateCurrentChecksum(snapshot Snapshot, change PublishedChange) error {
	expected := strings.TrimSpace(change.CurrentChecksum)
	if expected == "" && change.Config != nil {
		expected = chooseString(change.Config.ContentChecksum, change.Config.AggregateChecksum)
	}
	if expected == "" {
		return nil
	}
	app, ok := snapshot.Apps[strings.TrimSpace(change.ApplicationCode)]
	if !ok {
		return fmt.Errorf("change %s/%s current checksum %q cannot be validated because application is missing", change.ApplicationCode, change.ConfigID, expected)
	}
	cfg, ok := app.Configs[strings.TrimSpace(change.ConfigID)]
	if !ok {
		return fmt.Errorf("change %s/%s current checksum %q cannot be validated because config is missing", change.ApplicationCode, change.ConfigID, expected)
	}
	actual := chooseString(cfg.ContentChecksum, cfg.AggregateChecksum)
	if actual != expected {
		return fmt.Errorf("change %s/%s current checksum mismatch: got %q want %q", change.ApplicationCode, change.ConfigID, actual, expected)
	}
	return nil
}

func rawString(root map[string]json.RawMessage, names ...string) string {
	for _, name := range names {
		raw, ok := root[name]
		if !ok {
			continue
		}
		var value string
		if err := json.Unmarshal(raw, &value); err == nil {
			return strings.TrimSpace(value)
		}
		var number json.Number
		if err := json.Unmarshal(raw, &number); err == nil {
			return number.String()
		}
	}
	return ""
}

func rawInt64(root map[string]json.RawMessage, names ...string) int64 {
	for _, name := range names {
		raw, ok := root[name]
		if !ok {
			continue
		}
		var integer int64
		if err := json.Unmarshal(raw, &integer); err == nil {
			return integer
		}
		var float float64
		if err := json.Unmarshal(raw, &float); err == nil {
			return int64(float)
		}
		var text string
		if err := json.Unmarshal(raw, &text); err == nil {
			parsed, _ := strconv.ParseInt(strings.TrimSpace(text), 10, 64)
			return parsed
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(raw, &object); err == nil {
			return rawInt64(object, "seconds", "value", "size")
		}
	}
	return 0
}

func rawArray(root map[string]json.RawMessage, names ...string) []json.RawMessage {
	for _, name := range names {
		raw, ok := root[name]
		if !ok {
			continue
		}
		var values []json.RawMessage
		if err := json.Unmarshal(raw, &values); err == nil {
			return values
		}
	}
	return nil
}

func rawStringArray(root map[string]json.RawMessage, names ...string) []string {
	for _, name := range names {
		raw, ok := root[name]
		if !ok {
			continue
		}
		var values []string
		if err := json.Unmarshal(raw, &values); err == nil {
			return values
		}
		var rawValues []json.RawMessage
		if err := json.Unmarshal(raw, &rawValues); err != nil {
			continue
		}
		values = make([]string, 0, len(rawValues))
		for _, rawValue := range rawValues {
			var value string
			if err := json.Unmarshal(rawValue, &value); err == nil {
				values = append(values, value)
			}
		}
		return values
	}
	return nil
}

func rawStringMap(root map[string]json.RawMessage, names ...string) map[string]string {
	for _, name := range names {
		raw, ok := root[name]
		if !ok {
			continue
		}
		var values map[string]string
		if err := json.Unmarshal(raw, &values); err == nil {
			return values
		}
		var generic map[string]any
		if err := json.Unmarshal(raw, &generic); err != nil {
			continue
		}
		values = make(map[string]string, len(generic))
		for key, value := range generic {
			values[key] = fmt.Sprint(value)
		}
		return values
	}
	return nil
}

func chooseString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
