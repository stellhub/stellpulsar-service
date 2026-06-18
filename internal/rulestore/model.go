package rulestore

import (
	"encoding/json"
	"strconv"
	"time"
)

type Snapshot struct {
	Version     int64
	Checksum    string
	GeneratedAt time.Time
	Apps        map[string]Application
}

type Application struct {
	Code     string
	Version  int64
	Checksum string
	Configs  map[string]Config
	Rules    map[string]Rule
}

type Config struct {
	ApplicationCode   string
	ConfigID          string
	RuleName          string
	RuleType          string
	Content           json.RawMessage
	ContentChecksum   string
	AggregateChecksum string
	RuleCount         int
}

type Rule struct {
	ApplicationCode string
	ConfigID        string
	RuleID          string
	Name            string
	SnapshotVersion int64
	Checksum        string
	SchemaVersion   string
	Algorithm       string
	Quota           int64
	WindowSeconds   int64
	Burst           int64
	Dimensions      []string
	FailPolicy      string
	Attributes      map[string]string
	Raw             json.RawMessage
}

func EmptySnapshot(now time.Time) Snapshot {
	return Snapshot{
		Version:     0,
		GeneratedAt: now.UTC(),
		Apps:        map[string]Application{},
	}
}

func (s Snapshot) Empty() bool {
	return s.Version == 0 && s.Checksum == "" && len(s.Apps) == 0
}

func (s Snapshot) Clone() Snapshot {
	cloned := Snapshot{
		Version:     s.Version,
		Checksum:    s.Checksum,
		GeneratedAt: s.GeneratedAt,
		Apps:        make(map[string]Application, len(s.Apps)),
	}
	for code, app := range s.Apps {
		cloned.Apps[code] = app.Clone()
	}
	return cloned
}

func (a Application) Clone() Application {
	cloned := Application{
		Code:     a.Code,
		Version:  a.Version,
		Checksum: a.Checksum,
		Configs:  make(map[string]Config, len(a.Configs)),
		Rules:    make(map[string]Rule, len(a.Rules)),
	}
	for id, cfg := range a.Configs {
		cloned.Configs[id] = cfg.Clone()
	}
	for id, rule := range a.Rules {
		cloned.Rules[id] = rule.Clone()
	}
	return cloned
}

func (c Config) Clone() Config {
	cloned := c
	if c.Content != nil {
		cloned.Content = append(json.RawMessage(nil), c.Content...)
	}
	return cloned
}

func (r Rule) Clone() Rule {
	cloned := r
	if r.Dimensions != nil {
		cloned.Dimensions = append([]string(nil), r.Dimensions...)
	}
	if r.Attributes != nil {
		cloned.Attributes = make(map[string]string, len(r.Attributes))
		for key, value := range r.Attributes {
			cloned.Attributes[key] = value
		}
	}
	if r.Raw != nil {
		cloned.Raw = append(json.RawMessage(nil), r.Raw...)
	}
	return cloned
}

func (r Rule) Revision() string {
	if r.SnapshotVersion == 0 {
		return ""
	}
	return strconv.FormatInt(r.SnapshotVersion, 10)
}
