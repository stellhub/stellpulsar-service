package runtimeapi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/stellhub/stellpulsar-service/internal/rulestore"
)

const runtimePath = "/api/stellorbit/runtime/distributed-rate-limits"

type Client struct {
	baseURL    string
	httpClient *http.Client
	authToken  string
}

type ClientOption func(*Client)

func WithBearerToken(token string) ClientOption {
	return func(c *Client) {
		c.authToken = strings.TrimSpace(token)
	}
}

type SnapshotPage struct {
	SnapshotVersion   int64
	Checksum          string
	GeneratedAt       time.Time
	TotalApplications int
	TotalRules        int
	Configs           Page[runtimeConfig]
}

type Delta struct {
	FromSnapshotVersion int64
	ToSnapshotVersion   int64
	FromChecksum        string
	ToChecksum          string
	GeneratedAt         time.Time
	ChangeCount         int
	Changes             []runtimeChange
}

type WatchEvent struct {
	EventID                string
	EventType              string
	CurrentSnapshotVersion int64
	LatestSnapshotVersion  int64
	LatestChecksum         string
	GeneratedAt            time.Time
	Delta                  *Delta
}

type Page[T any] struct {
	Content       []T   `json:"content"`
	Page          int   `json:"page"`
	Size          int   `json:"size"`
	TotalElements int64 `json:"totalElements"`
	TotalPages    int   `json:"totalPages"`
}

type runtimeSnapshotPage struct {
	SnapshotVersion   int64               `json:"snapshotVersion"`
	Checksum          string              `json:"checksum"`
	GeneratedAt       time.Time           `json:"generatedAt"`
	TotalApplications int                 `json:"totalApplications"`
	TotalRules        int                 `json:"totalRules"`
	Configs           Page[runtimeConfig] `json:"configs"`
}

type runtimeConfig struct {
	ApplicationCode   string          `json:"applicationCode"`
	ConfigID          string          `json:"configId"`
	RuleName          string          `json:"ruleName"`
	RuleType          string          `json:"ruleType"`
	Content           string          `json:"content"`
	Checksum          string          `json:"checksum"`
	AggregateChecksum string          `json:"aggregateChecksum"`
	ContentModel      json.RawMessage `json:"contentModel"`
	RuleCount         int             `json:"ruleCount"`
}

type runtimeDelta struct {
	FromSnapshotVersion int64           `json:"fromSnapshotVersion"`
	ToSnapshotVersion   int64           `json:"toSnapshotVersion"`
	FromChecksum        string          `json:"fromChecksum"`
	ToChecksum          string          `json:"toChecksum"`
	GeneratedAt         time.Time       `json:"generatedAt"`
	ChangeCount         int             `json:"changeCount"`
	Changes             []runtimeChange `json:"changes"`
}

type runtimeChange struct {
	Operation           string         `json:"operation"`
	FromSnapshotVersion int64          `json:"fromSnapshotVersion"`
	ToSnapshotVersion   int64          `json:"toSnapshotVersion"`
	ApplicationCode     string         `json:"applicationCode"`
	ConfigID            string         `json:"configId"`
	PreviousChecksum    string         `json:"previousChecksum"`
	CurrentChecksum     string         `json:"currentChecksum"`
	Config              *runtimeConfig `json:"config"`
}

type runtimeWatchEvent struct {
	EventID                string        `json:"eventId"`
	EventType              string        `json:"eventType"`
	CurrentSnapshotVersion int64         `json:"currentSnapshotVersion"`
	LatestSnapshotVersion  int64         `json:"latestSnapshotVersion"`
	LatestChecksum         string        `json:"latestChecksum"`
	GeneratedAt            time.Time     `json:"generatedAt"`
	Delta                  *runtimeDelta `json:"delta"`
}

func NewClient(baseURL string, httpClient *http.Client, options ...ClientOption) *Client {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL != "" && !strings.HasSuffix(baseURL, runtimePath) {
		baseURL += runtimePath
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 5 * time.Second}
	}
	client := &Client{
		baseURL:    baseURL,
		httpClient: httpClient,
	}
	for _, option := range options {
		if option != nil {
			option(client)
		}
	}
	return client
}

func (c *Client) Enabled() bool {
	return c != nil && c.baseURL != ""
}

func (c *Client) Snapshot(ctx context.Context, page int, size int) (SnapshotPage, error) {
	values := url.Values{}
	values.Set("page", strconv.Itoa(page))
	values.Set("size", strconv.Itoa(size))

	var payload runtimeSnapshotPage
	if err := c.getJSON(ctx, "/snapshot", values, &payload); err != nil {
		return SnapshotPage{}, err
	}
	return SnapshotPage{
		SnapshotVersion:   payload.SnapshotVersion,
		Checksum:          payload.Checksum,
		GeneratedAt:       payload.GeneratedAt,
		TotalApplications: payload.TotalApplications,
		TotalRules:        payload.TotalRules,
		Configs:           payload.Configs,
	}, nil
}

func (c *Client) Changes(ctx context.Context, currentVersion int64, currentChecksum string) (Delta, error) {
	values := url.Values{}
	values.Set("currentSnapshotVersion", strconv.FormatInt(currentVersion, 10))
	if strings.TrimSpace(currentChecksum) != "" {
		values.Set("currentChecksum", currentChecksum)
	}

	var payload runtimeDelta
	if err := c.getJSON(ctx, "/changes", values, &payload); err != nil {
		return Delta{}, err
	}
	return Delta{
		FromSnapshotVersion: payload.FromSnapshotVersion,
		ToSnapshotVersion:   payload.ToSnapshotVersion,
		FromChecksum:        payload.FromChecksum,
		ToChecksum:          payload.ToChecksum,
		GeneratedAt:         payload.GeneratedAt,
		ChangeCount:         payload.ChangeCount,
		Changes:             payload.Changes,
	}, nil
}

func (c *Client) Watch(ctx context.Context, currentVersion int64, currentChecksum string, handle func(WatchEvent) error) error {
	values := url.Values{}
	if currentVersion > 0 {
		values.Set("currentSnapshotVersion", strconv.FormatInt(currentVersion, 10))
	}
	if strings.TrimSpace(currentChecksum) != "" {
		values.Set("currentChecksum", currentChecksum)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint("/watch", values), nil)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "text/event-stream")
	c.setAuthHeader(request)
	response, err := c.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("stellorbit watch failed: status=%d body=%s", response.StatusCode, strings.TrimSpace(string(body)))
	}

	return scanSSE(response.Body, func(data []byte) error {
		if len(bytes.TrimSpace(data)) == 0 {
			return nil
		}
		var payload runtimeWatchEvent
		if err := json.Unmarshal(data, &payload); err != nil {
			return err
		}
		event := WatchEvent{
			EventID:                payload.EventID,
			EventType:              payload.EventType,
			CurrentSnapshotVersion: payload.CurrentSnapshotVersion,
			LatestSnapshotVersion:  payload.LatestSnapshotVersion,
			LatestChecksum:         payload.LatestChecksum,
			GeneratedAt:            payload.GeneratedAt,
		}
		if payload.Delta != nil {
			delta := Delta{
				FromSnapshotVersion: payload.Delta.FromSnapshotVersion,
				ToSnapshotVersion:   payload.Delta.ToSnapshotVersion,
				FromChecksum:        payload.Delta.FromChecksum,
				ToChecksum:          payload.Delta.ToChecksum,
				GeneratedAt:         payload.Delta.GeneratedAt,
				ChangeCount:         payload.Delta.ChangeCount,
				Changes:             payload.Delta.Changes,
			}
			event.Delta = &delta
		}
		return handle(event)
	})
}

func (c *Client) getJSON(ctx context.Context, path string, values url.Values, target any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint(path, values), nil)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/json")
	c.setAuthHeader(request)
	response, err := c.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("stellorbit request failed: path=%s status=%d body=%s", path, response.StatusCode, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(response.Body).Decode(target)
}

func (c *Client) setAuthHeader(request *http.Request) {
	if strings.TrimSpace(c.authToken) == "" {
		return
	}
	token := c.authToken
	if !strings.HasPrefix(strings.ToLower(token), "bearer ") {
		token = "Bearer " + token
	}
	request.Header.Set("Authorization", token)
}

func (c *Client) endpoint(path string, values url.Values) string {
	endpoint := c.baseURL + path
	if len(values) > 0 {
		endpoint += "?" + values.Encode()
	}
	return endpoint
}

func scanSSE(reader io.Reader, handle func([]byte) error) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 4096), 1024*1024)

	var data bytes.Buffer
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if err := handle(bytes.TrimSpace(data.Bytes())); err != nil {
				return err
			}
			data.Reset()
			continue
		}
		if strings.HasPrefix(line, "data:") {
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if data.Len() > 0 {
		return handle(bytes.TrimSpace(data.Bytes()))
	}
	return nil
}

func (c runtimeConfig) Published() rulestore.PublishedConfig {
	content := c.Content
	if strings.TrimSpace(content) == "" && len(c.ContentModel) > 0 {
		content = string(c.ContentModel)
	}
	return rulestore.PublishedConfig{
		ApplicationCode:   c.ApplicationCode,
		ConfigID:          c.ConfigID,
		RuleName:          c.RuleName,
		RuleType:          c.RuleType,
		Content:           content,
		ContentChecksum:   c.Checksum,
		AggregateChecksum: c.AggregateChecksum,
		RuleCount:         c.RuleCount,
	}
}

func (c runtimeChange) Published() rulestore.PublishedChange {
	var cfg *rulestore.PublishedConfig
	if c.Config != nil {
		published := c.Config.Published()
		cfg = &published
	}
	return rulestore.PublishedChange{
		Operation:           c.Operation,
		FromSnapshotVersion: c.FromSnapshotVersion,
		ToSnapshotVersion:   c.ToSnapshotVersion,
		ApplicationCode:     c.ApplicationCode,
		ConfigID:            c.ConfigID,
		PreviousChecksum:    c.PreviousChecksum,
		CurrentChecksum:     c.CurrentChecksum,
		Config:              cfg,
	}
}
