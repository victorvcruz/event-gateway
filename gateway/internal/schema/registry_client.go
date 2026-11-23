package schema

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const maxRegistryResponse = 4 << 20

type RegistryObserver interface {
	ObserveRegistryRequest(operation, outcome string, elapsed time.Duration)
}

type nopRegistryObserver struct{}

func (nopRegistryObserver) ObserveRegistryRequest(string, string, time.Duration) {}

type RegistryClient struct {
	endpoint string
	http     *http.Client
	observer RegistryObserver
}

type RegistrySchema struct {
	Subject string
	ID      int
	Version int
	Type    string
	Schema  json.RawMessage
}

type registryError struct {
	ErrorCode int    `json:"error_code"`
	Message   string `json:"message"`
}

func NewRegistryClient(endpoint string, timeout time.Duration, observer RegistryObserver) (*RegistryClient, error) {
	endpoint = strings.TrimRight(strings.TrimSpace(endpoint), "/")
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("schema registry endpoint %q is invalid", endpoint)
	}
	if timeout <= 0 {
		return nil, errors.New("schema registry timeout must be positive")
	}
	if observer == nil {
		observer = nopRegistryObserver{}
	}
	return &RegistryClient{endpoint: endpoint, http: &http.Client{Timeout: timeout}, observer: observer}, nil
}

func (c *RegistryClient) Ping(ctx context.Context) error {
	return c.do(ctx, "ping", http.MethodGet, "/subjects", nil, nil)
}

func (c *RegistryClient) Subjects(ctx context.Context) ([]string, error) {
	var subjects []string
	if err := c.do(ctx, "subjects", http.MethodGet, "/subjects", nil, &subjects); err != nil {
		return nil, err
	}
	return subjects, nil
}

func (c *RegistryClient) Latest(ctx context.Context, subject string) (RegistrySchema, error) {
	var response struct {
		Subject    string `json:"subject"`
		ID         int    `json:"id"`
		Version    int    `json:"version"`
		SchemaType string `json:"schemaType"`
		Schema     string `json:"schema"`
	}
	path := "/subjects/" + url.PathEscape(subject) + "/versions/latest"
	if err := c.do(ctx, "latest", http.MethodGet, path, nil, &response); err != nil {
		return RegistrySchema{}, err
	}
	if response.SchemaType != "JSON" {
		return RegistrySchema{}, fmt.Errorf("%w: subject %s has schemaType %q, expected JSON", ErrRegistryUnavailable, subject, response.SchemaType)
	}
	if !json.Valid([]byte(response.Schema)) {
		return RegistrySchema{}, fmt.Errorf("%w: subject %s version %d contains invalid JSON schema", ErrRegistryUnavailable, subject, response.Version)
	}
	return RegistrySchema{Subject: response.Subject, ID: response.ID, Version: response.Version, Type: response.SchemaType, Schema: json.RawMessage(response.Schema)}, nil
}

func (c *RegistryClient) SetCompatibility(ctx context.Context, subject, level string) error {
	body := map[string]string{"compatibility": level}
	return c.do(ctx, "set_compatibility", http.MethodPut, "/config/"+url.PathEscape(subject), body, nil)
}

func (c *RegistryClient) Compatibility(ctx context.Context, subject string) (string, error) {
	var response struct {
		Level string `json:"compatibilityLevel"`
	}
	if err := c.do(ctx, "get_compatibility", http.MethodGet, "/config/"+url.PathEscape(subject), nil, &response); err != nil {
		return "", err
	}
	if response.Level == "" {
		return "", fmt.Errorf("%w: subject %s returned an empty compatibility level", ErrRegistryUnavailable, subject)
	}
	return response.Level, nil
}

func (c *RegistryClient) CheckCompatibility(ctx context.Context, subject string, schemaJSON json.RawMessage) (bool, []string, error) {
	return c.checkCompatibility(ctx, subject, "/versions?verbose=true", "check_compatibility", schemaJSON)
}

// CheckCompatibilityLatest serve aos experimentos com políticas não
// transitivas. O provisionamento de produção usa CheckCompatibility, que pede
// ao Registry para comparar com todo o histórico do subject.
func (c *RegistryClient) CheckCompatibilityLatest(ctx context.Context, subject string, schemaJSON json.RawMessage) (bool, []string, error) {
	return c.checkCompatibility(ctx, subject, "/versions/latest?verbose=true", "check_compatibility_latest", schemaJSON)
}

func (c *RegistryClient) checkCompatibility(ctx context.Context, subject, suffix, operation string, schemaJSON json.RawMessage) (bool, []string, error) {
	body := map[string]any{"schemaType": "JSON", "schema": string(schemaJSON)}
	var response struct {
		Compatible bool     `json:"is_compatible"`
		Messages   []string `json:"messages"`
	}
	// Este endpoint deixa a política do subject decidir contra quais versões
	// comparar. Com FULL_TRANSITIVE, é o histórico inteiro; usar
	// /versions/latest enfraqueceria isso sem avisar.
	path := "/compatibility/subjects/" + url.PathEscape(subject) + suffix
	if err := c.do(ctx, operation, http.MethodPost, path, body, &response); err != nil {
		return false, nil, err
	}
	return response.Compatible, response.Messages, nil
}

func (c *RegistryClient) Register(ctx context.Context, subject string, schemaJSON json.RawMessage) (int, error) {
	body := map[string]any{"schemaType": "JSON", "schema": string(schemaJSON)}
	var response struct {
		ID int `json:"id"`
	}
	if err := c.do(ctx, "register", http.MethodPost, "/subjects/"+url.PathEscape(subject)+"/versions", body, &response); err != nil {
		return 0, err
	}
	return response.ID, nil
}

func (c *RegistryClient) do(ctx context.Context, operation, method, path string, requestBody, responseBody any) error {
	var body io.Reader
	if requestBody != nil {
		raw, err := json.Marshal(requestBody)
		if err != nil {
			return fmt.Errorf("marshal schema registry request: %w", err)
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.endpoint+path, body)
	if err != nil {
		return fmt.Errorf("build schema registry request: %w", err)
	}
	if requestBody != nil {
		req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	}
	start := time.Now()
	resp, err := c.http.Do(req)
	if err != nil {
		c.observer.ObserveRegistryRequest(operation, "unavailable", time.Since(start))
		return fmt.Errorf("%w: %v", ErrRegistryUnavailable, err)
	}
	defer resp.Body.Close()
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, maxRegistryResponse))
	if readErr != nil {
		c.observer.ObserveRegistryRequest(operation, "unavailable", time.Since(start))
		return fmt.Errorf("%w: read response: %v", ErrRegistryUnavailable, readErr)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var apiErr registryError
		_ = json.Unmarshal(raw, &apiErr)
		message := strings.TrimSpace(apiErr.Message)
		if message == "" {
			message = strings.TrimSpace(string(raw))
		}
		switch {
		case resp.StatusCode == http.StatusNotFound:
			c.observer.ObserveRegistryRequest(operation, "not_found", time.Since(start))
			return fmt.Errorf("%w: %s", ErrSchemaNotFound, message)
		case resp.StatusCode == http.StatusConflict || resp.StatusCode == http.StatusUnprocessableEntity:
			c.observer.ObserveRegistryRequest(operation, "incompatible", time.Since(start))
			return fmt.Errorf("%w: %s", ErrIncompatibleSchema, message)
		case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
			c.observer.ObserveRegistryRequest(operation, "unavailable", time.Since(start))
			return fmt.Errorf("%w: status=%d message=%s", ErrRegistryUnavailable, resp.StatusCode, message)
		default:
			c.observer.ObserveRegistryRequest(operation, "error", time.Since(start))
			return fmt.Errorf("schema registry status=%d code=%d message=%s", resp.StatusCode, apiErr.ErrorCode, message)
		}
	}
	if responseBody != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, responseBody); err != nil {
			c.observer.ObserveRegistryRequest(operation, "unavailable", time.Since(start))
			return fmt.Errorf("%w: decode response: %v", ErrRegistryUnavailable, err)
		}
	}
	c.observer.ObserveRegistryRequest(operation, "ok", time.Since(start))
	return nil
}
