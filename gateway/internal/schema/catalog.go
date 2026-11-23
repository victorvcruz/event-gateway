package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

type Catalog map[string]json.RawMessage

func LoadEmbeddedCatalog(ctx context.Context) (Catalog, error) {
	entries, err := fs.ReadDir(embeddedFS, embeddedRoot)
	if err != nil {
		return nil, fmt.Errorf("read embedded schema catalog: %w", err)
	}
	catalog := make(Catalog, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		eventType := strings.TrimSuffix(entry.Name(), ".json")
		raw, err := fs.ReadFile(embeddedFS, embeddedRoot+"/"+entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read embedded schema %s: %w", eventType, err)
		}
		if !json.Valid(raw) {
			return nil, fmt.Errorf("embedded schema %s is invalid JSON", eventType)
		}
		var document map[string]any
		if err := json.Unmarshal(raw, &document); err != nil {
			return nil, fmt.Errorf("decode embedded schema %s: %w", eventType, err)
		}
		if id, _ := document["$id"].(string); id != eventType {
			return nil, fmt.Errorf("embedded schema %s has $id %q", eventType, id)
		}
		if _, err := compileRegistrySchema(eventType, raw); err != nil {
			return nil, fmt.Errorf("compile embedded schema %s: %w", eventType, err)
		}
		catalog[eventType] = append(json.RawMessage(nil), raw...)
	}
	if len(catalog) == 0 {
		return nil, fmt.Errorf("embedded schema catalog is empty")
	}
	return catalog, nil
}

func (c Catalog) EventTypes() []string {
	types := make([]string, 0, len(c))
	for eventType := range c {
		types = append(types, eventType)
	}
	sort.Strings(types)
	return types
}
