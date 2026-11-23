package schema

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
)

type ProvisionResult struct {
	EventType string
	Subject   string
	ID        int
	Version   int
	Created   bool
}

type Provisioner struct {
	client        *RegistryClient
	groupName     string
	compatibility string
}

func NewProvisioner(client *RegistryClient, groupName, compatibility string) (*Provisioner, error) {
	if client == nil {
		return nil, fmt.Errorf("schema registry client is required")
	}
	if strings.TrimSpace(groupName) == "" {
		return nil, fmt.Errorf("schema registry group name is required")
	}
	if compatibility != "FULL_TRANSITIVE" {
		return nil, fmt.Errorf("unsupported compatibility %q", compatibility)
	}
	return &Provisioner{client: client, groupName: groupName, compatibility: compatibility}, nil
}

func (p *Provisioner) Provision(ctx context.Context, catalog Catalog, configuredEventTypes []string) ([]ProvisionResult, error) {
	if err := assertCatalogMatches(catalog, configuredEventTypes); err != nil {
		return nil, err
	}
	results := make([]ProvisionResult, 0, len(catalog))
	for _, eventType := range catalog.EventTypes() {
		subject := p.groupName + "." + eventType + "-value"
		latest, err := p.client.Latest(ctx, subject)
		if errors.Is(err, ErrSchemaNotFound) {
			id, registerErr := p.client.Register(ctx, subject, catalog[eventType])
			if registerErr != nil {
				return nil, fmt.Errorf("register initial schema %s: %w", subject, registerErr)
			}
			if configErr := p.client.SetCompatibility(ctx, subject, p.compatibility); configErr != nil {
				return nil, fmt.Errorf("configure compatibility %s: %w", subject, configErr)
			}
			latest, err = p.client.Latest(ctx, subject)
			if err != nil {
				return nil, fmt.Errorf("read registered schema %s: %w", subject, err)
			}
			results = append(results, ProvisionResult{EventType: eventType, Subject: subject, ID: id, Version: latest.Version, Created: true})
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read latest schema %s: %w", subject, err)
		}
		if err := p.client.SetCompatibility(ctx, subject, p.compatibility); err != nil {
			return nil, fmt.Errorf("configure compatibility %s: %w", subject, err)
		}
		if sameJSON(latest.Schema, catalog[eventType]) {
			results = append(results, ProvisionResult{EventType: eventType, Subject: subject, ID: latest.ID, Version: latest.Version})
			continue
		}
		compatible, messages, err := p.client.CheckCompatibility(ctx, subject, catalog[eventType])
		if err != nil {
			return nil, fmt.Errorf("check compatibility %s: %w", subject, err)
		}
		if !compatible {
			return nil, fmt.Errorf("%w: subject=%s reasons=%s", ErrIncompatibleSchema, subject, strings.Join(messages, "; "))
		}
		id, err := p.client.Register(ctx, subject, catalog[eventType])
		if err != nil {
			return nil, fmt.Errorf("register schema %s: %w", subject, err)
		}
		latest, err = p.client.Latest(ctx, subject)
		if err != nil {
			return nil, fmt.Errorf("read updated schema %s: %w", subject, err)
		}
		results = append(results, ProvisionResult{EventType: eventType, Subject: subject, ID: id, Version: latest.Version, Created: true})
	}
	return results, nil
}

func assertCatalogMatches(catalog Catalog, configuredEventTypes []string) error {
	want := append([]string(nil), configuredEventTypes...)
	sort.Strings(want)
	got := catalog.EventTypes()
	if len(want) != len(got) {
		return fmt.Errorf("schema catalog/config mismatch: catalog=%v configured=%v", got, want)
	}
	for i := range want {
		if want[i] != got[i] {
			return fmt.Errorf("schema catalog/config mismatch: catalog=%v configured=%v", got, want)
		}
	}
	return nil
}
