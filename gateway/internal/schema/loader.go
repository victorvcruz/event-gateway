package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

type Loader struct {
	fsys     fs.FS
	root     string
	compiler *jsonschema.Compiler
	schemas  map[string]*jsonschema.Schema
	mu       sync.RWMutex
}

// NewLoader devolve um Loader que lê os schemas embarcados em
// schemas/*.json. Não há caminho externo: os schemas vão junto com o binário.
func NewLoader() *Loader {
	return newLoader(embeddedFS, embeddedRoot)
}

// NewLoaderFromFS existe para testes: aceita uma fs.FS sintética (por exemplo
// fstest.MapFS) para cobrir os caminhos de falha. Não use em produção.
func NewLoaderFromFS(fsys fs.FS, root string) *Loader {
	return newLoader(fsys, root)
}

func newLoader(fsys fs.FS, root string) *Loader {
	compiler := jsonschema.NewCompiler()
	compiler.Draft = jsonschema.Draft7
	return &Loader{
		fsys:     fsys,
		root:     root,
		compiler: compiler,
		schemas:  make(map[string]*jsonschema.Schema),
	}
}

// Load compila todos os *.json do embed e falha no primeiro inválido.
func (l *Loader) Load(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	entries, err := fs.ReadDir(l.fsys, l.root)
	if err != nil {
		return fmt.Errorf("read schemas fs at %q: %w", l.root, err)
	}

	loaded := make(map[string]*jsonschema.Schema, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		eventType := strings.TrimSuffix(name, ".json")

		if err := ctx.Err(); err != nil {
			return err
		}

		s, err := l.compileFile(eventType, l.root+"/"+name)
		if err != nil {
			return fmt.Errorf("schema load failed file=%s event_type=%s: %w", name, eventType, err)
		}
		loaded[eventType] = s
	}
	l.schemas = loaded
	return nil
}

func (l *Loader) compileFile(eventType, path string) (*jsonschema.Schema, error) {
	raw, err := fs.ReadFile(l.fsys, path)
	if err != nil {
		return nil, fmt.Errorf("read %q: %w", path, err)
	}
	var probe map[string]any
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	url := "file:///" + eventType + ".json"
	if err := l.compiler.AddResource(url, strings.NewReader(string(raw))); err != nil {
		return nil, fmt.Errorf("add resource: %w", err)
	}
	s, err := l.compiler.Compile(url)
	if err != nil {
		return nil, fmt.Errorf("compile: %w", err)
	}
	return s, nil
}

func (l *Loader) Validate(_ context.Context, eventType string, payload json.RawMessage) error {
	l.mu.RLock()
	s, ok := l.schemas[eventType]
	l.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%w for event_type: %s", ErrSchemaNotFound, eventType)
	}

	var doc any
	if err := json.Unmarshal(payload, &doc); err != nil {
		return fmt.Errorf("invalid JSON payload: %w", err)
	}
	if err := s.Validate(doc); err != nil {
		return err
	}
	return nil
}

// Garante em tempo de compilação que Loader implementa Validator.
var _ Validator = (*Loader)(nil)
