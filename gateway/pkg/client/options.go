package client

import (
	"net/http"
	"time"
)

// HTTPDoer é o mínimo que o Client precisa. *http.Client atende, e também
// qualquer transporte customizado ou mock.
type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

type options struct {
	doer    HTTPDoer
	timeout time.Duration
}

type Option func(*options)

// WithHTTPClient troca o cliente HTTP. O padrão é um &http.Client com o
// timeout de WithTimeout, ou 5s.
func WithHTTPClient(d HTTPDoer) Option {
	return func(o *options) { o.doer = d }
}

// WithTimeout define o timeout de cada chamada a PublishEvent. O padrão é 5s.
func WithTimeout(d time.Duration) Option {
	return func(o *options) { o.timeout = d }
}
