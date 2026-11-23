//go:build chaos

package failpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"time"

	"go.uber.org/zap"
)

type httpProbe struct {
	processID string
	endpoint  string
	client    *http.Client
	log       *zap.Logger
	kill      func() error
}

type hitRequest struct {
	Process string `json:"process"`
	Checkpoint
}

type hitResponse struct {
	Action string `json:"action"`
}

func newRuntime(processID string, log *zap.Logger) Probe {
	return &httpProbe{
		processID: processID,
		endpoint:  strings.TrimRight(os.Getenv("CHAOS_CONTROLLER_URL"), "/"),
		client:    &http.Client{Timeout: 2 * time.Second},
		log:       log,
		kill: func() error {
			// 137 é o código que o container mostra após um SIGKILL. Usa-se
			// os.Exit porque runtimes sem root podem recusar kill(2), e ele
			// mantém o que importa num crash: nenhum defer roda.
			os.Exit(137)
			return nil
		},
	}
}

func (p *httpProbe) Hit(ctx context.Context, checkpoint Checkpoint) {
	if p.endpoint == "" || checkpoint.Point == "" {
		return
	}
	body, err := json.Marshal(hitRequest{Process: p.processID, Checkpoint: checkpoint})
	if err != nil {
		p.log.Warn("chaos failpoint encode failed", zap.Error(err))
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint+"/hit", bytes.NewReader(body))
	if err != nil {
		p.log.Warn("chaos failpoint request failed", zap.Error(err))
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		p.log.Warn("chaos failpoint controller unavailable", zap.Error(err))
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		p.log.Warn("chaos failpoint controller rejected hit", zap.Int("status", resp.StatusCode))
		return
	}
	var decision hitResponse
	if err := json.NewDecoder(resp.Body).Decode(&decision); err != nil {
		p.log.Warn("chaos failpoint response invalid", zap.Error(err))
		return
	}
	if decision.Action != "kill" {
		return
	}
	p.log.Warn("chaos failpoint killing process",
		zap.String("process", p.processID),
		zap.String("point", checkpoint.Point),
		zap.String("correlation_id", checkpoint.CorrelationID))
	// Não dê flush aqui: o Sync de stdout/stderr pode travar no container e
	// tirar o crash da janela determinística que estamos testando.
	if err := p.kill(); err != nil {
		p.log.Error("chaos failpoint kill failed", zap.Error(err))
	}
}
