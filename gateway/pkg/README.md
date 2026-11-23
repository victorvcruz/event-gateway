# `gateway/pkg` — SDK Go do Event Gateway

Superfície pública do módulo `gateway/` para os serviços de negócio. O SDK implementa em Go o contrato HTTP do gateway, com um cliente (`gateway/pkg/client`) e um handler para o lado receptor (`gateway/pkg/dispatcher`), para que os serviços não precisem escrever isso à mão.

- `POST /events` (produtor → gateway): `gateway/pkg/client`
- `POST /dispatch` (gateway → subscriber): `gateway/pkg/dispatcher`

O SDK não depende de `franz-go`, `pgx` nem de `internal/`; o `make verify-sdk-deps` confere isso.

---

## `gateway/pkg/client` — Publicar eventos ao Gateway

Cliente para `POST /events`. Nos serviços de negócio ele é adaptado pelo pacote `serviceoutbox`, que lê o envelope completo persistido na tabela `outbox`, classifica erros permanentes e aplica retry persistente aos erros transitórios.

```go
package main

import (
    "context"
    "time"

    "github.com/victorvcruz/event-gateway/gateway/pkg/client"
    "github.com/victorvcruz/event-gateway/gateway/pkg/serviceoutbox"
    "github.com/victorvcruz/event-gateway/shared/outbox"
)

func runWorker(ctx context.Context, cfg Config, store *outbox.Client) error {
    gw, err := client.New(cfg.GatewayURL, client.WithTimeout(5*time.Second))
    if err != nil {
        return err
    }
    sender, err := serviceoutbox.New(gw)
    if err != nil {
        return err
    }
    worker, err := outbox.NewWorker(store, sender, outbox.WorkerConfig{
        MaxAttempts: 5,
        BaseBackoff: time.Second,
    })
    if err != nil {
        return err
    }
    return worker.Run(ctx)
}
```

Ao criar um evento, use `serviceoutbox.NewRow`: o `payload` persistido é o envelope completo de `client.Event`, incluindo `event_id`, `event_type`, `correlation_id`, `timestamp` e o payload de domínio. Assim, retries e reinícios não regeneram identidade nem timestamp.

### Códigos vs resultado

| HTTP do Gateway | Resultado do `PublishEvent` |
|---|---|
| 202 Accepted | `PublishResult{Outcome: OutcomeAccepted}, nil` |
| 409 Conflict | `PublishResult{Outcome: OutcomeDuplicate}, nil` |
| 422 Unprocessable | `nil, &client.ValidationError{Body: <application/problem+json>}` (`errors.Is` → `ErrValidationFailed`) |
| 400 Bad Request | `nil, ErrBadRequest` |
| 5xx / timeout / erro rede / DNS | `nil, ErrGatewayUnavailable` |

Sem retry no cliente HTTP: uma tentativa por chamada. Retry pertence ao worker da outbox, que persiste `attempts`, `next_attempt_at` e `last_error`.

### Options

- `WithTimeout(d)` — timeout total de UMA chamada. Default `5s`.
- `WithHTTPClient(d)` — injeta cliente custom (útil em testes ou para políticas de retry específicas).

---

## `gateway/pkg/dispatcher` — Receber eventos do Gateway

Helper `http.Handler` para servir `POST /dispatch` no serviço. Decoda envelope, roteia por `event_type`, mapeia erro do handler em código HTTP.

```go
package main

import (
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "net/http"

    "github.com/victorvcruz/event-gateway/gateway/pkg/dispatcher"
    "go.uber.org/zap"
)

func run(logger *zap.Logger) error {
    d := dispatcher.New(dispatcher.WithLogger(logger))

    d.Handle("order.created", handleOrderCreated)
    d.Handle("payment.failed", handlePaymentFailed)

    mux := http.NewServeMux()
    mux.Handle("POST /dispatch", d)
    mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
        w.WriteHeader(200)
    })
    return http.ListenAndServe(":8080", mux)
}

func handleOrderCreated(ctx context.Context, evt dispatcher.Event) error {
    corrID := dispatcher.CorrelationIDFromContext(ctx) // propagar em eventos emitidos

    var body PaymentRequest
    if err := json.Unmarshal(evt.Payload, &body); err != nil {
        return fmt.Errorf("bad payload: %w", dispatcher.ErrBadRequest)
    }

    if err := repo.CreatePayment(ctx, tx, body); err != nil {
        if errors.Is(err, repo.ErrAlreadyExists) {
            return dispatcher.ErrDuplicate  // Gateway → 409 → SENT
        }
        return err  // Gateway → 500 → retry com backoff
    }

    repo.InsertOutbox(ctx, tx, "payment.processed", corrID, ...)
    return nil  // Gateway → 200 → SENT
}
```

### Códigos que o Dispatcher devolve

| Situação | HTTP |
|---|---|
| decode do envelope falha | 400 |
| `event_type` sem handler registrado | 200 (silencioso — Gateway avança offset) |
| handler retorna `nil` | 200 |
| handler retorna erro embrulhando `ErrDuplicate` | 409 (Gateway trata como sucesso, marca SENT) |
| handler retorna erro embrulhando `ErrBadRequest` | 400 (Gateway marca DEAD imediato) |
| handler retorna qualquer outro erro | 500 (Gateway retenta com backoff) |

### `CorrelationIDFromContext(ctx) string`

O Dispatcher injeta o valor de `X-Correlation-Id` no `ctx` recebido pelo handler. Use para propagar em novos eventos que o serviço emitir (mantém a saga inteira rastreável por um único id).

Fora do fluxo do `ServeHTTP` retorna string vazia — handler tolerante a testes que não injetam o header.

### Options

- `WithLogger(l)` — injeta `*zap.Logger`. Default `zap.NewNop()`.
- `WithAfterHandle(hook)` — executa um hook após sucesso do handler e antes do ACK HTTP. O hook deve retornar rapidamente.

---

## Guardrails

- `pkg/client` e `pkg/dispatcher` dependem APENAS de: biblioteca padrão, `github.com/google/uuid`, `go.uber.org/zap` (só no dispatcher). **Zero import de `franz-go`, `pgx` ou `internal/`.** O adaptador `pkg/serviceoutbox` integra deliberadamente o SDK ao módulo `shared/outbox`.
- A struct `dispatcher.Event = client.Event` é type alias — mudanças no envelope propagam automaticamente para handlers.
- SDK NÃO faz retry — retry de rede vive no `shared/httpx` (opt-in) e retry de alto nível vive no outbox local do serviço.
