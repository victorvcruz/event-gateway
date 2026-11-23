package outbox

import "errors"

// ErrDuplicate é devolvido por Client.InsertPending quando o event_id já
// existe. Quem chama costuma traduzir para HTTP 409.
var ErrDuplicate = errors.New("outbox: duplicate event_id")

// ErrNonRetryable avisa o Worker que a falha do Sender é permanente (por
// exemplo, o subscriber respondeu 4xx diferente de 409). Em vez de somar
// retries até MaxRetries, o Worker manda a linha direto para DEAD pelo
// DeadLetterHook. Para carregar a causa, use
// fmt.Errorf("%w: <detalhe>", ErrNonRetryable).
var ErrNonRetryable = errors.New("outbox: non-retryable sender failure")
