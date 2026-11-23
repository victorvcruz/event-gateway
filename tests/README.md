# tests/

Scripts curl para smoke manual do Event Gateway. Complementa os testes
automatizados em `gateway/internal/**/*_test.go` — aqui o objetivo é
reproduzir cenários a partir do host, com o output legível.

## Pré-requisitos

1. Rede docker-compose de pé:
   ```bash
   cd infrastructure && docker compose up -d
   ```
   Espere `gateway-server` ficar em `Started` e `gateway-migrations` em
   `Exited (0)`.

2. `/etc/hosts` com aliases (uma vez):
   ```
   127.0.0.1 gateway-db kafka schema-registry
   ```

3. Utilitários no PATH: `curl`, `python3`, `uuidgen`, `docker`.

## Layout

```
tests/
├── README.md
├── lib.sh                    utilidades compartilhadas (URLs, uuidgen wrap)
├── smoke.sh                  roda o happy path completo (POST + validações)
├── schema-compat.sh          demonstra modos de compatibilidade no Registry
├── chaos/                   resiliência, failpoints, Kafka rewind e contenção
├── requests/                 exemplos curl por cenário
│   ├── 01-order-created.sh   POST válido → 202
│   ├── 02-duplicate.sh       POST duplicado → 202 seguido de 409
│   ├── 03-invalid-payload.sh payload que não valida → 422
│   ├── 04-unknown-type.sh    event_type sem schema → 422
│   ├── 05-malformed.sh       JSON quebrado → 400
│   ├── metrics.sh            GET /metrics — imprime só as gateway_*
│   └── healthz.sh            GET /healthz
└── payloads/
    ├── order.created.json
    ├── payment.processed.json
    └── invalid-order.json
```

Todos os scripts em `requests/` são executáveis diretamente (`./requests/01-...sh`).

## Uso rápido

```bash
# happy path completo
./smoke.sh

# um cenário isolado
./requests/03-invalid-payload.sh

# BACKWARD, FORWARD, FULL e diferença FULL × FULL_TRANSITIVE
make schemas-compat-test
```

## Observação

Os scripts assumem `gateway-server` em `localhost:8080` e `gateway-db` em
`localhost:5438`. A UI do Kafka (Provectus) fica em `http://localhost:8180`.

## Suíte de caos

Execute da raiz com `make chaos`. Use `make chaos-resilience` para a camada de
Toxiproxy/restarts e `make chaos-crash` para crash windows instrumentados. O runner usa o projeto
`event-gateway-chaos`, portas `18080–18085`, bancos `15433–15438` e Toxiproxy
em `18474`, portanto pode coexistir com a stack de desenvolvimento. Para um
caso isolado, use `make chaos-scenario SCENARIO=gateway-outage` ou
`SCENARIO=payment-compensation-crash`.
`CHAOS_KEEP=1` preserva containers e volumes para `make chaos-diagnostics`.
O último `order_id` também é preservado entre processos; alternativamente use
`make chaos-diagnostics ORDER_ID=<uuid>`. `make chaos-harness-test` valida lock,
persistência diagnóstica e deadlines sem depender de uma stack Docker real.

O escopo atual cobre falhas determinísticas nas fronteiras, reinícios antes da
entrega e crash windows internos entre `COMMIT` e ACK por meio de failpoints
exclusivos do build `chaos`. A suíte também força replay de offsets do Kafka,
interrompe compensações antes do commit e executa concorrência full-stack pelo
mesmo SKU. Particionamento de rede real e pressão de CPU/memória permanecem para
uma fase posterior.

### Cenários de verificação (TCC, Fase B)

`make chaos-verification` executa dois cenários adicionais, também incluídos em
`make chaos`:

- `invalid-event-not-dispatched` — evento que viola o JSON Schema recebe 422 e
  não aparece na Outbox, no `event_log`, na `dispatch_outbox` nem nos bancos
  dos assinantes.
- `compensation-during-gateway-outage` — pedido recusado pelo pagamento criado
  com o Gateway indisponível; após o retorno, converge para `CANCELLED`, com
  estoque restaurado, um único `payment.failed` e nenhuma `dead_letter`.

Testes Go correspondentes (PostgreSQL real via testcontainers):
`TestPaymentCompensationIsAppliedOnce`, `TestInventoryCompensationIsAppliedOnce`,
`TestPaymentLateOrderCreatedAfterStockFailedIsNotCharged`,
`TestInventoryLateOrderCreatedAfterPaymentFailedReservesNothing`,
`TestFulfillmentRequiresPaymentAndReservationInAnyOrder`,
`TestSiblingSuccessSurvivesWorkerRestart`,
`TestDuplicateEventIDWithDifferentContentIsTreatedAsDuplicate` e
`TestSchemaValidationRunsBeforeDeduplication`.

