# Event Gateway

Arquitetura de referência para sistemas orientados a eventos com **Sagas Coreografadas**, oferecendo governança e resiliência via um serviço central de infraestrutura.

> **Status:** PoC funcional. O Gateway e os cinco serviços de negócio implementam o fluxo feliz, compensações, idempotência e outbox transacional.

## O que é

Um **Event Gateway** — serviço HTTP independente que atua como plano de governança sem se tornar orquestrador. Centraliza:

- **Validação de contratos** via JSON Schema draft-07 consultado no Confluent Schema Registry.
- **Entrega garantida ao Kafka** via Transactional Outbox (`gateway_outbox`).
- **Dispatch aos serviços consumidores** via HTTP com retry, backoff exponencial e dead-letter.
- **Auditoria passiva** de cada saga em `event_log` — sem influenciar o fluxo.
- **Roteamento estático** por `event_type → [callback_urls]`.

Serviços de negócio são **HTTP puro** — não conhecem Kafka, Schema Registry ou uns aos outros. Toda comunicação passa pelo Gateway.

## A saga de exemplo (fluxo de pedido)

Cinco serviços encadeados por eventos, sem chamadas diretas:

```
Cliente → OrderService ──[order.created]──► Gateway ──► PaymentService
                                                    └──► InventoryService

PaymentService ──[payment.processed]──► Gateway ──► FulfillmentService
InventoryService ──[stock.reserved]──► Gateway ──► FulfillmentService

FulfillmentService (aguarda os dois) ──[order.fulfilled]──► Gateway ──► NotificationService
                                                               └──► OrderService
```

Compensações automáticas em caso de falha: `order.cancelled`, `stock.released`, `payment.refunded`.

## Estrutura atual do repositório

```
event-gateway/
├── infrastructure/
│   ├── docker-compose.yml           # stack local (Postgres, Kafka, Schema Registry, Prom/Grafana)
│   ├── gateway-config.yaml          # configuração completa do Event Gateway
│   └── prometheus/                  # config de scrape
├── shared/                           # contratos, logging, Postgres e outbox
├── gateway/                          # governança, Kafka e dispatch HTTP
├── order-service/                    # API pública e estado do pedido
├── payment-service/                  # simulador de cobrança e reembolso
├── inventory-service/                # estoque, reserva e liberação
├── fulfillment-service/              # correlação payment + stock
├── notification-service/             # sink terminal persistente
├── README.md
├── LICENSE
└── .gitignore
```

## Módulos

Monorepo Go multi-módulo unificado por `go.work`, com sete módulos independentes:

```
poc-services/
├── go.work
├── shared/                          # tipos e utilitários comuns
├── gateway/                         # o Event Gateway
├── order-service/                   # gerencia pedidos
├── payment-service/                 # cobrança e refund
├── inventory-service/               # reserva de estoque
├── fulfillment-service/             # correlaciona payment + stock
└── notification-service/            # notificação terminal (sink)
```

Cada serviço produtor possui três binários:

- `cmd/server/` — HTTP (`POST /events` no Gateway; `POST /dispatch` nos serviços)
- `cmd/worker/` — outbox relay (processo separado)
- `cmd/migrations/` — aplica migrations via `golang-migrate` e encerra

O NotificationService é um sink terminal e, portanto, não possui worker. Os testes em `tests/` cobrem fluxo feliz, compensações e eventos fora de ordem.

O Gateway também possui `cmd/schema-init/`, um job administrativo idempotente.
Ele publica os nove schemas versionados no Git antes do server e do worker
iniciarem. Os processos de runtime têm responsabilidade somente de leitura.

## Como subir a infraestrutura

**Pré-requisitos**: Docker + Docker Compose v2.

```bash
cd infrastructure

docker compose up -d --build
cd ..
make smoke-saga
```

O Compose define `DISPATCH_TOKEN=local-dev-dispatch-token` por padrão. Em outros
ambientes, defina um segredo próprio e igual no `gateway-worker` e nos cinco
serviços. O Gateway o envia em `X-Gateway-Token`; chamadas diretas sem o token
são rejeitadas. Além da autenticação, cada serviço repete a validação estrita do
payload antes de acessar o banco. Os healthchecks dos serviços fazem `Ping` no
PostgreSQL e só sinalizam prontidão após a migration e o banco estarem disponíveis.
O `/healthz` do Gateway exige simultaneamente PostgreSQL e Schema Registry.

## Governança de schemas

Os arquivos em `gateway/internal/schema/schemas/` são a fonte de autoria. O
`schema-init` registra cada contrato como `tcc-poc.<event_type>-value`, com
`schemaType=JSON` e política `FULL_TRANSITIVE`. Conteúdo idêntico não cria uma
nova versão; alterações são verificadas quanto à compatibilidade antes da
publicação.

`FULL_TRANSITIVE` é deliberadamente a única política aceita pela configuração
operacional dos nove contratos. Os modos `BACKWARD`, `FORWARD` e `FULL`
são exercitados somente pelo experimento `schemas-compat-test`; eles não são
opções de produção desta PoC.

```bash
make schemas-register
make schemas-list
make schemas-compat-test
```

Server e worker resolvem a versão mais recente e mantêm cache concorrente por
cinco minutos. Depois do TTL, uma falha no Registry é _fail-closed_: o HTTP
retorna `503 application/problem+json` com `Retry-After: 1`, enquanto o consumer
Kafka não confirma o offset. Não existe fallback para o schema embarcado.
Violações e subjects inexistentes retornam `422 application/problem+json`.

```json
{
  "type": "urn:event-gateway:error:schema_violation",
  "title": "Event schema validation failed",
  "status": 422,
  "code": "schema_violation",
  "detail": "payload does not conform to the registered contract",
  "event_type": "order.created"
}
```

No caminho Kafka, JSON malformado, envelope inválido, divergência entre tópico
e `event_type`, subject ausente e violação de contrato são persistidos em
`dead_letter`, incluindo bytes originais, tópico, partição, offset e código de
falha. A coordenada Kafka possui índice único, portanto replay não duplica DLQ.

## Testes de caos e resiliência

A suíte local cria um projeto Compose isolado, com portas alternativas e volumes descartáveis. Ela usa Toxiproxy para falhas HTTP e pause/stop/restart do Docker para Kafka, bancos e workers.

```bash
make chaos
make chaos-resilience
make chaos-crash
make chaos-scenario SCENARIO=lost-ack
make chaos-scenario SCENARIO=kafka-consumer-before-offset
make chaos-scenario SCENARIO=schema-registry-outage
CHAOS_KEEP=1 make chaos-scenario SCENARIO=kafka-outage
make chaos-diagnostics
make chaos-down
make chaos-harness-test
```

Pré-requisitos adicionais: `curl`, `python3` e Docker Compose v2. Cada espera tem deadline e as consultas PostgreSQL possuem timeout de conexão, de statement e do processo Docker. Em caso de falha, o runner imprime containers, logs e registros da saga antes de limpar o ambiente. Use `CHAOS_KEEP=1` para manter a stack e o último `order_id` para depuração; `make chaos-diagnostics ORDER_ID=<uuid>` também permite consultar uma saga específica. O limite `max_retries: 6` significa seis tentativas totais: a inicial e cinco retries após 1, 2, 4, 8 e 16 segundos.

A suíte possui duas camadas. `make chaos-resilience` injeta falhas de rede e indisponibilidade determinística com Toxiproxy e Docker. `make chaos-crash` compila os processos com a tag `chaos` e usa um controlador one-shot para encerrar o processo com código 137, sem executar `defer`, exatamente nos Seams entre `COMMIT` e resposta/ACK. Essa camada também valida commit de dispatch antes do offset Kafka, rewind real do consumer group, compensações interrompidas e dez pedidos full-stack competindo pela mesma unidade de estoque. `make chaos` executa as duas camadas na mesma stack e deixa dead-letter por último. Builds normais usam o Adapter no-op e não expõem o controlador ou capacidade de crash.

## Experimentos quantitativos do TCC

A stack `event-gateway-experiments` executa os experimentos A–D em portas
28xxx, com volumes descartáveis, Prometheus a cada segundo e k6 2.1.0. O
controle dual-write sem Outbox só existe nessa stack e não altera os processos
de produção.

```bash
make experiments-dry-run
make experiment EXPERIMENT=A
make experiments
make experiments-analyze RUN_ID=<run_id>
```

A coleta oficial usa cinco repetições de 100 amostras por cenário. O runner
registra manifesto reprodutível, amostras individuais e evidências k6,
Prometheus, SQL e logs; a promoção mantém somente resultados curados e os
checksums do raw. Consulte [experiments/README.md](experiments/README.md) para
metodologia, retomada, oráculos e limitações.

Uma coleta oficial só é aceita a partir de um commit limpo e quando commit,
digest, parâmetros, matriz de batches, evidências e cardinalidade passam na
promoção fail-closed. O dry-run corrigido A–D foi validado; a coleta oficial
5×100 deve ser executada novamente após o commit destas alterações.

## Endpoints

| Serviço             | URL                                 |
| ------------------- | ----------------------------------- |
| Event Gateway       | http://localhost:8080               |
| OrderService        | http://localhost:8081               |
| PaymentService      | http://localhost:8082               |
| InventoryService    | http://localhost:8083               |
| FulfillmentService  | http://localhost:8084               |
| NotificationService | http://localhost:8085               |
| Prometheus          | http://localhost:9099               |
| Grafana             | http://localhost:3000 (admin/admin) |
| Schema Registry     | http://localhost:8181               |

## Padrões arquiteturais implementados

| Padrão                         | Onde                                        | Garante                                             |
| ------------------------------ | ------------------------------------------- | --------------------------------------------------- |
| Transactional Outbox (nível 1) | tabela `outbox` em cada serviço             | evento não se perde se serviço cair após COMMIT     |
| Transactional Outbox (nível 2) | tabela `outbox` no banco do Gateway         | evento não se perde entre Gateway e Kafka           |
| Idempotent Consumer            | constraints naturais das tabelas de negócio | reentrega não duplica operação                      |
| Partition Key uniforme         | `correlation_id` no Kafka                   | ordenação dos eventos da mesma saga                 |
| Event Log                      | tabela `event_log` no Gateway               | timeline completa de cada saga por `correlation_id` |
| Dead Letter                    | tabela `dead_letter`                        | eventos que esgotaram retries ficam rastreáveis     |
| Schema Registry + cache TTL    | `schema-init` e `internal/schema`           | versão e compatibilidade centralizadas, fail-closed |

## Stack tecnológica

| Camada                 | Tecnologia                     |
| ---------------------- | ------------------------------ |
| Linguagem              | Go 1.25                        |
| Mensageria             | Apache Kafka (KRaft)           |
| Schema Registry        | Confluent Schema Registry      |
| Banco de dados         | PostgreSQL 16 (um por serviço) |
| Validação de contratos | JSON Schema (draft-07)         |
| Observabilidade        | Prometheus + Grafana           |
| Containerização        | Docker Compose                 |

## Licença

MIT — ver [LICENSE](LICENSE).
