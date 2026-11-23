# Framework quantitativo A–D

Esta pasta contém a camada experimental do TCC. Ela é deliberadamente separada
dos processos de produção: o único controle inseguro é `experiment-control`,
presente exclusivamente em `docker-compose.experiments.yml`.

## Execução

Pré-requisitos: Docker Compose v2 e Python 3.9+. O k6 é executado pela imagem
fixa `grafana/k6:2.1.0`; não é necessária instalação local.

```bash
make experiments-dry-run
make experiment EXPERIMENT=A
make experiments
make experiments-analyze RUN_ID=<run_id>
make experiments-diagnostics RUN_ID=<run_id>
EXPERIMENT_KEEP=1 make experiment EXPERIMENT=C
```

`make experiments-dry-run` usa uma repetição, dez amostras, dois warm-ups e
outages de três segundos. `make experiments` executa a coleta oficial: cinco
repetições de 100 amostras, vinte warm-ups em B e outages de 30, 60 e 120
segundos em C. Uma execução interrompida pode continuar com
`make experiments RESUME=<run_id>` somente quando a interrupção ocorreu entre
batches concluídos. Se houver qualquer artefato k6/evidência de um batch ainda
não checkpointado, o resume é recusado e um novo `run_id` deve ser iniciado:
reexecutar IDs determinísticos mediria respostas idempotentes (`200`/`409`) em
vez das requisições originais.

A retomada só é aceita quando o digest da árvore fonte e todos os parâmetros do
protocolo coincidem com o manifesto imutável: experimento, amostras, repetições,
warm-ups, outages, seed e configuração do bootstrap. Depois de qualquer
alteração, deve-se iniciar outro `run_id`.

Cada batch possui um arquivo JSON canônico escrito atomicamente. O
`observations.csv` é uma projeção reconstruída desses arquivos após o
checkpoint; IDs e chaves k6 são determinísticos pela seed. Latência e status
HTTP são associados aos efeitos no banco por `sample_id` e `sample_index`, não
pela ordem de conclusão ou criação. Uma queda durante o batch preserva os
artefatos para diagnóstico, mas bloqueia sua reexecução sob o mesmo `run_id`;
somente batches já checkpointados são ignorados com segurança pelo resume.

A captura de SQL, Prometheus ou logs é obrigatória: qualquer falha encerra o
batch antes do checkpoint e invalida a retomada desse batch. O retorno do
Compose e o JSON/status do Prometheus são verificados antes da gravação. Os snapshots SQL
contêm as linhas de domínio, eventos, DLQ e Outboxes filtradas pela lista exata
de correlações do batch; a promoção também percorre seu conteúdo e rejeita
qualquer `order_id` ou `correlation_id` fora desse escopo.

O lock `/tmp/event-gateway-experiments.lock` impede duas mutações simultâneas
da stack. Todos os waits e processos externos possuem deadline. Por padrão, o
teardown remove containers e volumes mesmo quando um cenário falha;
`EXPERIMENT_KEEP=1` preserva a stack para inspeção.

O contexto Docker é protegido por `.dockerignore`: resultados experimentais,
metadados Git, PDFs e testes não são enviados aos builds dos serviços.

## Desenho dos experimentos

- **A — perda e consistência:** `POST /orders/direct` faz o dual-write inseguro
  (commit PostgreSQL seguido de publicação síncrona) e é comparado ao
  `POST /orders` com Outbox após `SIGKILL` no broker.
- **B — overhead:** baseline de decode, validação contra Registry aquecido e
  Gateway completo são medidos em ordem alternada. O fixture é
  `order.cancelled`, evitando iniciar sagas e mantendo contrato real.
- **C — MTTR:** Gateway e Kafka ficam indisponíveis por 30/60/120 segundos. O
  worker produtor é pausado durante a janela para preservar o evento além do
  orçamento operacional de retries; a suíte de caos cobre separadamente a
  exaustão para `DEAD`. MTTR é a diferença entre readiness recuperada e o
  último estado terminal, depois da drenagem de todas as Outboxes relacionadas.
  O runner comprova a indisponibilidade antes da carga e persiste início,
  confirmação, restauração, readiness e último terminal em cada observação.
- **D — completude:** happy path, pagamento recusado e estoque ausente são
  classificados por estados em todos os bancos, cardinalidade do `event_log`,
  compensações, DLQ e todas as Outboxes não-SENT. Estados `RELEASED` e
  `REFUNDED` exigem os eventos `stock.released` e `payment.refunded`.

Cada cenário usa `run_id`, SKU, chaves de idempotência e UUIDs exclusivos. As
repetições são serializadas para que variantes não disputem CPU entre si.

## Execução suplementar do Experimento C (sem pausar o worker)

`make experiments-c-extended` repete o Experimento C para as janelas de 60 e
120 s **sem pausar o worker reenviador** e com a política de tentativas
ampliada para 10 tentativas, espera exponencial a partir de 1 s e teto de 30 s
(cerca de 151 s de espera acumulada, maior que a janela de 120 s). A política
chega aos workers por `EXPERIMENT_RETRY_MAX_ATTEMPTS` e
`EXPERIMENT_RETRY_MAX_BACKOFF` (compose de experimentos); sem elas, os padrões
do código continuam valendo (6 tentativas, ≈31 s). A execução é sempre não
oficial (`--official` recusa `--retry-policy extended`), usa `run_id` próprio e
registra `retry_policy` no manifesto; os artefatos analisados ficam em `experiments/results/raw/<run_id>/analysis/` e são copiados para `experiments/results/supplementary/<run_id>/` para serem versionados (a execução citada na monografia é `run-20261121T211217Z-168fda1c`). Cada observação traz, em `notes`,
`retry_count_max`, `rows_retried` e `rows` do `order.created` na Outbox
responsável pela janela (a do *Order Service* na queda do Gateway; a do Gateway
na queda do Kafka).

## Dados e análise

Dados k6, snapshots Prometheus, SQL, logs e estado de resume ficam em
`experiments/results/raw/<run_id>/` e não entram no Git. O manifesto registra
commit, digest da árvore fonte (inclusive dirty), imagens, host, parâmetros,
seed e timestamps.

A promoção oficial copia apenas:

- `manifest.json`;
- `observations.csv`;
- `summary.json` e `summary.csv`;
- `report.md`;
- `raw-checksums.sha256`;
- `raw-evidence.tar.gz`, contendo os dados brutos necessários para verificar os
  checksums em um clone novo.

O analisador usa P50/P95/P99, média, desvio-padrão, bootstrap de média com a seed
registrada e 10.000 reamostragens e intervalo de Wilson de 95% para proporções. Os
resultados caracterizam o host registrado no manifesto; não são um benchmark
universal de capacidade.

Promoção oficial exige árvore limpa e correspondência exata entre o commit e o
digest do manifesto. Também valida `state.json`, matriz completa, cardinalidade,
batches canônicos, status HTTP, latência, flags/estado terminal, ordem e durações
da timeline C, identidade e métricas k6 e todas as evidências antes de criar a
pasta oficial. O protocolo oficial é fixo: `ALL`, 5 repetições, 100 amostras,
20 warm-ups, outages de 30/60/120 segundos, seed 20260828 e 10.000
reamostragens. Parâmetros reduzidos são aceitos somente em execuções não
oficiais.

Os checksums são relativos à raiz do archive e podem ser auditados assim:

```bash
mkdir /tmp/tcc-raw-audit
tar -xzf experiments/results/official/<run_id>/raw-evidence.tar.gz -C /tmp/tcc-raw-audit
cd /tmp/tcc-raw-audit
shasum -a 256 -c <repo>/experiments/results/official/<run_id>/raw-checksums.sha256
```
