# Relatório experimental run-20261121T211217Z-168fda1c

## Método executado

- Commit: `e7a9da06773341d928dcea0d9a4a8e3e012c1d93`
- Árvore dirty: `True`
- Amostras por repetição: `100`
- Repetições: `5`
- Seed k6/estatística: `20260828`; bootstrap: `10000` reamostragens.
- Percentis calculados sobre amostras individuais; proporções usam Wilson 95%.

## Resultados

| Exp. | Cenário | Variante | n | P50 ms | P95 ms | P99 ms | Perda | Completude |
|---|---|---|---:|---:|---:|---:|---:|---:|
| C | gateway-120 | outbox | 500 | 8.49 | 13.25 | 18.00 | 0.00% | 100.00% |
| C | gateway-60 | outbox | 500 | 9.32 | 15.28 | 20.15 | 0.00% | 100.00% |
| C | kafka-120 | outbox | 500 | 10.24 | 16.77 | 18.71 | 0.00% | 100.00% |
| C | kafka-60 | outbox | 500 | 8.79 | 18.28 | 28.57 | 0.00% | 100.00% |

## Decomposição do overhead


## Recuperação

| Cenário | MTTR médio s | MTTR P95 s | Total médio s |
|---|---:|---:|---:|
| gateway-120 | 14.340 | 14.533 | 17.365 |
| gateway-60 | 13.372 | 13.694 | 16.373 |
| kafka-120 | 6.263 | 7.084 | 11.608 |
| kafka-60 | 5.165 | 6.088 | 10.314 |

## Verificação dos critérios

- Experimento C (convergência e zero perda em todas as janelas): **PASS**.

## Critérios e limitações

Os resultados descrevem somente o host e as imagens registrados no manifesto. A execução é serial e não representa um benchmark universal de capacidade.
