# Relatório experimental run-20260830T134049Z-bc4aacf8

## Método executado

- Commit: `57cedbfda73ccc9dc686b2bc24b8f5ff5eb3af4a`
- Árvore dirty: `False`
- Amostras por repetição: `100`
- Repetições: `5`
- Seed k6/estatística: `20260828`; bootstrap: `10000` reamostragens.
- Percentis calculados sobre amostras individuais; proporções usam Wilson 95%.

## Resultados

| Exp. | Cenário         | Variante |   n |  P50 ms |  P95 ms |  P99 ms |   Perda | Completude |
| ---- | --------------- | -------- | --: | ------: | ------: | ------: | ------: | ---------: |
| A    | loss            | direct   | 500 | 3009.81 | 3018.54 | 3031.00 | 100.00% |      0.00% |
| A    | loss            | outbox   | 500 |    9.20 |   14.71 |   15.64 |   0.00% |    100.00% |
| B    | overhead        | baseline | 500 |    0.78 |    3.27 |    5.23 |   0.00% |    100.00% |
| B    | overhead        | gateway  | 500 |    6.82 |   12.11 |   14.16 |   0.00% |    100.00% |
| B    | overhead        | schema   | 500 |    0.80 |    3.33 |    5.14 |   0.00% |    100.00% |
| C    | gateway-120     | outbox   | 500 |   10.02 |   15.37 |   19.74 |   0.00% |    100.00% |
| C    | gateway-30      | outbox   | 500 |   10.18 |   16.31 |   23.66 |   0.00% |    100.00% |
| C    | gateway-60      | outbox   | 500 |    8.34 |   14.08 |   16.02 |   0.00% |    100.00% |
| C    | kafka-120       | outbox   | 500 |    6.44 |   11.39 |   13.11 |   0.00% |    100.00% |
| C    | kafka-30        | outbox   | 500 |    8.43 |   15.42 |   17.46 |   0.00% |    100.00% |
| C    | kafka-60        | outbox   | 500 |    8.80 |   14.51 |   16.75 |   0.00% |    100.00% |
| D    | happy           | outbox   | 500 |    3.57 |    6.91 |    8.67 |   0.00% |    100.00% |
| D    | payment-failure | outbox   | 500 |    3.68 |    8.60 |    9.72 |   0.00% |    100.00% |
| D    | stock-failure   | outbox   | 500 |    3.79 |    9.45 |   14.87 |   0.00% |    100.00% |

## Decomposição do overhead

- Validação de schema sobre baseline: `0.009 ms` (`0.74%`).
- Persistência após validação: `5.951 ms` (`516.44%`).
- Overhead total sobre baseline: `5.960 ms` (`521.03%`).

## Recuperação

| Cenário     | MTTR médio s | MTTR P95 s | Total médio s |
| ----------- | -----------: | ---------: | ------------: |
| gateway-120 |        9.210 |      9.443 |        12.184 |
| gateway-30  |        9.326 |      9.457 |        12.245 |
| gateway-60  |        9.215 |      9.424 |        12.137 |
| kafka-120   |       13.827 |     14.309 |        26.511 |
| kafka-30    |       13.870 |     14.274 |        26.436 |
| kafka-60    |       13.826 |     14.357 |        26.413 |

## Verificação dos critérios

- Experimento B (Gateway sem erros, P95 < 250 ms, P99 < 500 ms): **PASS**.
- Experimento A (controle perde e Outbox não perde): **PASS**.
- Experimento D (completude integral): **PASS**.
- Experimento C (convergência e zero perda em todas as janelas): **PASS**.

## Critérios e limitações

Os resultados descrevem somente o host e as imagens registrados no manifesto. A execução é serial e não representa um benchmark universal de capacidade.
