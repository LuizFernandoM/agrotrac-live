# Mapa de implementação

O contrato aprovado está em [contracts.md](contracts.md). Este incremento implementa somente projeção e transporte local; sessões/autenticação reais e eventos de sessão continuam pendentes completos.

| Responsabilidade | Arquivo |
| --- | --- |
| Result, invariantes e envelope Core real | `src/domain/model.ts` |
| JSON estrito, UTF-8, duplicação de chaves, precisão | `src/domain/json.ts` |
| Portas sem dependência de frameworks | `src/application/ports.ts` |
| Ingestão, barreira local, readiness e coleta estável | `src/application/live.ts` |
| Lua single-HSET, geração, snapshots e dedup | `src/infra/redis.ts` |
| GET Core com limites, abort e timeout | `src/infra/core-http.ts` |
| Consumer, confirmação de quarentena e reconexão | `src/infra/rabbit.ts` |
| REST/WS, snapshot inicial, gap, heartbeat e buffers | `src/infra/server.ts` |
| Recuperação automática/rebuild e shutdown | `src/infra/runtime.ts` |
| Configuração env opt-in e bootstrap executável | `src/infra/config.ts`, `src/main.ts` |

Um hash Redis por geração contém campos `generation`, `revision`, `scan`, `s_count`, `w_count`, `s:<uuid>`, `w:<uuid>`, `v:s:<uuid>`, `v:w:<uuid>` e `d:<event_id>`. IDs de campos são o índice; contagens e pares snapshot/versão detectam membros ausentes/inconsistentes. Snapshots são strings canônicas para preservar inteiros de 53 bits sem reserialização de números por cjson Lua. O script valida todos os conflitos/tipos/revisões antes de um único HSET; um erro técnico inesperado invalida a geração, sem prometer rollback Lua. Dados de reserva não entram no hash: só digest SHA-256 canônico do envelope completo.

O runtime inicia com uma geração candidata vazia, conecta consumer e revalida o catálogo por duas coletas completas iguais. O ponteiro ativo é uma referência de processo único protegida pela barreira da aplicação e por revalidação de geração/fault na leitura, sem alegação de ponteiro distribuído. Após timeout Redis a conexão é descartada, não reutilizada com respostas tardias. Rebuild falho gera candidato novo no ciclo seguinte; as gerações antigas são preservadas. Reconciliação HTTP pode emitir update sem `event_id`; revisões são locais e não simulam eventos do Core.

O transporte WS mantém snapshot primeiro e fila limitada de updates recebidos durante o envio. Checkpoints com revisão Redis permitem ao cliente detectar o último broadcast perdido. O consumidor não espera cliente WS: send retorna promise própria, limitada por bytes/deadline. WS é não durável; reconectar sempre pede um snapshot completo. Web deve implementar timeout de checkpoint de 20 s, resync e backoff 1–30 s, além de apresentar indisponibilidade.

O modo `per_aggregate_bounded` é eventual, sem corte global, watermark de outbox ou garantia de atraso máximo. A reconciliação interna é feita por HTTP Core; não há SQL entre serviços nem polling periódico no navegador. O painel não autoriza reservas. Só Core verifica capacidade.

Fontes oficiais consultadas para APIs em 16/11/2026: [ioredis](https://github.com/redis/ioredis), [Lua Redis](https://redis.io/docs/latest/develop/programmability/eval-intro/), [amqplib](https://amqp-node.github.io/amqplib/channel_api.html) e [ws](https://github.com/websockets/ws). As versões realmente resolvidas estão no lockfile e em validation.md.
