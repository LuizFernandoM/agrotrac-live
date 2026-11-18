# Validação do primeiro incremento Live

## P2 residual de AMQP — shutdown em 17/11/2026

Revisão independente `task_d892561c5a79/ctx_5b128192865a`, correção `task_4ffdd4e9e50b/ctx_1173836dcd5e`. A alteração se limita ao LIVE. O aceite permanece pendente de nova revisão independente.

Antes da alteração, um proxy TCP próprio encaminhou frames ao RabbitMQ local, adiou `Connection.OpenOk` em 4.800 ms e reteve `Connection.CloseOk` e o FIN do broker no lado voltado ao consumidor. Chamado com o handshake pendente, `RabbitConsumer.stop()` retornou em **5.002 ms** com dois sockets no proxy. Nesse intervalo o handshake terminou e um `Connection.Close` foi emitido. O teste encerrou com código 1. O deadline do loop era silenciado e o stop capturava a conexão antes de ela existir; a sessão abria tarde e entrava em outro deadline de close. [Reprodução anterior à correção](../tests/fixtures/3ebdf3ec6e5c41c88d985ec21d8a8504/rabbit-shutdown.json)

O consumer propaga o abort ao transporte antes da abertura via opção `signal` que o `amqplib` repassa a `net.connect/tls.connect`. Um modelo entregue após o deadline é destruído; uma abertura que conclui na corrida com abort é desconectada ou entregue ao loop para encerramento antes que a sessão avance. A sessão verifica abort nas transições de abertura dos canais, topologia, prefetch e consume. O loop é dono do encerramento e o stop compartilhado aguarda sua conclusão. Em sessões inscritas, cancel e espera das entregas já iniciadas correm em paralelo sob deadline de I/O; o socket é destruído mesmo quando cancel expira e o timeout de cancel ou de drenagem é propagado. O runtime preserva o deadline global de 9.500 ms e desconecta Redis em `finally`, inclusive se o consumer rejeitar.

| Cenário | Stop | Fechamento observado no proxy ou HTTP | Evidência |
| --- | ---: | ---: | --- |
| Handshake do consumer com I/O de 5.000 ms | 4 ms | 16 ms | [JSON](../tests/fixtures/7e3027882eba4a28b2ba2f38b0fe1334/rabbit-shutdown.json) |
| Handshake via `LiveRuntime.stop()`, I/O de 5.000 ms | 4 ms | 16 ms | [JSON](../tests/fixtures/b7e184b836d94ef7b8157d1b0dabf5e8/rabbit-shutdown.json) |
| Leitura do corpo HTTP do Core pendente no runtime | 11 ms | 19 ms | [JSON](../tests/fixtures/16b083611d0c4cf6a8932ea0b8f5f562/rabbit-shutdown.json) |
| Abertura do primeiro canal logo após conectar | 1 ms | 14 ms | [JSON](../tests/fixtures/91a13cb53569468b90ae6e212356486f/rabbit-shutdown.json) |
| `CloseOk` retido durante fechamento para reconexão | 1 ms | 16 ms | [JSON](../tests/fixtures/304383de3afd41fba5cc8736a703711f/rabbit-shutdown.json) |
| Sessão já inscrita | 2 ms | 16 ms | [JSON](../tests/fixtures/fb30c7def9684f8293b480e2ba017a08/rabbit-shutdown.json) |
| `CancelOk` retido, rejeição `IO_TIMEOUT` | 315 ms | 327 ms | [JSON](../tests/fixtures/df9ff1f01a2c4691864d467ecc12d6f7/rabbit-shutdown.json) |
| Confirm de quarentena retido, original voltou à fila | 4 ms | 16 ms | [JSON](../tests/fixtures/c46a9ec6b21f4498a951dd9c01b25135/rabbit-shutdown.json) |
| Entrega bloqueada após commit Redis, rejeição `IO_TIMEOUT` | 314 ms | 314 ms | [JSON](../tests/fixtures/7d35707eafce4e92a620418e290387c4/rabbit-shutdown.json) |

Os nove casos mediram zero sockets AMQP locais abertos no retorno do stop, via `net.client.socket` e `Socket.closed`. O proxy pode observar o fechamento no turno de I/O seguinte. Mantido ativo por pelo menos 10.200 ms nos handshakes e 800 ms nos casos com deadline reduzido de 300 ms, o proxy terminou com zero sockets, timers, frames tardios e novas conexões. Nenhum cenário de abort no startup chegou a declarar topologia ou inscrever o consumer. O RabbitMQ voltou a disponibilizar a entrega original sem ACK nos casos de confirm e pós-commit. A revisão persistida no Redis real permaneceu 1 após liberar o hook pós-commit.

Limites da evidência. Os cenários usam Node 22.14.0, a versão de `amqplib` do lockfile e AMQP TCP local, com retenção de frames pelo proxy. AMQPS e perda de rede remota ficam fora destes ensaios. Para o modelo AMQP já aberto, o fechamento físico usa o stream interno de `amqplib`, uma dependência que deve ser revalidada ao atualizar o pacote. Abortar antes do ACK permite reentrega. Um commit já aceito pelo Redis permanece e o replay passa pela deduplicação existente.

`npm run build`, `npm run typecheck`, `npm test` (9/9) e `npm run test:integration` (31/31, código 0, 28,37 s) passaram. O [resultado pós-teardown da integração completa do LIVE](../tests/fixtures/40cc6d12b84a4ff5ae90ca157c58d623/result.json) registra run e namespace Redis próprios, schema `live_it_1794961446943_40cc6d12`, filas isoladas, 17 cenários, stop durante rebuild em 4 ms e teardown em 204 ms. Os testes de Redis [READY](../tests/fixtures/fc8c6760ac4e42c3bf6d1bbeb97e23dd/shutdown.json), [CONFIG](../tests/fixtures/3a701e5b08474aec92eca5a9c68ea407/shutdown.json) e [EVAL](../tests/fixtures/cc120b261a124053a453eb0608bd599c/shutdown.json) passaram, assim como heartbeat (pings em 256 e 521 ms no ensaio de 250 ms). Os JSONs anteriores foram preservados. Só o relay existente de PostgreSQL na porta 15432 foi iniciado e parado. PostgreSQL, Redis e RabbitMQ permaneceram healthy, e os dados e filas de teste foram preservados.

### Revalidação do dispatch `ctx_b798465bc697`

Em 17/11/2026, das 21:33 às 21:34 em São Paulo, a correção já estava no workspace. A revalidação executou build, typecheck, unidade (9/9) e integração LIVE (31/31 em 28,26 s, exit 0). O [JSON pós-teardown](../tests/fixtures/7f47cd4ca71448c281c70348a8933faf/result.json) registra 198 ms de teardown, schema `live_it_1794962002415_7f47cd4c` e os novos namespaces de Redis e AMQP. O stop durante rebuild levou 3 ms, com zero comandos ou conexões tardias nos 5.200 ms de observação. Os pings do ensaio de heartbeat foram observados em 263 e 526 ms. O relay de PostgreSQL na porta 15432 foi iniciado e parado, e PostgreSQL, RabbitMQ e Redis permaneceram healthy.

| Cenário AMQP | Stop | Fechamento observado no proxy ou HTTP | Evidência |
| --- | ---: | ---: | --- |
| Handshake do consumer | 5 ms | 25 ms | [JSON](../tests/fixtures/43c8a925c8804be493b32747ef1eee47/rabbit-shutdown.json) |
| Handshake do runtime | 1 ms | 16 ms | [JSON](../tests/fixtures/9c153428dc984f269a32eb08eed7a31a/rabbit-shutdown.json) |
| Leitura HTTP pendente | 10 ms | 16 ms | [JSON](../tests/fixtures/576576c0dad944e9b55660a1aef64981/rabbit-shutdown.json) |
| Canal abrindo | 2 ms | 15 ms | [JSON](../tests/fixtures/d8663c6aa5ce4e3ba2c2bcb03e89ca2d/rabbit-shutdown.json) |
| `CloseOk` retido | 1 ms | 16 ms | [JSON](../tests/fixtures/1b57c507221d4f17870364c6df8c8367/rabbit-shutdown.json) |
| Consumer inscrito | 3 ms | 16 ms | [JSON](../tests/fixtures/641ee7934e4c4a8089d5f7188586e61a/rabbit-shutdown.json) |
| `CancelOk` retido, rejeição `IO_TIMEOUT` | 315 ms | 329 ms | [JSON](../tests/fixtures/b0a96e2663b547a08f61b70ffc651380/rabbit-shutdown.json) |
| Confirm retido, entrega devolvida à fila | 3 ms | 16 ms | [JSON](../tests/fixtures/801a5c40fad746fd93c1364b86175ee7/rabbit-shutdown.json) |
| Pós-commit, rejeição `IO_TIMEOUT`, entrega devolvida e revisão Redis 1 | 311 ms | 311 ms | [JSON](../tests/fixtures/20e58e59190d41f5a698ef57a51ac6dc/rabbit-shutdown.json) |

Os nove cenários registraram zero sockets AMQP locais abertos quando o stop resolveu ou rejeitou. Após 10.204–10.214 ms nos handshakes e 806–813 ms nos demais casos, os proxies registraram zero sockets, timers, frames tardios e conexões adicionais. A revalidação também passou por Redis [READY](../tests/fixtures/b71743f20198446588c38bf426cc700d/shutdown.json), [CONFIG](../tests/fixtures/a2915ba3589a4eeab7b1f4de903263ed/shutdown.json) e [EVAL](../tests/fixtures/d75f81f5e8b94778999ad9a02f9d7dd6/shutdown.json). Os recursos dos ensaios e os resultados anteriores foram preservados. O aceite segue pendente de revisão independente.

## Correção após revisão independente — shutdown P2 e pontos P3

Revisão `task_80b7bcc7df77/ctx_69c4da0afe96`, correção autorizada em `task_8e9964f83f62/ctx_33cd7e84714f`. Somente Live foi alterado; o aceite independente desta correção continua pendente. Execução em 16/11/2026 à noite America/Sao_Paulo (timestamps UTC já em 17/11 para a validação final).

**Reprodução antes da correção:** [shutdown.json](../tests/fixtures/91f7c9a25322469c9c30e5988cff2de8/shutdown.json). Proxy RESP próprio, Redis real intacto, INFO/READY e CONFIG atrasados separadamente em 4900 ms, cada fase abaixo dos 5000 ms de I/O. Stop no começo de INFO retornou em **9514 ms**, mas houve EVAL/initialize depois e nova conexão AMQP com estado DRAINING. Aos 10350 ms o proxy ainda observava **2 sockets Redis e 1 socket AMQP**, e o runner terminou exit 1. A falha foi realmente executada; o artefato original foi preservado.

Causa: o runtime só assumia ownership de Redis após `connectRedis()` concluir tanto handshake quanto CONFIG, e shutdown silenciava o deadline sem aguardar o término real do loop. Um cliente ainda não atribuído escapava do disconnect; initialize e início do consumer não revalidavam abort depois dos awaits.

Correção em `src/infra/runtime.ts`, `redis.ts`, `time.ts` e `main.ts`:

- Criar Redis lazy sincronamente e atribuir ownership **antes** do primeiro await. Propagar AbortSignal para handshake, CONFIG e operações de projeção; abort fecha o transporte próprio e remove listeners dos waits. Revalidar abort após conectar/inicializar/reconciliar e dentro da barreira de nova geração.
- No stop, marcar DRAINING, abortar e desconectar o cliente já possuído imediatamente; aguardar servidor, consumer e loop. Manter deadline global de 9500 ms, sem aumentá-lo ou converter timeout/falha de shutdown em sucesso. Erro de shutdown chega ao executável com código sanitizado e exitCode 1.
- Initialize/apply/mark/read recusam novas operações depois de abort. A mesma projeção cancelável cobre rebuild e inicialização de candidato. Um comando já executado pelo servidor antes do stop continua com resultado possivelmente ambíguo; cancelar transporte não oferece rollback Redis.

**Resultado completo depois da correção:** build e typecheck exit 0; unidade **9/9**, 335.6845 ms; integração Live completa **22/22**, 0 failed/skipped, runner **22557.8447 ms**, saída natural exit 0. Nenhuma suíte completa Core/Audit foi repetida.

| Momento de stop | Stop observado | Janela de observação mantida depois / efeito tardio |
| --- | --- | --- |
| INFO/READY com INFO e CONFIG configurados para 4900 ms cada | 2 ms | Endpoints mantidos até 10350 ms desde início; INFO não encaminhado, zero CONFIG/EVAL/AMQP, zero sockets/timers de proxy |
| CONFIG pendente por 4900 ms | 1 ms | Mais 5200 ms; CONFIG não encaminhado, zero EVAL/AMQP e zero sockets/timers |
| EVAL de initialize pendente por 4900 ms | 2 ms | Mais 5200 ms; EVAL não encaminhado, zero escrita/AMQP e zero sockets/timers |
| EVAL de apply durante rebuild real Core HTTP/Rabbit/Redis | 6 ms | Mais 5200 ms; nenhuma operação Redis ou conexão AMQP nova, zero sockets Redis/AMQP e timers de proxy |

Artefatos novos de regressão: [READY](../tests/fixtures/2fe66650bbf34a7b8eaeecfc4c066520/shutdown.json), [CONFIG](../tests/fixtures/d67e0b2f76e045f1a401d637a5c6e354/shutdown.json), [initialize](../tests/fixtures/771a3ad3952d414a9965c3b4e4f4d95d/shutdown.json). O teste mantém os servidores de proxy/sentinel vivos durante a janela posterior ao stop; não os fecha prematuramente para esconder resoluções tardias. Shutdown aguarda a tarefa do runtime, e o runner termina naturalmente sem handles próprios remanescentes. Um ensaio intermediário após a correção também passou: run `33f0c64737c24a4d93e5035664bbc33a`, stop 2 ms, artefato preservado.

P3 heartbeat: substituir tick fixo de min(15 s,10 s), que podia arredondar ping para 20 s, por timeout no próximo deadline de cada conexão; reagendar no open/pong/close e cancelar no fechamento do servidor. Teste real WS com proporção equivalente 250/170 ms observou pings em **255 e 520 ms** desde open (intervalo 265 ms), dentro da tolerância de scheduling de 60 ms e abaixo da quantização anterior de 340 ms. Continua sem promessa de precisão de tempo real sob bloqueio arbitrário do event loop.

P3 relatório: `tests/teardown.ts` tenta todos os closers e registra duração/resultado individual; `live.test.ts` só calcula status após teardown, grava falha se algum closer falhar e então falha o runner. Não há mais `extras.close().catch` convertendo falha em aprovação. Unidade injeta falha de cleanup, comprova que os demais closers ainda executam e que status não pode ser passed. JSON separa status interno de cenário/cleanup do exit code, que só o processo pai observa. Artefatos antigos não foram reescritos.

Relatório novo completo: [result.json](../tests/fixtures/8bdf70a210b8474188f771e9623157a5/result.json), UTC 00:27:34.058–00:27:53.452 em 17/11 (21:27 no dia 16 local). Corpo 19394 ms; teardown **217 ms**, todos os closers passed. Recursos novos preservados:

- Schema Core `live_it_1794875253591_8bdf70a2`.
- Prefixo Redis `agrotrac:live:test:8bdf70a210b8474188f771e9623157a5:`; candidatos e subprefixos preservados, incluindo `stoprebuild:`.
- Exchange `agrotrac.test.live.8bdf70a210b8474188f771e9623157a5`, fila `.events`, quarentena `.quarantine`/fila `.rejected`, fila fixture Core `.audit` e sufixos isolados `.poison`, `.recover`, `.runtime`, `.stoprebuild`.
- Core outbox 5/5 PUBLISHED; consumer principal 12 entregas/12 ACKs e 2 mensagens na quarentena principal. Redis continuou `noeviction`, inspecionado apenas por leitura.

Comandos: `npm run build`, `npm run typecheck`, `npm test`; depois `LIVE_INTEGRATION=1` e `LIVE_INFRA_ENV_PATH` explícito para `npm run test:integration`. Só o relay existente aprovado foi iniciado para PostgreSQL 15432 e parado ao final; containers da infra preservados healthy. Sem mudança Core/Audit/Mobile/infra, política Redis, credenciais, filas operacionais, cleanup destrutivo, commit/push/deploy/tunnel ou início Gateway/Web. Regressões usam proxies próprios; não desligam Redis/broker compartilhados.

As garantias mantidas do incremento (Lua/HSET, versões independentes, dedup, ACK/quarentena, snapshot/barreira/checkpoint) passaram novamente. Rebuild continua `per_aggregate_bounded`; autenticação/sessões, Gateway/Web, escala e retenção continuam pendentes. A evidência cobre os pontos de cancelamento exercitados e ausência de efeitos tardios nas janelas mantidas, não cancelamento retroativo de comando Redis já commitado nem indisponibilidade ilimitada do SO.

## Histórico da primeira entrega

Executada 16/11/2026, Windows PowerShell, Node 22.14.0, npm 10.9.2. Escritas limitadas a agrotrac-live. Sem commit/push/deploy, Gateway/Web, autenticação fictícia, aparelho ou túnel. Relato iPhone offline/persistência permanece evidência manual do usuário em outro módulo; não foi repetido nem ampliado aqui.

## Resultado da primeira entrega (histórico)

| Comando | Evidência |
| --- | --- |
| npm run build | exit 0, tsc gerou dist/ |
| npm run typecheck | exit 0, fonte e testes |
| npm test | exit 0, 8/8, sem Docker/rede, 0 skipped |
| npm run test:integration | exit 0, 16 cenários + pai = 17 aprovados, 0 failed/skipped; runner 16175.6588 ms |

Execução final iniciou 23:49:47.200 UTC e terminou 23:50:00.465 UTC (20:49–20:50 America/Sao_Paulo), corpo da integração 13265 ms; teardown próprio 199 ms. O processo saiu naturalmente com exit 0, sem loop/process.exit forçado. O teste do runtime passou por geração inexistente, 503, rebuild e timeout/reconexão Redis, e comprova shutdown <10 s. Os asserts de fechamento do servidor WS também passaram (<1 s no cenário).

Comandos reais a partir de agrotrac-live:

```powershell
npm run build
npm run typecheck
npm test
docker start agrotrac-core-test-relay-cf6006
$env:LIVE_INTEGRATION = '1'
$env:LIVE_INFRA_ENV_PATH = (Resolve-Path '../agrotrac-infra/.env').Path
npm run test:integration
docker stop agrotrac-core-test-relay-cf6006
```

Credenciais só lidas do arquivo explicitamente escolhido, sem imprimir valores. PostgreSQL alvo 127.0.0.1:15432 pelo relay existente aprovado; não foi usada porta Windows 5432. Ao final o relay ficou **Exited (143)** e foi preservado; PostgreSQL, RabbitMQ e Redis da infra permaneceram **Up (healthy)**. Só esse relay foi iniciado/parado. Nenhum CONFIG SET, alteração .env/Compose/roles/portas/volumes, limpeza de fixtures, SQL destrutivo, FLUSH/DEL de limpeza, purge/delete de filas ou consumo de filas operacionais.

## Recursos preservados da execução final

Relatório: [result.json](../tests/fixtures/d4e2b8aeb7584a42a340cf7b68f385c8/result.json).

- Run ID: `d4e2b8aeb7584a42a340cf7b68f385c8`.
- Schema Core novo: `live_it_1794872986799_d4e2b8ae`.
- Prefixo Redis: `agrotrac:live:test:d4e2b8aeb7584a42a340cf7b68f385c8:`; todas as gerações preservadas.
- Exchange topic: `agrotrac.test.live.d4e2b8aeb7584a42a340cf7b68f385c8`.
- Fila Live: exchange acima + `.events`, binding `agrotrac.core.#`.
- Fila Audit fixture: exchange acima + `.audit`, criada pelo publisher Core real somente neste namespace.
- Quarentena direct: exchange acima + `.quarantine`; fila exchange base + `.rejected`, chave `rejected`.
- Cenários próprios usam sufixos `.poison`, `.recover` e `.runtime`, com mesmos tipos/durabilidade, preservados. Redis runtime usa subprefixo `runtime:`.
- Outbox Core: 5 eventos, todos PUBLISHED (silo, janela, reserva, cancelamento e bloqueio).
- Consumer principal: 12 entregas, 12 ACKs; quarentena principal: 2 mensagens. Consumer poison separado confirmou 2 mensagens após recuperação da rota (poison e evento válido), com 0 ACK antes da recuperação.
- Redis `CONFIG GET maxmemory-policy` retornou `noeviction`. Nenhuma modificação da política foi feita.

## Cenários comprovados

1. Core real importado apenas pelos testes, HTTP TCP/fetch, PostgreSQL fixture e outbox/publisher reais até RabbitMQ; Redis Lua real e dois clientes WS TCP recebem snapshot primeiro e atualizações sem polling REST periódico. Reserva 2000 kg, cancelamento para reservado zero, silo v3, janela bloqueada v4. Cache/WS inspecionados sem nome/documento/placa/booking.
2. Reentregas de eventos capturados em ordem inversa não mudam revisão/saldos. Lua concorrente com 25 chamadas termina na maior versão. Fixture mista silo v8/janela v2 recebe silo v7/janela v3 e avança só janela. Conflito igual versão/diferente conteúdo não aplica metade do lote. Bad key type e versão armazenada inválida preservam hash/string anteriores. MAX_SAFE_INTEGER é preservado e revisão esgotada não escreve.
3. Conflito de event_id preserva original e vai à quarentena. JSON poison também. Mandatory/return rejeita confirmação sem rota. Remoção temporária apenas do binding da quarentena fixture prova ACK=0; restaurar binding permite confirm+ACK e uma mensagem válida seguinte continua sendo drenada.
4. Hook depois de commit Redis lança falha antes de broadcast; reconexão WS recebe snapshot confirmado. Outro hook suprime só o último broadcast, sem crash e sem sucessor; checkpoint aciona resync do cliente, segundo snapshot recupera revisão sem GET periódico. Intervalo de checkpoint encurtado somente na fixture (100 ms).
5. Hook segura leitura de snapshot já capturado durante abertura; update Redis/WS ocorre antes de liberar. Cliente recebe snapshot primeiro, depois revisão contígua correspondente ao Redis, sem aplicar snapshot antigo após update.
6. Nova geração/prefixo vazio simula Redis loss sem apagar nenhuma chave. Core HTTP repovoa silo e janela antigos/bloqueados. Coleta parcial, resposta acima do limite e catálogo continuamente variável nunca viram READY. Mudança contínua é injetada na porta de coleta; não prova snapshot global do Core.
7. Proxies TCP exclusivos congelam/cortam Redis, Rabbit e Core HTTP sem parar infra. Redis timeout impede confirmação, tira readiness e reconexão usa conexão nova; runtime usado pelo executável também recompõe geração. Broker se reconecta com backoff. HTTP timeout não promove candidato. Handshake AMQP silencioso e confirm silencioso vencem deadlines; conexões próprias são encerradas e há conexão/confirm real posterior.
8. WS recusa subprotocolo incompatível com HTTP 400; gap provoca resync; frame >4 KiB fecha 1009; JSON inválido fecha 1007 e versão inválida 1008. Cliente real com autoPong=false é terminado por heartbeat. Socket de cliente pausado sob 5000 updates de carga fecha por backpressure/deadline enquanto cliente saudável recebe updates e permanece aberto. Essa carga é injeção de transporte, não eventos Core nem benchmark de domínio.

Unidade cobre JSON estrito/UTF-8/chaves duplicadas (inclusive escapadas)/NUL/Unicode/limites/profundidade, envelope e redundâncias, safe integers, versões/conflitos, coleta estável/orfandade/oversize, falha de persistência/quarentena sem confirmação, fault durante coleta, expiração de readiness, deadline/cleanup tardio e abort de backoff. Não requer containers.

## Histórico de desenvolvimento e limites honestos

- Run `cca06b19a5834b06b32b3d3b2e229dc8`, schema `live_it_1794872550411_cca06b19`: falhou no teste de reutilização de conexão Redis após timeout. O relatório inicial marcou status passado incorretamente apesar do runner exit 1; **não é evidência de aprovação**. O adapter passou a descartar conexão em falha; o relatório passou a capturar resultado por cenário. Artefato original preservado.
- Run `d356699721b54a8ca33453d754954e36`, schema `live_it_1794872619431_d3566997`: 13 cenários aprovados, 8232 ms, teardown 180 ms, aprovação de etapa anterior.
- Run intermediário por volta de 23:46 UTC completou 15 cenários, mas teardown teve erro no helper (`close` ausente no wrapper de runtime). Runner foi interrompido, sem alegar aprovação/ausência de handles. Recursos foram preservados; wrapper foi corrigido. As execuções seguintes finalizaram naturalmente.
- Run `33817dc81384461da1d06172ecbd2e71`, schema `live_it_1794872896671_33817dc8`: 15 cenários + pai aprovados, 13013 ms, teardown 170 ms. Execução final acima adiciona casos de subprotocolo/gap/oversize/heartbeat.

Testes de crash são hooks nos pontos de falha do código, não desligamento do host/Redis nem prova de durabilidade física. Redis loss é ausência de geração nova, não FLUSH da infraestrutura. Coletas HTTP não oferecem corte global; duas rodadas iguais continuam apenas evidência limitada por agregado. Não há SLA, garantia de exactly-once, atraso máximo da outbox, ausência absoluta de deadlocks ou proteção contra administrador externo corrompendo dados.

Não há validação de Gateway/WSS/TLS, autenticação/sessões reais, Web de produto ou múltiplas instâncias. São etapas posteriores explícitas, não APIs fictícias entregues. O contrato/servidor prevê resync e timeout de checkpoint; implementação da futura Web deve segui-los. Retenção/limpeza de gerações, dedup e quarentena exige política própria; nenhum dado foi removido. Crescimento de memória do hash e dedup é limite operacional deste MVP; `noeviction` impede mascarar perda por TTL/eviction, mas não garante disponibilidade sob exaustão.

Versões locais resolvidas: amqplib 0.10.9, ioredis 5.11.1, ws 8.23.0, dotenv 17.4.2, TypeScript 5.9.3, tsx 4.23.15, pg 8.23.1 (somente testes), prettier 3.10.1. Node global não foi alterado. Package-lock fixa a árvore; documentação oficial consultada está em implementation.md. Revisão independente da coordenação permanece pendente antes de ampliar escopo.
