# AgroTrac Live — contrato incremental para revisão

Data: 16/11/2026. Status: primeiro incremento implementado e validado conforme docs/validation.md, sujeito à revisão independente da coordenação. As decisões vinculantes abaixo foram preservadas. Antigravity continua bloqueado por cota; não autenticar, trocar conta ou modelo para contornar o bloqueio.

## Decisões vinculantes da coordenação — 16/11/2026

- Aprovado o modo `per_aggregate_bounded` como projeção eventual observada para o painel, sem corte transacional global e sem garantia de atraso máximo. Os bounds delimitam coleta/recursos, não a defasagem do estado Core. Nenhuma resposta Live autoriza reserva, e indisponibilidade conhecida deve ser explícita. Não há necessidade de alterar Core para esta entrega.
- Aprovados os limites operacionais propostos como defaults configuráveis validados, sem alterar políticas, credenciais, portas, volumes ou memória/eviction da infraestrutura existente. Inspecionar `maxmemory-policy` em Redis por leitura; se incompatível com o contrato, reportar ao coordenador em vez de executar CONFIG SET. Uma única instância é limite operacional documentado, não implementação de escala distribuída.
- Os contratos REST/WS e de cache podem ser implementados no repositório Live; todos os outros repositórios permanecem somente leitura para o executor. Testes usam fixtures novas e isoladas e preservam recursos. Sem FLUSH, DROP, TRUNCATE, purge ou remoção de dados/arquivos/filas.
- Não importar domínio/código Core/Audit em produção. Reuso de fixtures de teste via opt-in é permitido. Mensagens inválidas/conflitantes não devem bloquear indefinidamente a drenagem de mensagens válidas; retirar readiness, quarentenar com confirmação e reconciliar com backoff conforme contrato.
- Readiness/health e metadados não devem expor informação pessoal ou credenciais; WS/REST usam somente snapshots de silo/janela já selecionados. Nenhuma sessão ou autenticação fictícia, mudança de bind externo, deploy ou túnel público é autorizada. Requisitos completos de sessões/autenticação e Gateway continuam pendentes, não removidos.
- Testes precisam comprovar comportamento real de Redis/AMQP/HTTP/WS e distinguir hooks de injeção de falhas, testes automatizados e limitações não executadas; não prometer inexistência absoluta de deadlocks, exactly-once ou durabilidade do Redis. Uma revisão independente posterior decidirá o aceite do incremento.

## 1. Entrega e limites

Primeiro incremento: Core/outbox → RabbitMQ → projeção Redis versionada → WebSocket local `/live`, com `GET /v1/live/snapshot` para a futura Web. Preservar sete repositórios independentes, Node.js/TypeScript, Redis e RabbitMQ existentes. Não adicionar CDC, componentes ou banco persistente do Live. Redis é projeção descartável; Core continua sendo a única autoridade para reserva/capacidade. A aprovação de uma reserva nunca depende do saldo exibido pelo Live.

Bind obrigatório `127.0.0.1:3002`, sem exposição externa, túnel ou autenticação fictícia. Não aceitar configuração de bind externo neste incremento. Autenticação real, emissão/validação/revogação/expiração de tokens, sessões e eventos `agrotrac.live.session.*` permanecem requisitos futuros completos, ainda pendentes. Não criar usuário, senha ou token de demonstração nem publicar eventos de sessão antes da definição desse domínio. Este recorte não substitui essas responsabilidades do README.

Sem alterações no Core, infra, Audit, Mobile, Gateway ou Web nesta implementação. Sem evento produtor Audit, interpretação de pesagem como descarga ou conversão automática de reserva em ocupação física. Testes conectados autorizados são opt-in, isolados e documentados; sem aparelho, tunnel, commit, push ou deploy.

Fontes locais lidas: [README Live](../README.md), planejamento arquitetural da raiz, [contratos Core](../../agrotrac-core/docs/contracts.md) incluindo revisão vinculante e [validação Core](../../agrotrac-core/docs/validation.md), `core/src/application/core.ts`, `core/src/domain/model.ts`, `core/src/infra/http.ts`, [contrato consumer Audit](../../agrotrac-audit/docs/contracts.md), contrato de pesagens Audit, README/design/validação Mobile e README Gateway. O envelope abaixo acompanha o código real, não exemplos antigos divergentes.

## 2. Entrada Core v1

```ts
type CoreEvent = {
  event_id: UUIDv4;
  type: 'agrotrac.core.silo.created' | 'agrotrac.core.window.created'
    | 'agrotrac.core.window.updated' | 'agrotrac.core.booking.created'
    | 'agrotrac.core.booking.cancelled';
  version: '1.0.0';                 // contrato, não versão do agregado
  producer: 'agrotrac-core';
  occurred_at: UTC;
  correlation_id: UUIDv4;
  payload: CorePayload;
};
```

`type` deve coincidir com a routing key. UUID v4 canônico minúsculo; UTC com calendário real e formato `YYYY-MM-DDTHH:mm:ss[.sss]Z`. Não ordenar por timestamp, `event_id` ou versão semântica. AMQP `messageId`, quando presente, deve coincidir com `event_id`; não usar delivery tag como identidade persistente.

Todo payload contém `silo_id`, `silo_version`, `silo` completo e `saldo_disponivel_silo_kg`. `silo.created` não contém janela. Os demais contêm `window_id`, `window_version`, `window` completo, `inicio_em`, `termino_em`, `status`, `updated_at`, `motivo` e `saldo_disponivel_janela_kg`. Aqui `payload.status` é status da JANELA, mesmo em cancelamento. Eventos de reserva também contêm `booking_id`, `booking` completo, `placa`, `motorista_nome`, `peso_estimado_kg`; cancelamento acrescenta `peso_estornado_kg`, `cancelled_at` e motivo do cancelamento.

Snapshots reais:

```ts
type SiloSnapshot = {
  id: UUIDv4; codigo: string; nome: string; tipo_grao: string;
  capacidade_total_kg: number; ocupacao_fisica_kg: number;
  saldo_reservado_kg: number; saldo_disponivel_kg: number;
  status: 'ACTIVE' | 'INACTIVE'; version: number;
  created_at: UTC; updated_at: UTC;
};
type WindowSnapshot = {
  id: UUIDv4; silo_id: UUIDv4; inicio_em: UTC; termino_em: UTC;
  capacidade_maxima_kg: number; saldo_reservado_kg: number;
  saldo_disponivel_kg: number; status: 'OPEN' | 'BLOCKED' | 'CLOSED';
  version: number; created_at: UTC; updated_at: UTC;
};
```

Validar envelope e payload antes de qualquer escrita: JSON UTF-8 estrito, objeto (não array/null), sem chaves repetidas, NUL ou Unicode isolado, profundidade máxima 32 e tamanho máximo 1 MiB. Rejeitar versão/tipo não suportado para quarentena; não ignorar silenciosamente um futuro evento ligado por `#`. Campos obrigatórios, enums, UUIDs, datas, tamanhos de texto e vínculos seguem o contrato Core; campos desconhecidos de negócio requerem nova revisão, não coerção. Validar também os campos de reserva recebidos, mas não persistir nem transmitir a reserva pessoal no painel.

Massas e versões precisam de `Number.isSafeInteger`: kg entre 0 e 9007199254740991; capacidades, peso da reserva e versões positivos. Rejeitar string numérica, fração, negativo, NaN e overflow. Conferir identidades e redundâncias: `silo_id === silo.id`, `silo_version === silo.version`, `window_id === window.id`, `window.silo_id === silo.id`, `window_version === window.version`; datas/status/saldos repetidos iguais ao snapshot. Vínculos/valores de booking devem coincidir com os campos correspondentes; cancelamento exige CANCELLED e estorno igual ao peso estimado. Não exigir que `payload.status` seja CANCELLED.

Validar `ocupacao_fisica_kg + saldo_reservado_kg <= capacidade_total_kg` por aritmética segura (ou subtrações limitadas), disponível do silo = total − físico − reservado; disponível da janela = máxima − reservado. Não impor cota da janela <= capacidade livre do silo. Início < término; `created_at <= updated_at`. Nenhuma regra fiscal, no-show, multa ou conclusão automática é introduzida.

## 3. Projeção e atomicidade

Uma instância Live e um consumer serial (`prefetch=1`) neste incremento; múltiplas instâncias exigem coordenação/fanout futuro. Namespace `agrotrac:live:v1:<generation>` com geração ativa selecionada pelo runtime único. Um hash reúne metadados/revision/coleta, snapshots, versões, índice por campos, contagens e dedup `event_id → digest canônico do envelope inteiro`. Canonicalização ordena chaves recursivamente, preserva arrays e valores/textos; SHA-256 permite comparar conteúdo sem guardar booking/documentos no cache. Mesma identidade com conteúdo divergente é `EVENT_ID_CONFLICT`, como no Audit; não sobrescrever o original.

Uma operação Lua atômica, ou WATCH/MULTI/EXEC com CAS e retry limitado equivalente, deve executar TODO o seguinte no mesmo commit:

1. Verificar geração/estado esperado e tipos das chaves; comparar digest do `event_id`. Igual retorna DUPLICATE sem novo efeito; diferente retorna conflito sem mudança.
2. Comparar o silo e a janela SEPARADAMENTE com os registros existentes. Versão maior substitui snapshot completo; menor não altera; igual exige igualdade canônica do snapshot ou retorna `AGGREGATE_VERSION_CONFLICT`. Pré-validar ambos antes de escrever: um conflito não deixa metade aplicada.
3. Atualizar apenas agregados avançados e seus índices; registrar dedup mesmo se ambos já estiverem antigos. Incrementar `revision` local uma vez apenas se houver mudança visível, com verificação de safe integer. Retornar revisão e snapshots efetivamente aplicados.

Não usar incremento/decremento de kg para interpretar eventos: substituir snapshots versionados preserva estorno e evita descontar novamente em redelivery. Exemplo vinculante: cache silo v8, janela X v2; mensagem silo v7/janela X v3 aplica SOMENTE janela v3. `window.updated` pode carregar a mesma versão de silo; `window.created` começa janela v1 sem incrementar silo; booking/cancelamento incrementam os dois. Cancelamento v5 seguido de criação de reserva v4 não repõe saldo antigo. Versão maior pode saltar números porque há snapshot completo; salto não significa necessariamente perda de dados do estado atual daquele agregado.

Lua/MULTI não oferecem rollback automático de erros de execução depois de comandos já aplicados. Pré-validar tipos, limites e conflitos antes da primeira escrita, usar comandos de escrita sem falhas previsíveis e testar falhas parciais; resultado técnico inesperado invalida a geração e exige rebuild, sem ACK. Em Lua, operações com versões/revisões devem preservar a precisão até MAX_SAFE_INTEGER; recusar incremento no teto. Atomicidade de visibilidade não deve ser confundida com durabilidade ou rollback em erro.

ACK manual somente depois do resultado Redis confirmado (incluindo DUPLICATE/STALE) ou da quarentena confirmada. Falha/timeout ambíguo Redis deixa sem ACK: retry pode encontrar o commit anterior. Falha entre commit e ACK produz reentrega idempotente. Não broadcast antes do commit. A resposta do Redis confirma aplicação, não durabilidade absoluta contra perda do Redis; rebuild é obrigatório após perda/eviction/corrupção detectada.

Sem TTL em snapshots, índices, dedup ou marcador ativo neste incremento; configurar namespace sem eviction exigida e monitorar limites. Não expirar marcador para disfarçar stale. Exaustão de memória/eviction/missing key inesperado tira readiness e força rebuild. Persistência Redis existente não vira garantia de fonte de verdade. Retenção futura de dedup exige política explícita; após eventual expiração, versões ainda impedem regressão, mas detecção histórica de ID conflitante já não seria garantida.

## 4. Bootstrap, Redis loss e lacuna Core HTTP

Core tem GET `/v1/core/silos`, `/windows` e consultas individuais com versões e snapshots. Não possui endpoint de snapshot conjunto atômico, watermark global/outbox cursor de consumo, paginação ou barreira de publicação/drenagem. Cada GET roda em transação própria. Duas respostas válidas não provam um mesmo instante; duas coletas iguais também não provam ausência de mutações entre elas. Fila vazia/consumer conectado não prova que a outbox Core já publicou tudo. Não consultar tabelas SQL Core nem usar ledger Audit como substituto de snapshot.

Estratégia mínima segura com as interfaces atuais: rebuild HTTP limitado em geração candidata, explicitamente `consistency: 'per_aggregate_bounded'`, nunca alegar corte transacional global ou frescor absoluto. A coordenação precisa aceitar esse modo para habilitar a primeira entrega; se exigir snapshot global atual, readiness permanece falsa até contrato futuro Core de snapshot atômico + watermark/replay. Essa extensão é dependência pendente, não endpoint inventado aqui.

Procedimento proposto:

1. Marcar BOOTSTRAPPING/REBUILDING; fechar clientes ativos com 1012 e negar snapshots de sucesso. Declarar topologia/consumer antes da primeira coleta; eventos são aplicados serialmente à geração candidata pelo mesmo CAS. Sem capacidade de concluir, pausar consumer/fechar conexão própria, deixando mensagens sem ACK no broker.
2. Coletar listas COMPLETAS de todos os silos e janelas por HTTP, sem filtrar apenas ACTIVE/OPEN ou período (cancelamentos/estoque dependem de reservas de outras janelas). Deadline de 5 s por GET, 30 s por rodada, máximo 10 MiB por resposta e 10000 entidades por tipo; excesso é erro explícito, nunca truncamento. Sem paginação Core, exceder bounds bloqueia rebuild até evolução revisada.
3. Validar tudo como snapshots; mesclar no candidato pelo mesmo comparador por agregado, sem dedup fictício para HTTP. Nunca substituir versão mais nova recebida via Rabbit. Repetir coleta completa; exigir duas rodadas consecutivas com mesmo conjunto de IDs e mesmos snapshots por versão, relações janela→silo válidas, sem conflitos e sem falhas. Até 3 pares em 60 s totais; se não estabilizar, ficar indisponível e retry com backoff (1–30 s), sem servir parcial. Mudanças contínuas podem impedir disponibilidade: risco aceito do fallback.
4. Antes de READY, ler atomicamente o hash e verificar índices/contagens, cobertura e ausência de janela órfã. Selecionar candidato no runtime sob barreira local, sem disponibilizá-lo antes da validação; não existe ponteiro Redis compartilhado entre processos neste incremento de instância única. Leitura captura a referência e revalida geração/fault depois do comando Redis. Não apagar geração anterior nem recursos preexistentes. A nova geração recebe UUID novo e revisão inicial própria; consumidores Web descartam o estado anterior.

Metadados de cada sucesso incluem `core_scan_started_at`, `core_scan_completed_at`, `consistency`, `generation`, `revision`, `broker_connected` e `observed_at`. O bound é intervalo observado de coleta, NÃO limite garantido de atraso da outbox. Exibir estado materializado observado, sem afirmar que é o estado transacional atual do Core. Freshness real não pode ser inferida apenas de silêncio do broker.

Revalidar catálogo pelo mesmo rebuild/merge HTTP a cada 60 s no servidor (não polling HTTP na Web); se última verificação completa exceder 120 s ou falhar, retirar readiness e invalidar clientes até recuperação. Isso limita idade da verificação de cobertura, não cria atomicidade global nem resolve atraso ilimitado da outbox. Um namespace vazio após restart não significa instalação sem silos: só duas listas completas válidas e estabilizadas podem confirmar arrays vazios. Reinício do Live sempre revalida geração; Redis reconectado não é automaticamente READY. Missing marker, chave/index inconsistente, Redis loss ou conflito de versão exigem rebuild. Mensagem poison de estado suportado retira readiness até reconciliação; demais eventos podem continuar sendo drenados com falha visível.

## 5. REST, readiness e WebSocket v1

`GET /health/live` retorna 200 se processo responde, sem dependências. `GET /health/ready` retorna 200 só em READY no modo de consistência aprovado; caso contrário 503 com código sanitizado. Broker desconectado, Redis indisponível, rebuild, conflito ou shutdown retiram readiness imediatamente. Não servir cache antigo com status 200 durante falha conhecida. Não exigir fila vazia como falsa prova de sincronização.

`GET /v1/live/snapshot`: sem query neste incremento (query desconhecida → 400); leitura Redis atômica da geração ativa/metadados/índices/valores. Arrays determinísticos ordenados por ID; não usar SCAN como snapshot. Limites de catálogo/10 MiB valem também para resposta; excesso retorna 503 `SNAPSHOT_TOO_LARGE`, sem resposta parcial. 200 com `Cache-Control: no-store`:

```ts
type Snapshot = {
  protocol_version: '1.0.0'; generation: UUIDv4; revision: number;
  consistency: 'per_aggregate_bounded';
  core_scan_started_at: UTC; core_scan_completed_at: UTC;
  observed_at: UTC; broker_connected: true;
  silos: SiloSnapshot[]; windows: WindowSnapshot[];
};
```

Sem bookings, placas, nomes/documentos de motorista, fotos, motivo livre ou payload Core completo. Campos de cadastro/capacidade acima bastam para o primeiro painel. Fila nominal de recepção e dados pessoais precisam de contrato/autorização futuros.

Erros REST seguem AgroTrac `application/json` `{error:{code,message,details:null,correlation_id,timestamp}}`, `X-Correlation-Id` UUID válido gerado se ausente; header inválido → 400 `INVALID_CORRELATION_ID` com novo ID. 400 `INVALID_QUERY`, 404 `NOT_FOUND`, 503 `LIVE_NOT_READY`, `REDIS_UNAVAILABLE`, `BROKER_UNAVAILABLE`, `SNAPSHOT_TOO_LARGE`, 500 `INTERNAL_SERVER_ERROR`; sem stack/path/credencial. 503 inclui `Retry-After: 1`.

WS usa `/live`, subprotocolo obrigatório `agrotrac.live.v1`, JSON UTF-8 e `protocol_version: '1.0.0'` em todo frame de aplicação. Upgrade em não READY → HTTP 503; subprotocolo ausente/incompatível → HTTP 400 antes do upgrade. Frames:

```ts
// servidor → cliente
{ protocol_version:'1.0.0', type:'snapshot', data: Snapshot }
{ protocol_version:'1.0.0', type:'update', generation:UUIDv4,
  previous_revision:number, revision:number, event_id?:UUIDv4,
  silos:SiloSnapshot[], windows:WindowSnapshot[] }
{ protocol_version:'1.0.0', type:'checkpoint', generation:UUIDv4, revision:number }
{ protocol_version:'1.0.0', type:'resync_required', code:string }
{ protocol_version:'1.0.0', type:'error', code:string, correlation_id:UUIDv4 }
// cliente → servidor: sem sessão/token inventados
{ protocol_version:'1.0.0', type:'resync' }
```

Abertura/reconexão SEMPRE entrega snapshot inteiro, sem promessa de replay desde cursor do cliente. Registrar conexão como SYNCING antes da leitura atômica Redis; serializar commits e broadcasts pela barreira local. A fila SYNCING captura updates concorrentes e a referência/fault é revalidada após ler. Snapshot revisão R é primeiro frame de dados, depois somente updates > R, ordenados e sem lacunas locais. Descartar frames <= R já cobertos. Não manter lock enquanto socket lento envia: buffer limitado, transição SYNCING→ACTIVE atômica no coordenador local. Leitura/commit revalidam geração. Rebuild desconecta em vez de misturar gerações.

Updates originados da reconciliação HTTP não têm event_id de produtor; nesse caso o campo é omitido, sem inventar evento Core. Cliente substitui estado no snapshot; update requer mesma geração e `previous_revision === revisão local`, compara versões por entidade e aplica lote inteiro. Duplicata com revisão <= local é ignorada; mesma versão com conteúdo conflitante, revisão faltante ou geração diferente exige `resync`, suspende aplicação incremental e sinaliza painel desatualizado. Servidor trata `resync` pela mesma barreira da abertura. `revision` é ordem local da projeção, não watermark Core nem ordem de commits SQL.

Notificações WS são NÃO duráveis, transporte Core/Rabbit é pelo menos uma vez; não prometer exactly-once nem entrega de toda transição intermediária. Dedup não garante broadcast: crash depois do commit Redis e antes do envio é coberto por snapshot no reconnect. Para falha de envio sem crash e último evento sem sucessor, servidor lê revisão ativa Redis e manda `checkpoint` a cada 10 s; checkpoint maior que revisão do cliente exige resync, inclusive sem próximo update. Erro nessa leitura invalida/fecha conexões; timer de checkpoint vencido por 20 s obriga fechamento/reconexão pelo cliente. Não depender apenas de chegada de um próximo evento. No reinício não reaproveitar conexão ou geração sem revalidar.

Ping WS a cada 15 s, pong em até 10 s ou terminate do próprio socket. Limites: frame cliente 4 KiB, máximo 1 resync/s por cliente; buffer de saída até 2 MiB ou 100 updates, snapshot até 10 MiB com deadline de envio 5 s (limite próprio separado do buffer incremental). Cliente lento não bloqueia consumer/ACK ou outro cliente: mandar `resync_required` se possível e fechar 1013, sem crescimento ilimitado. Não ficar repetindo snapshot para um cliente incapaz de recebê-lo. Frames malformados → erro sanitizado e 1007; tipo/versão de aplicação inválido → 1008; oversized → 1009; indisponibilidade/restart → 1012/1013. Cliente reconecta com backoff 1–30 s e jitter, reiniciado após snapshot válido, sem polling REST periódico.

## 6. Rabbit, poison e encerramento

Exchange canônica `agrotrac.events`, topic durable, autoDelete false. Fila `agrotrac.live.events`, durable, autoDelete false; binding `agrotrac.core.#` (não o `*` antigo do README). Não alterar argumentos de filas existentes, instalar TTL/DLX, purgar ou recriar recursos incompatíveis. Falha de declaração é falha de readiness e sessão.

Quarentena própria: exchange direct durable `agrotrac.live.quarantine`, fila durable `agrotrac.live.quarantine.events`, binding `rejected`; não usar a quarentena Audit. Canal confirm separado, publicação persistente, mandatory true, deadline 5 s; return é falha mesmo com confirm positivo. Só ACK original após confirmação roteada. Falha/timeout deixa original sem ACK. Crash entre confirmação e ACK pode duplicar quarentena, honestamente.

Registro: `{quarantine_id, error:{code}, quarantined_at, source:{exchange,routing_key,message_id}, raw_base64}` com bytes originais. Códigos: `INVALID_JSON`, `INVALID_ENVELOPE`, `INVALID_PAYLOAD`, `UNSUPPORTED_VERSION`, `UNSUPPORTED_EVENT_TYPE`, `ROUTING_KEY_MISMATCH`, `MESSAGE_TOO_LARGE`, `EVENT_ID_CONFLICT`, `AGGREGATE_VERSION_CONFLICT`. Dados pessoais podem existir nos bytes: acesso restrito e retenção/redrive administrativo pendentes, sem logs do corpo. Não republicar poison como evento válido nem emitir evento Audit.

Falhas transientes Redis/AMQP: até 3 tentativas locais por entrega (250 ms, 500 ms); esgotou → cancelar consumer, drenar tarefas limitadas, fechar SOMENTE conexão própria, sem ACK pendente. Reconectar com backoff exponencial 1–30 s e jitter; não zerar por mero handshake, apenas após progresso confirmado. Não usar nack/requeue em hotloop, retry infinito em memória, descarte por número de tentativas ou quarentena para indisponibilidade técnica. Sem limite global de retries que perca eventos. Ambiguidade de commit Redis se resolve por dedup na nova entrega.

I/O Redis/Core/AMQP tem deadline 5 s; falha invalida readiness e sockets. SIGINT/SIGTERM: marcar DRAINING, negar novos upgrades/snapshots, cancelar novas entregas, notificar/fechar WS 1001, aguardar efeitos em curso até deadline global 10 s, ACK só dos commits/quarentenas confirmados; fechar conexões/timers próprios. Ao vencer prazo, fechar canal/conexão sem ACK do restante, que retorna ao broker. Resolver conexão tardia deve fechá-la; nenhum handle órfão. Não desligar infra compartilhada.

## 7. Implementação e aceite executável

Portas pequenas sugeridas: `validateCoreEvent(bytes,routingKey)`, `Projection.apply(event) -> APPLIED|STALE|DUPLICATE|CONFLICT`, `Projection.readSnapshot()`, `CoreSnapshotReader.collect()`, `Quarantine.publishConfirmed()`, `Realtime.open/resync/broadcast/checkpoint`, `Consumer.start/stop`. Domínio e comparadores puros; adapters Redis/HTTP/AMQP/WS separados. Não importar código Core em produção nem consultar seu SQL. Testes podem iniciar Core real isolado e usar sua API para fixtures.

Unidade sem Docker/rede: validações, safe integers e limites; digest canônico/conflito; tabela de versões independentes; cancelamento fora de ordem; regras de ACK; retry com relógio controlado; máquina readiness/rebuild; corrida abertura/update; checkpoint sem evento posterior; backpressure e shutdown. Mocks não substituem prova de atomicidade Redis.

Integração opt-in (`LIVE_INTEGRATION=1`): ambiente/secrets só por env ou arquivo explicitamente selecionado `LIVE_INFRA_ENV_PATH`; nunca carregar `.env` da infra automaticamente, imprimir URL com senha ou logar payload pessoal. Gerar run ID UUID; schema Core, exchange/filas/quarentena e prefixo Redis exclusivos novos. Usar mesma topologia/tipos/bindings com nomes de teste; não consumir/publicar em filas operacionais, não usar FLUSHDB/FLUSHALL, DROP/TRUNCATE, purge/delete de filas, DEL de prefixo alheio ou limpeza de fixtures. Preservar recursos e registrar nomes sanitizados. Não mudar argumentos de recursos existentes. Execuções reais e falhas de desenvolvimento estão registradas em docs/validation.md.

| Cenário real futuro | Critério de aceite |
| --- | --- |
| Core HTTP → outbox real → Rabbit real → Redis real → WS TCP | Criar silo/janelas/reserva por HTTP, comprovar mesmo event_id publicado/consumido e versões Redis; dois clientes recebem snapshot inicial e update sem GET periódico, com deadlines de 5 s em ambiente local saudável |
| Estorno/bloqueio | Cancelar via Core HTTP; reservas/saldos refletidos por snapshot, reentrega não estorna duas vezes; bloquear preserva saldo e só muda versão da janela |
| Fora de ordem/versões | Reentregar eventos capturados da fixture em ordem inversa e mistura silo v8/janela v3 vs silo v7/janela v4; nenhum agregado regride; CAS real sob concorrência não deixa escrita parcial |
| Identidade/conflito | Mesmo ID igual é no-op; mesmo ID diferente e agregado mesma versão diferente preservam estado original, quarentena confirmada e readiness invalidada quando necessário |
| Poison/roteamento | JSON/UTF-8 inválido, inseguro, excesso e versão desconhecida chegam à quarentena; simular mandatory return/confirm perdido prova ausência de ACK prematuro e eventual redelivery |
| Crash entre Redis e WS | Hook só no processo de teste interrompe depois do commit antes do broadcast/ACK; restart+reconnect entrega snapshot; caminho sem crash perde só broadcast, checkpoint detecta último update perdido em até 20 s e resync recupera |
| Abertura concorrente | Mutação enquanto snapshot é capturado/enviado; cada cliente recebe snapshot R seguido de updates > R ou resync, nunca snapshot velho depois de update novo |
| Redis/broker indisponível | Proxies TCP exclusivos congelam/cortam conexões do teste; nunca parar container/serviço compartilhado; 503/readiness falsa, nenhum ACK antes de efeito, reconexão/backoff e convergência |
| Redis loss/catálogo | Nova geração/prefixo vazio simula perda só do teste; reconstrução por HTTP completo inclui silos/janelas antigos; coleta parcial, divergente, oversized ou conflito não ativa geração; Core sob mutação não é anunciado como snapshot global atômico |
| Lentidão e encerramento | Um cliente lento estoura limites e é desconectado sem bloquear segundo cliente/consumer; handshake/confirm/Core/Redis silenciosos expiram; shutdown termina em <=10 s, sem sockets/timers próprios restantes |

Relatório separa aprovado, falhou e não executado, mede latência local sem prometer SLA de produção e preserva fixtures. Build/typecheck/unidade e integração reais estão definidos no package.json e documentados em docs/validation.md.

## 8. Decisões pendentes e dependências

1. Coordenação aprovou `per_aggregate_bounded` e os defaults para implementar; revisão independente da implementação é o próximo gate. Se a Web precisar de corte global atual ou prova de atraso máximo, definir com Core snapshot atômico/watermark/replay; não alterar Core nesta tarefa.
2. Confirmar limites de catálogo, buffers, tempos e memória/eviction do Redis para a carga esperada. Não há garantia de disponibilidade durante mutação contínua ou falha prolongada. TTL/retencão de dedup/quarentena e operação de redrive continuam pendentes.
3. Gateway futuro deve preservar `/live`, upgrade/subprotocolo, timeouts compatíveis com heartbeat, `/v1/live/*`, no-store, correlação e limites de resposta; TLS/WSS, autenticação/autorização e acesso externo precisam de etapa própria. Não expor este servidor anônimo como solução de produção.
4. Web deve implementar snapshot inicial, versões/generation/revision, checkpoint/gap/resync, sinalização de indisponibilidade e reconexão sem polling. Agendamento continua pelo Core; saldo Live não autoriza operação. Não renderizar reserva como estoque físico.
5. Definir identidade, sessão, tokens, revogação, logout, expiração e payloads versionados `agrotrac.live.session.*` para Audit em etapa futura completa. Audit hoje aceita envelope Live, mas não inventa schema de sessão; nenhuma publicação é autorizada aqui.
6. Atualização recebida da coordenação nesta revisão: usuário relatou testes físicos iPhone/Expo Go/tunnel aprovados para abertura, modo avião, registro de pesagem offline/lista e persistência após fechar completamente/reabrir Expo Go. Evidência manual relatada é distinta dos automatizados Windows; não inferir validação de câmera/permissões/bytes de foto, recuperação IN_FLIGHT ou sync nativo. Android nativo, FormData/sincronização real via Gateway e API acessível autorizada continuam pendentes. Live não resolve essas pendências, não reduz validações Mobile/Audit e não abre API/túnel por causa delas.

O primeiro incremento foi implementado sob a nova autorização da coordenação, exclusivamente no Live. Gateway/Web e sessões/autenticação permanecem pendentes; revisão independente da implementação é o próximo passo.
