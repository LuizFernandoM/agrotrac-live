# AgroTrac Live — projeção e tempo real local

Primeiro incremento implementado: Core/outbox → RabbitMQ → Redis versionado → WebSocket `/live`. REST: `GET /v1/live/snapshot`, `/health/live` e `/health/ready`. Uma instância, **127.0.0.1:3002**, sem autenticação pronta ou exposição externa. `LIVE_HOST` externo e `LIVE_PORT` diferente de 3002 são recusados. Somente fixtures usam portas loopback efêmeras.

Projeção eventual `per_aggregate_bounded`, versões independentes de silo/janela, sem corte global ou garantia de atraso máximo. Live não autoriza reserva/capacidade: comandos continuam no Core. Sessões, tokens, login/logout, revogação/expiração, eventos `agrotrac.live.session.*`, Gateway/WSS e Web continuam requisitos futuros completos, ainda pendentes. Nenhuma identidade fictícia foi criada. O roteiro original no fim deste README descreve essas responsabilidades futuras, não funcionalidades entregues.

## Executar e verificar

Node 22, validado 22.14.0; dependências locais fixadas no lockfile:

```powershell
npm ci
npm run build
npm run typecheck
npm test
```

Unidade não requer Docker/rede. Para iniciar localmente, fornecer credenciais pelo ambiente ou escolher explicitamente arquivo dotenv (nunca carregado automaticamente):

```powershell
$env:LIVE_INFRA_ENV_PATH = (Resolve-Path '../agrotrac-infra/.env').Path
$env:LIVE_CORE_URL = 'http://127.0.0.1:3001'
npm start
```

Core HTTP/Redis/Rabbit precisam estar disponíveis. Startup não inicia containers, migra Core ou abre túnel. Readiness é 503 até coleta completa estável, broker conectado e cache íntegro. Não servir snapshot parcial. Shutdown SIGINT/SIGTERM fecha somente recursos próprios.

## Defaults e limites implementados

Valores numéricos devem ser inteiros positivos; defaults também são tetos:

| Ambiente | Default / função |
| --- | --- |
| LIVE_INFRA_ENV_PATH | ausente; opt-in, processo tem precedência |
| LIVE_REDIS_URL | redis://127.0.0.1:6379 |
| RABBITMQ_URL | explícita ou RABBITMQ_DEFAULT_USER/PASS do env opt-in, host local 5672 |
| LIVE_CORE_URL | http://127.0.0.1:3001, sem credenciais/query/hash |
| LIVE_REDIS_PREFIX | agrotrac:live:v1:, cada geração recebe UUID |
| LIVE_IO_TIMEOUT_MS | 5000 ms |
| LIVE_MAX_ENTITIES | 10000 por tipo |
| LIVE_MAX_SNAPSHOT_BYTES | 10485760 bytes, sem truncamento |
| LIVE_SCAN_INTERVAL_MS | 60000 ms; cobertura verificada vence em 120000 ms |
| LIVE_CHECKPOINT_MS | 10000 ms |
| LIVE_SEND_TIMEOUT_MS | 5000 ms |
| LIVE_BUFFER_BYTES | 2097152 bytes de updates por cliente |
| LIVE_BUFFER_UPDATES | 100 enquanto envia snapshot |

Até 6 coletas/60 s, exigindo duas completas consecutivas iguais. Não há SQL entre serviços. Mudança contínua pode impedir estabilização. Redis exige `maxmemory-policy=noeviction`, inspecionado somente por CONFIG GET. Não há TTL ou limpeza automática; memória/retencão de gerações/dedup exigem planejamento posterior. Timeout Redis descarta a conexão e reconstrói em geração nova via Core HTTP.

Rabbit canônico: topic durable `agrotrac.events`, fila durable `agrotrac.live.events`, binding **agrotrac.core.#**, prevalecendo sobre o wildcard antigo no roteiro abaixo. Nenhum argumento novo na fila operacional. Quarentena própria durable `agrotrac.live.quarantine` (direct) → `agrotrac.live.quarantine.events`, chave `rejected`. ACK após efeito Redis ou quarentena persistente confirmada/roteada (mandatory). Retry técnico sem descarte/hotloop. Não há produtor Audit nem publicação de sessão Live neste incremento.

## WebSocket para futura Web

Conectar `ws://127.0.0.1:3002/live` com subprotocolo **agrotrac.live.v1**. Primeiro frame: snapshot inteiro. Updates: generation, previous_revision, revision e snapshots selecionados; event_id apenas quando originados de evento Core. Cada frame tem protocol_version 1.0.0. Checkpoint detecta último broadcast perdido mesmo sem evento sucessor.

Cliente deve substituir estado no snapshot, aplicar revisões contíguas/mesma geração e versões por agregado, ignorar duplicatas e enviar `{protocol_version:'1.0.0',type:'resync'}` em gap/checkpoint adiantado. Sem checkpoint por 20 s, reconectar com backoff 1–30 s/jitter. Toda reconexão recebe snapshot. Sem polling REST periódico. Sem tokens fictícios. Payload só silos/janelas; sem motorista, booking ou foto. Erros e fechamentos estão em contracts.md.

## Integração real opt-in

Infra existente e fixtures novas isoladas. O PostgreSQL Docker de teste usa relay aprovado 127.0.0.1:15432; não usar PostgreSQL Windows 5432.

```powershell
docker start agrotrac-core-test-relay-cf6006
$env:LIVE_INTEGRATION = '1'
$env:LIVE_INFRA_ENV_PATH = (Resolve-Path '../agrotrac-infra/.env').Path
npm run test:integration
docker stop agrotrac-core-test-relay-cf6006
```

Se iniciou o relay, pará-lo no fim inclusive em falha, preservando container e restante da infra. Testes importam Core real somente em testes e criam schema/outbox, HTTP TCP, topologias UUID, prefixos Redis e clientes WS reais. Não executam suítes completas Core/Audit nem alteram seus arquivos. Não há FLUSH, limpeza DEL, DROP/TRUNCATE, purge/delete de filas ou remoção de volumes. Proxies TCP próprios simulam falhas sem parar serviços compartilhados. Recursos e `tests/fixtures/<run>/result.json` são preservados; nenhum segredo vai para logs/relatórios.

[Contrato](docs/contracts.md), [mapa técnico](docs/implementation.md), [validação](docs/validation.md). Próximo gate: revisão independente da coordenação, sem iniciar Gateway/Web. Mobile iPhone tem aprovação relatada para offline/persistência; câmera detalhada/sync Gateway não foram extrapolados nem o aparelho foi tocado.

---

# Roteiro original — responsabilidades futuras de sessão/autenticação

Microsserviço responsável pela performance, sessões efêmeras e comunicação ao vivo de baixa latência do sistema **AgroTrac**, em estrita conformidade com a especificação original (`acac36b6`).

---

## 1. Responsabilidades

- **Gestão de Sessões e Autenticação**: Emissão e validação de tokens efêmeros de sessão para usuários conectados.
- **Cache Dinâmico em Memória**: Armazenamento do estado das vagas e ocupação dos silos no Redis para responder consultas com altíssima velocidade sem sobrecarregar o banco relacional do Core A.
- **Comunicação em Tempo Real**: Manutenção de conexões persistentes WebSockets (WSS) com o Portal Web.
- **Distribuição de Notificações**: Consumo de eventos do RabbitMQ emitidos pelo Core A e propagação imediata aos clientes Web via WebSocket (eliminando polling HTTP periódico).
- **Publicação de Telemetria/Sessões**: Emissão de eventos de sessão (login, logout, desconexões anômalas) para o RabbitMQ para registro histórico no Audit C.

---

## 2. Ferramentas e Tecnologias

- **Runtime**: Node.js com TypeScript.
- **Banco em Memória / Cache**: Redis 7 (chave-valor de alta velocidade).
- **Cliente Redis**: `ioredis` ou `redis`.
- **Servidor WebSocket**: `ws` ou `socket.io`.
- **Broker de Mensageria**: `amqplib` (consumidor e produtor RabbitMQ).
- **Framework Web / HTTP**: Fastify ou Express (rotas de autenticação/sessão).
- **Testes Automatizados**: Vitest ou Jest com mocks/in-memory para Redis e WebSockets.

---

## 3. Arquitetura (Clean Architecture)

A estrutura segue o isolamento de domínio:

```text
agrotrac-live/
├── src/
│   ├── core/                    # Regras puras e Casos de Uso
│   │   ├── domain/              # Entidades de Sessão, Token, Status de Conexão
│   │   │   ├── entities/        # UserSession, ConnectionStatus
│   │   │   ├── errors/          # Erros funcionais (InvalidToken, SessionExpired)
│   │   │   └── repositories/    # Interfaces de cache e sessão (ISessionCache)
│   │   └── use-cases/           # Casos de uso
│   │       ├── AuthenticateUserUseCase.ts
│   │       ├── BroadcastWindowUpdateUseCase.ts
│   │       └── GetCachedOccupancyUseCase.ts
│   ├── infra/                   # Implementações de infraestrutura
│   │   ├── cache/               # Adaptador Redis
│   │   ├── websockets/          # Servidor WebSocket e gerenciador de clientes conectados
│   │   ├── messaging/           # Consumidor RabbitMQ da fila 'agrotrac.live.events'
│   │   └── http/                # Controllers e rotas de login/sessão
│   └── main.ts                  # Inicialização do servidor HTTP e WebSocket
├── tests/
│   └── unit/                    # Testes de casos de uso e gerenciamento de estado
├── package.json
└── tsconfig.json
```

---

## 4. Conexões e Integrações

- **Entrada (HTTP via Gateway)**:
  - `POST /v1/live/auth/login`: Autentica usuário e retorna JWT de sessão.
  - `POST /v1/live/auth/logout`: Invalida sessão ativa no Redis.
- **Entrada (WebSockets WSS via Gateway)**:
  - Rota `/live`: Clientes do Portal Web abrem conexão WSS para receber eventos em tempo real.
- **Entrada (Mensageria RabbitMQ)**:
  - Fila: `agrotrac.live.events` (ligada à exchange `agrotrac.events` ouvindo tópicos `agrotrac.core.*`).
  - Ao receber evento de nova reserva ou bloqueio de janela: atualiza o cache Redis e dispara broadcast para os clientes conectados no WebSocket.
- **Saída (Cache Redis)**:
  - Conexão na porta `6379`.
- **Saída (Mensageria RabbitMQ)**:
  - Publica eventos de sessão (`agrotrac.live.session.started`, `agrotrac.live.session.ended`) consumidos pelo Audit C.
