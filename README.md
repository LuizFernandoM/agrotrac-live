# AgroTrac Live — Serviço Live B (Sessão, Cache e Tempo Real)

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
