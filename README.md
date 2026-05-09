# OpenCode MQTT Service

基于 MQTT v5.0 协议的 OpenCode AI 代理服务，接收私聊消息并通过 OpenCode AI 异步处理，将结果返回给发送方。

## 架构

```
MQTT Client ──publish──▶ MQTT Broker ──subscribe──▶ opencode-mqtt ──promptAsync──▶ OpenCode Server
                                                                          │
                          MQTT Broker ◀──publish─── opencode-mqtt ◀──SSE event──┘
```

### 核心流程

1. 订阅私聊主题（如 `opencode-agent/inbound`）
2. 收到消息后调用 OpenCode `promptAsync` API（立即返回 204）
3. 通过 SSE 事件流监听 `message.part.delta` 累积 AI 响应
4. 收到 `session.idle` 事件后，将汇总文本通过 MQTT 发送至 `reply_to` 主题

## 消息格式

### 接收消息

```typescript
interface MqttMessage {
  id: string;
  text?: string;
  senderId: string;
  timestamp: number | string;
  targetIds?: string[];
  type?: 'text' | 'file';
  fileName?: string;
  fileType?: string;
  fileData?: string;
}
```

### 回复消息

```typescript
{
  id: string;            // 消息ID
  text: string;          // AI 响应文本
  senderId: string;      // MQTT 客户端ID
  kind: 'final' | 'intermediate' | 'error';
  ts: number;            // 时间戳
  originalMessageId: string;
  status: 'success' | 'error';
}
```

### MQTT v5.0 User Properties

发送方在消息中需携带 `reply_to` 属性，服务据此回复：

```typescript
{
  userProperties: {
    reply_to: 'web-viewer/inbound',  // 回复主题
    name: 'web-viewer',
    // ...
  }
}
```

回复消息携带以下 User Properties：

| 属性 | 来源 | 说明 |
|------|------|------|
| `name` | `MQTT_USER_PROPERTIES_NAME` | 代理名称 |
| `description` | `MQTT_USER_PROPERTIES_DESCRIPTION` | 代理描述 |
| `emoji` | `MQTT_USER_PROPERTIES_EMOJI` | 代理图标 |

## 项目结构

```
src/
├── main.ts                              # 入口，初始化 NestJS 应用
├── app.module.ts                        # 根模块
├── config/
│   └── mqtt.config.ts                   # MQTT 及 OpenCode 配置
├── interfaces/
│   └── mqtt-message.interface.ts        # 消息格式定义
└── services/
    ├── mqtt-subscriber.service.ts       # MQTT 订阅、消息接收与路由
    ├── opencode.service.ts              # OpenCode API 调用（创建会话、promptAsync）
    ├── opencode-sse.service.ts          # SSE 事件监听、文本累积、MQTT 回复
    └── opencode-integration.service.ts  # 旧版集成服务（未使用）
```

## 配置

复制 `.env.example` 为 `.env` 并修改：

```bash
cp .env.example .env
```

### 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `MQTT_HOST` | `localhost` | MQTT Broker 地址 |
| `MQTT_PORT` | `1883` | MQTT Broker 端口 |
| `MQTT_USERNAME` | - | MQTT 认证用户名 |
| `MQTT_PASSWORD` | - | MQTT 认证密码 |
| `MQTT_CLIENT_ID` | 随机生成 | MQTT 客户端ID，同时作为回复消息的 `senderId` |
| `MQTT_PRIVATE_CHAT_TOPIC` | `private/chat/+` | 订阅的私聊主题 |
| `MQTT_USER_PROPERTIES_NAME` | `nestjs-mqtt-service` | 回复消息 User Properties 的 name |
| `MQTT_USER_PROPERTIES_DESCRIPTION` | `NestJS MQTT Microservice Client` | 回复消息 User Properties 的 description |
| `MQTT_USER_PROPERTIES_EMOJI` | `🤖` | 回复消息 User Properties 的 emoji |
| `OPENCODE_BASE_URL` | `http://localhost:4096` | OpenCode Server 地址 |
| `OPENCODE_API_KEY` | - | OpenCode API 密钥 |
| `OPENCODE_MODEL` | - | 模型，格式 `provider_id/model_id`，如 `anthropic/claude-sonnet-4-5` |
| `OPENCODE_WORKSPACE_DIR` | `process.cwd()` | OpenCode 工作目录 |

## 运行

### 本地开发

```bash
npm install
npm run start:dev
```

### Docker 部署

**1. 构建镜像**

```bash
docker build -f docker/Dockerfile -t opencode-mqtt:latest .
```

**2. 启动服务**

```bash
cd docker
docker compose up -d
```

**3. 查看日志**

```bash
docker compose logs -f
```

### docker-compose 服务说明

| 服务 | 镜像 | 说明 |
|------|------|------|
| `opencode-mqtt` | 本地构建 | MQTT 代理服务 |
| `opencode` | `ghcr.io/anomalyco/opencode:latest` | OpenCode AI Server |

两个服务共享 `/opt/workspace` 卷作为工作目录。

## SSE 事件处理

服务通过 SSE 连接 OpenCode Server 的 `/event` 端点，监听以下事件：

| 事件 | 说明 | 处理 |
|------|------|------|
| `message.part.delta` | AI 流式输出增量 | 累积 `delta` 到 `accumulatedText` |
| `message.part.updated` | Part 更新 | 记录完整 part 文本 |
| `message.updated` | 消息状态变更 | 标记会话为 processing |
| `session.idle` | AI 处理完成 | 触发最终响应发送 |
| `session.status` | 会话状态 | 日志记录 |
| `session.error` | 会话错误 | 发送错误响应 |
| `server.heartbeat` | 心跳 | 忽略 |

事件数据嵌套在 `properties` 字段中：

```json
{
  "type": "session.idle",
  "properties": {
    "sessionID": "ses_xxx"
  }
}
```

## 超时机制

- **SSE 会话等待超时**：300 秒（5 分钟）
- **HTTP 请求超时**：30 秒
- 超时后通过 MQTT 发送 `kind: 'error'` 消息至 `reply_to` 主题

## 前置要求

- Node.js >= 20
- MQTT Broker（如 EMQX、Mosquitto）
- OpenCode Server（`opencode serve`）
