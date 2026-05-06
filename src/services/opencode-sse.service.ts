import { Injectable, Logger, OnApplicationBootstrap, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { MqttClient } from 'mqtt';

interface PendingSession {
  sessionID: string;
  messageID: string;
  replyToTopic: string;
  userProperties: any;
  originalPayload: any;
  createdAt: number;
  status: 'pending' | 'processing' | 'completed' | 'error';
  accumulatedText: string;
  intermediateResults: Array<{
    type: string;
    content: string;
    timestamp: number;
    partID?: string;
  }>;
}

@Injectable()
export class OpenCodeSSEService implements OnApplicationBootstrap, OnApplicationShutdown, OnModuleInit {
  private readonly logger = new Logger(OpenCodeSSEService.name);
  private pendingSessions: Map<string, PendingSession> = new Map();
  private reconnectTimer: NodeJS.Timeout | null = null;
  private abortController: AbortController | null = null;

  constructor(
    private configService: ConfigService,
    private eventEmitter: EventEmitter2,
  ) {}

  onModuleInit() {
    // Register MQTT publish handler
    this.eventEmitter.on('sse.publishToMqtt', (data: any) => {
      this.eventEmitter.emit('mqtt.publish', data);
    });
  }

  async onApplicationBootstrap() {
    this.logger.log('Initializing OpenCode SSE Service...');
    
    // Start SSE event subscription
    this.subscribeToSSEEvents();
    
    this.logger.log('OpenCode SSE Service initialized');
  }

  /**
   * 订阅SSE事件流
   */
  private subscribeToSSEEvents() {
    const baseUrl = this.configService.get('mqtt.opencodeBaseUrl') || 'http://localhost:4096';
    const eventUrl = `${baseUrl}/event`;
    
    this.logger.log(`Connecting to SSE event stream: ${eventUrl}`);
    
    // 使用Node.js的EventSource polyfill或直接使用fetch
    // 这里使用简单的实现，实际项目中可以使用 eventsource 包
    this.setupEventSource(eventUrl);
  }

  /**
   * 设置EventSource连接
   */
  private setupEventSource(url: string) {
    // 在Node.js环境中，我们需要使用fetch或专门的EventSource实现
    // 这里简化处理，实际应该使用 eventsource 或 eventsource-polyfill 包
    
    this.logger.log('Setting up SSE event listener...');
    
    // 使用fetch API实现SSE
    this.listenToSSEWithFetch(url);
  }

  /**
   * 使用Fetch API监听SSE事件
   */
  private async listenToSSEWithFetch(url: string) {
    try {
      this.abortController = new AbortController();
      
      const headers: any = {
        'Accept': 'text/event-stream',
        'Cache-Control': 'no-cache',
        ...(this.configService.get('mqtt.opencodeApiKey') && {
          'Authorization': `Bearer ${this.configService.get('mqtt.opencodeApiKey')}`
        })
      };

      const response = await fetch(url, {
        method: 'GET',
        headers,
        signal: this.abortController.signal
      });

      if (!response.ok) {
        throw new Error(`SSE connection failed: ${response.status}`);
      }

      const reader = response.body?.getReader();
      const decoder = new TextDecoder();
      let buffer = '';

      this.logger.log('SSE event stream connected successfully');

      while (reader) {
        const { done, value } = await reader.read();
        
        if (done) {
          this.logger.warn('SSE event stream ended, reconnecting...');
          this.scheduleReconnect();
          break;
        }

        buffer += decoder.decode(value, { stream: true });
        
        // 解析SSE事件
        const lines = buffer.split('\n');
        buffer = lines.pop() || ''; // 保留不完整的行
        
        for (const line of lines) {
          if (line.startsWith('data: ')) {
            const data = line.substring(6);
            try {
              const event = JSON.parse(data);
              this.handleSSEEvent(event);
            } catch (error: any) {
              this.logger.warn(`Failed to parse SSE event: ${error.message}`);
            }
          }
        }
      }
    } catch (error: any) {
      if (error.name === 'AbortError') {
        this.logger.log('SSE connection aborted during shutdown');
        return;
      }
      this.logger.error(`SSE connection error: ${error.message}`);
      this.scheduleReconnect();
    }
  }

  /**
   * 处理SSE事件
   */
  private handleSSEEvent(event: any) {
    const props = event.properties || {};
    this.logger.debug(`Received SSE event: ${event.type}, sessionID: ${props.sessionID || 'N/A'}`);
    
    switch (event.type) {
      case 'message.updated':
        this.handleMessageUpdated(event);
        break;
      case 'message.part.delta':
        this.handleMessagePartDelta(event);
        break;
      case 'message.part.updated':
        this.handleMessagePartUpdated(event);
        break;
      case 'session.idle':
        this.handleSessionIdle(event);
        break;
      case 'session.status':
        this.handleSessionStatus(event);
        break;
      case 'session.updated':
        this.handleSessionUpdated(event);
        break;
      case 'session.diff':
        this.handleSessionDiff(event);
        break;
      case 'session.error':
        this.handleMessageError(event);
        break;
      case 'server.heartbeat':
        // Ignore heartbeat
        break;
      case 'error':
        this.handleMessageError(event);
        break;
      default:
        this.logger.debug(`Unhandled event type: ${event.type}`);
    }
  }

  /**
   * 处理消息part增量事件（流式输出）
   */
  private handleMessagePartDelta(event: any) {
    const props = event.properties || {};
    const { sessionID, messageID, partID, field, delta } = props;
    
    const pendingSession = this.pendingSessions.get(sessionID);
    // 注意：事件中的 messageID 是 Assistant 消息的 ID，而 pendingSession.messageID 是 User 消息的 ID
    // 所以这里只需要检查 sessionID 是否匹配，且会话处于 pending 或 processing 状态
    if (!pendingSession || (pendingSession.status !== 'pending' && pendingSession.status !== 'processing')) {
      return;
    }

    // 标记状态为 processing
    if (pendingSession.status === 'pending') {
      pendingSession.status = 'processing';
    }

    // 累积文本内容（delta 是字符串，不是对象）
    if (delta && (field === 'text' || !field)) {
      pendingSession.accumulatedText += delta;
      pendingSession.intermediateResults.push({
        type: 'delta_text',
        content: delta,
        timestamp: Date.now(),
        partID
      });

      this.logger.debug(`Accumulated delta for session ${sessionID}, partID: ${partID}, total length: ${pendingSession.accumulatedText.length}`);

      // 发送流式更新（可选）
      if (pendingSession.userProperties.send_partial_results) {
        this.sendIntermediateResult(pendingSession, {
          type: 'partial_text',
          content: delta,
          timestamp: Date.now()
        });
      }
    }
  }

  /**
   * 处理消息更新事件
   */
  private handleMessageUpdated(event: any) {
    const props = event.properties || {};
    const { sessionID, info } = props;
    const { id, role, parentID } = info || {};
    
    // 检查是否是我们正在等待的会话
    const pendingSession = this.pendingSessions.get(sessionID);
    if (!pendingSession) {
      return;
    }

    this.logger.debug(`Message updated for session ${sessionID}: id=${id}, role=${role}, parentID=${parentID}`);
    
    // 如果有消息更新，标记为processing（无论是user还是assistant）
    if (pendingSession.status === 'pending') {
      pendingSession.status = 'processing';
      this.logger.debug(`Session ${sessionID} marked as processing`);
    }
  }

  /**
   * 处理消息部分更新事件
   */
  private handleMessagePartUpdated(event: any) {
    const props = event.properties || {};
    const { sessionID, part } = props;
    
    // 查找对应的会话
    const pendingSession = this.pendingSessions.get(sessionID);
    if (!pendingSession) {
      return;
    }

    // 提取文本内容
    if (part?.type === 'text' && part?.text) {
      // 检查是否已经累积过这个part
      const alreadyExists = pendingSession.intermediateResults.some(
        r => r.type === 'part_text' && r.partID === part.id
      );

      if (!alreadyExists) {
        pendingSession.intermediateResults.push({
          type: 'part_text',
          content: part.text,
          timestamp: Date.now(),
          partID: part.id
        });
      }
    }
  }

  /**
   * 处理会话空闲事件（表示AI处理完成）
   */
  private handleSessionIdle(event: any) {
    const props = event.properties || {};
    const { sessionID } = props;
    
    const pendingSession = this.pendingSessions.get(sessionID);
    if (!pendingSession) {
      return;
    }

    this.logger.log(`Session ${sessionID} idle - AI processing completed, status: ${pendingSession.status}`);
    
    // 如果是processing状态或已累积了文本，立即获取响应
    // 否则等待一小段时间让消息持久化
    if (pendingSession.status === 'processing' || pendingSession.accumulatedText.length > 0) {
      this.getFinalResponse(sessionID, pendingSession);
    } else {
      // 短暂延迟确保消息已写入
      setTimeout(() => {
        const session = this.pendingSessions.get(sessionID);
        if (session && session.status !== 'completed') {
          this.logger.debug(`Delayed fetch for session ${sessionID}`);
          this.getFinalResponse(sessionID, session);
        }
      }, 1000);
    }
  }

  /**
   * 处理会话状态事件
   */
  private handleSessionStatus(event: any) {
    const props = event.properties || {};
    const { sessionID, status } = props;
    
    const pendingSession = this.pendingSessions.get(sessionID);
    if (!pendingSession) {
      return;
    }

    this.logger.debug(`Session ${sessionID} status: ${JSON.stringify(status)}`);
  }

  /**
   * 处理会话更新事件
   */
  private handleSessionUpdated(event: any) {
    const props = event.properties || {};
    const { sessionID } = props;
    
    const pendingSession = this.pendingSessions.get(sessionID);
    if (!pendingSession) {
      return;
    }

    this.logger.debug(`Session ${sessionID} updated`);
  }

  /**
   * 处理会话diff事件
   */
  private handleSessionDiff(event: any) {
    const props = event.properties || {};
    const { sessionID, diff } = props;
    
    const pendingSession = this.pendingSessions.get(sessionID);
    if (!pendingSession) {
      return;
    }

    this.logger.debug(`Session ${sessionID} diff: ${diff?.length || 0} files changed`);
  }

  /**
   * 处理错误事件
   */
  private handleMessageError(event: any) {
    const props = event.properties || {};
    const { sessionID, error } = props;
    
    const pendingSession = this.pendingSessions.get(sessionID);
    if (!pendingSession) {
      return;
    }

    this.logger.error(`Error in session ${sessionID}: ${JSON.stringify(error)}`);
    
    pendingSession.status = 'error';
    
    // 发送错误响应
    this.sendErrorResponse(pendingSession, error);
  }

  /**
   * 获取最终响应
   */
  private async getFinalResponse(sessionID: string, pendingSession: PendingSession) {
    try {
      pendingSession.status = 'completed';
      
      let responseText = pendingSession.accumulatedText;
      let tokens: any;
      let cost: any;

      this.logger.debug(`Session ${sessionID} accumulated text length: ${responseText.length}`);

      // 如果没有累积的文本，通过API获取
      if (!responseText || responseText.length === 0) {
        const baseUrl = this.configService.get('mqtt.opencodeBaseUrl') || 'http://localhost:4096';
        const headers: any = {
          'Accept': 'application/json'
        };
        
        if (this.configService.get('mqtt.opencodeApiKey')) {
          headers['Authorization'] = `Bearer ${this.configService.get('mqtt.opencodeApiKey')}`;
        }

        this.logger.debug(`Fetching messages from API: ${baseUrl}/session/${sessionID}/message`);
        
        const response = await fetch(`${baseUrl}/session/${sessionID}/message`, {
          method: 'GET',
          headers
        });

        if (!response.ok) {
          throw new Error(`Failed to fetch messages: ${response.status}`);
        }

        const messages = await response.json();
        
        // 处理可能的不同响应格式
        const messageList = Array.isArray(messages) ? messages : (messages.data || messages.messages || []);
        this.logger.debug(`API returned ${messageList?.length || 0} messages`);

        // 尝试多种方式查找assistant消息
        let assistantMessage: any = null;

        // 方式1: 查找parentID匹配的assistant消息
        assistantMessage = messageList?.find((m: any) => 
          m.info?.role === 'assistant' && m.info?.parentID === pendingSession.messageID
        );

        // 方式2: 如果没找到，查找最新的assistant消息
        if (!assistantMessage) {
          assistantMessage = messageList?.find((m: any) => m.info?.role === 'assistant');
          if (assistantMessage) {
            this.logger.debug(`Found assistant message without parentID match`);
          }
        }

        // 方式3: 检查消息是否在parts中
        if (!assistantMessage) {
          for (const m of messageList || []) {
            const parts = m.parts || [];
            const hasText = parts.some((p: any) => p.type === 'text' && p.text);
            if (hasText) {
              assistantMessage = m;
              this.logger.debug(`Found message with text parts`);
              break;
            }
          }
        }

        if (assistantMessage) {
          this.logger.debug(`Assistant message found, ID: ${assistantMessage.info?.id}`);
          this.logger.debug(`Parts count: ${assistantMessage.parts?.length || 0}`);
          
          // 提取所有text类型的part
          const parts = assistantMessage.parts || [];
          const textParts = parts
            .filter((p: any) => p.type === 'text')
            .map((p: any) => {
              this.logger.debug(`Part text length: ${p.text?.length || 0}`);
              return p.text;
            })
            .filter(Boolean)
            .join('') || '';

          responseText = textParts;
          tokens = assistantMessage.info?.tokens;
          cost = assistantMessage.info?.cost;
          
          this.logger.debug(`Extracted response text length: ${responseText.length}`);
        } else {
          this.logger.warn(`No assistant message found in session ${sessionID}`);
          this.logger.debug(`Messages structure: ${JSON.stringify(messageList?.map((m: any) => ({ id: m.info?.id, role: m.info?.role, parentID: m.info?.parentID, partsCount: m.parts?.length })))}`);
        }
      }

      // 构建响应消息
      const responseMessage = {
        id: pendingSession.messageID,
        text: responseText,
        senderId: this.configService.get('mqtt.clientId') || 'opencode-agent',
        kind: 'final',
        ts: Date.now(),
        originalMessageId: pendingSession.originalPayload.id,
        processedAt: new Date().toISOString(),
        status: 'success',
        tokens,
        cost
      };

      // 通过MQTT发送响应
      this.sendMQTTResponse(pendingSession, responseMessage);
      
      // 清理会话
      this.pendingSessions.delete(sessionID);
    } catch (error: any) {
      this.logger.error(`Failed to get final response: ${error.message}`);
      this.sendErrorResponse(pendingSession, error.message);
    }
  }

  /**
   * 发送中间结果
   */
  private sendIntermediateResult(pendingSession: PendingSession, result: any) {
    const intermediateMessage = {
      id: pendingSession.messageID,
      text: result.content,
      senderId: this.configService.get('mqtt.clientId') || 'opencode-agent',
      kind: 'intermediate',
      ts: result.timestamp,
      originalMessageId: pendingSession.originalPayload.id,
      type: result.type
    };

    this.sendMQTTMessage(pendingSession.replyToTopic, intermediateMessage, pendingSession.userProperties);
  }

  /**
   * 发送MQTT响应
   */
  private sendMQTTResponse(pendingSession: PendingSession, response: any) {
    this.sendMQTTMessage(pendingSession.replyToTopic, response, pendingSession.userProperties);
  }

  /**
   * 发送MQTT消息
   */
  private sendMQTTMessage(topic: string, message: any, userProperties: any) {
    this.logger.log(`Publishing to ${topic}: ${JSON.stringify(message)}`);
    
    // Emit event for MQTT subscriber to handle the actual publishing
    this.eventEmitter.emit('mqtt.publish', {
      topic,
      message: JSON.stringify(message),
      options: {
        qos: 1,
        properties: {
          userProperties: {
            name: this.configService.get('mqtt.properties.userProperties.name') || 'opencode-agent',
            description: this.configService.get('mqtt.properties.userProperties.description') || 'Advanced Developer',
            emoji: this.configService.get('mqtt.properties.userProperties.emoji') || '🤖',
          }
        }
      }
    });
  }

  /**
   * 发送错误响应
   */
  private sendErrorResponse(pendingSession: PendingSession, error: any) {
    const errorMessage = {
      id: pendingSession.messageID,
      text: `Error: ${typeof error === 'string' ? error : JSON.stringify(error)}`,
      senderId: this.configService.get('mqtt.clientId') || 'opencode-agent',
      kind: 'error',
      ts: Date.now(),
      originalMessageId: pendingSession.originalPayload.id,
      processedAt: new Date().toISOString(),
      status: 'error'
    };

    this.sendMQTTResponse(pendingSession, errorMessage);
    this.pendingSessions.delete(pendingSession.sessionID);
  }

  /**
   * 注册新的待处理会话
   */
  public async registerPendingSession(
    sessionID: string,
    messageID: string,
    replyToTopic: string,
    userProperties: any,
    originalPayload: any
  ): Promise<void> {
    const pendingSession: PendingSession = {
      sessionID,
      messageID,
      replyToTopic,
      userProperties,
      originalPayload,
      createdAt: Date.now(),
      status: 'pending',
      accumulatedText: '',
      intermediateResults: []
    };

    this.pendingSessions.set(sessionID, pendingSession);
    
    this.logger.log(`Registered pending session: ${sessionID} -> ${replyToTopic}`);
    
    // 设置超时清理
    setTimeout(() => {
      const session = this.pendingSessions.get(sessionID);
      if (session && (session.status === 'pending' || session.status === 'processing')) {
        this.logger.warn(`Session ${sessionID} timed out after 60s`);
        this.sendErrorResponse(session, 'Processing timeout');
      }
    }, 60000);
  }

  /**
   * 获取待处理会话数量
   */
  public getPendingCount(): number {
    return this.pendingSessions.size;
  }

  /**
   * 获取待处理会话列表
   */
  public getPendingSessions(): PendingSession[] {
    return Array.from(this.pendingSessions.values());
  }

  /**
   * 安排重连
   */
  private scheduleReconnect() {
    if (this.reconnectTimer) {
      return;
    }

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.logger.log('Reconnecting to SSE event stream...');
      this.subscribeToSSEEvents();
    }, 5000);
  }

  async onApplicationShutdown() {
    this.logger.log('Shutting down OpenCode SSE Service...');
    
    // Abort the SSE connection
    if (this.abortController) {
      this.abortController.abort();
      this.abortController = null;
    }
    
    // Clean up reconnect timer
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
    }
    
    // Clean up all pending sessions
    this.pendingSessions.clear();
  }
}