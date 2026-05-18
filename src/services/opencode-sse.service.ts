import { Injectable, Logger, OnApplicationBootstrap, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { OpencodeClientService } from './opencode-client.service.js';

interface PendingSession {
  sessionID: string;
  messageID: string;
  replyToTopic: string;
  userProperties: any;
  originalPayload: any;
  createdAt: number;
  lastActivity: number;
  inactivityTimer: NodeJS.Timeout | null;
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
  private childToParent: Map<string, string> = new Map();
  private reconnectTimer: NodeJS.Timeout | null = null;
  private abortController: AbortController | null = null;
  private readonly INACTIVITY_TIMEOUT: number;

  constructor(
    private configService: ConfigService,
    private eventEmitter: EventEmitter2,
    private opencodeClient: OpencodeClientService,
  ) {
    this.INACTIVITY_TIMEOUT = this.configService.get('mqtt.sseInactivityTimeout') || 60000;
  }

  onModuleInit() {
    this.eventEmitter.on('sse.publishToMqtt', (data: any) => {
      this.eventEmitter.emit('mqtt.publish', data);
    });

    this.eventEmitter.on('session.closed', (data: { sessionID: string }) => {
      this.logger.log(`Session ${data.sessionID} closed externally, cleaning up SSE tracking`);
      this.cleanupSseTracking(data.sessionID);
    });
  }

  async onApplicationBootstrap() {
    this.logger.log('Initializing OpenCode SSE Service...');
    this.subscribeToSSEEvents();
    this.logger.log('OpenCode SSE Service initialized');
  }

  private subscribeToSSEEvents() {
    const eventUrl = `${this.opencodeClient.baseUrl}/event`;
    this.logger.log(`Connecting to SSE event stream: ${eventUrl}`);
    this.listenToSSEWithFetch(eventUrl);
  }

  private async listenToSSEWithFetch(url: string) {
    try {
      this.abortController = new AbortController();

      const headers: Record<string, string> = {
        'Accept': 'text/event-stream',
        'Cache-Control': 'no-cache',
      };

      if (this.opencodeClient.apiKey) {
        headers['Authorization'] = `Bearer ${this.opencodeClient.apiKey}`;
      }

      const response = await fetch(url, {
        method: 'GET',
        headers,
        signal: this.abortController.signal,
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

        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

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

  private handleSSEEvent(event: any) {
    const props = event.properties || {};
    const sessionID = props.sessionID || props.info?.id;
    this.logger.debug(`Received SSE event: ${event.type}, sessionID: ${sessionID || 'N/A'}`);

    if (sessionID && this.childToParent.has(sessionID)) {
      this.resetInactivityTimer(sessionID);
    }

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
      case 'session.created':
        this.handleSessionCreated(event);
        break;
      case 'session.diff':
        this.handleSessionDiff(event);
        break;
      case 'session.error':
        this.handleMessageError(event);
        break;
      case 'server.heartbeat':
        break;
      case 'error':
        this.handleMessageError(event);
        break;
      default:
        this.logger.debug(`Unhandled event type: ${event.type}`);
    }
  }

  private handleMessagePartDelta(event: any) {
    const props = event.properties || {};
    const { sessionID, messageID, partID, field, delta } = props;

    const pendingSession = this.pendingSessions.get(sessionID);
    if (!pendingSession || (pendingSession.status !== 'pending' && pendingSession.status !== 'processing')) {
      return;
    }

    this.resetInactivityTimer(sessionID);

    if (pendingSession.status === 'pending') {
      pendingSession.status = 'processing';
    }

    if (delta && (field === 'text' || !field)) {
      pendingSession.accumulatedText += delta;
      pendingSession.intermediateResults.push({
        type: 'delta_text',
        content: delta,
        timestamp: Date.now(),
        partID,
      });

      this.logger.debug(`Accumulated delta for session ${sessionID}, partID: ${partID}, total length: ${pendingSession.accumulatedText.length}`);

      if (pendingSession.userProperties.send_partial_results) {
        this.sendIntermediateResult(pendingSession, {
          type: 'partial_text',
          content: delta,
          timestamp: Date.now(),
        });
      }
    }
  }

  private handleMessageUpdated(event: any) {
    const props = event.properties || {};
    const { sessionID, info } = props;
    const { id, role, parentID } = info || {};

    const pendingSession = this.pendingSessions.get(sessionID);
    if (!pendingSession) return;

    this.resetInactivityTimer(sessionID);

    this.logger.debug(`Message updated for session ${sessionID}: id=${id}, role=${role}, parentID=${parentID}`);

    if (pendingSession.status === 'pending') {
      pendingSession.status = 'processing';
      this.logger.debug(`Session ${sessionID} marked as processing`);
    }
  }

  private handleMessagePartUpdated(event: any) {
    const props = event.properties || {};
    const { sessionID, part } = props;

    const pendingSession = this.pendingSessions.get(sessionID);
    if (!pendingSession) return;

    this.resetInactivityTimer(sessionID);

    if (part?.type === 'text' && part?.text) {
      const alreadyExists = pendingSession.intermediateResults.some(
        r => r.type === 'part_text' && r.partID === part.id,
      );

      if (!alreadyExists) {
        pendingSession.intermediateResults.push({
          type: 'part_text',
          content: part.text,
          timestamp: Date.now(),
          partID: part.id,
        });
      }
    }
  }

  private handleSessionIdle(event: any) {
    const props = event.properties || {};
    const { sessionID } = props;

    const pendingSession = this.pendingSessions.get(sessionID);
    if (!pendingSession) return;

    this.resetInactivityTimer(sessionID);

    this.logger.log(`Session ${sessionID} idle - AI processing completed, status: ${pendingSession.status}`);

    if (pendingSession.status === 'processing' || pendingSession.accumulatedText.length > 0) {
      this.getFinalResponse(sessionID, pendingSession);
    } else {
      setTimeout(() => {
        const session = this.pendingSessions.get(sessionID);
        if (session && session.status !== 'completed') {
          this.logger.debug(`Delayed fetch for session ${sessionID}`);
          this.getFinalResponse(sessionID, session);
        }
      }, 1000);
    }
  }

  private handleSessionStatus(event: any) {
    const props = event.properties || {};
    const { sessionID, status } = props;

    const pendingSession = this.pendingSessions.get(sessionID);
    if (!pendingSession) return;

    this.resetInactivityTimer(sessionID);

    this.logger.debug(`Session ${sessionID} status: ${JSON.stringify(status)}`);
  }

  private handleSessionCreated(event: any) {
    const props = event.properties || {};
    const { info } = props;
    const sessionID = info?.id;
    const parentID = info?.parentID;

    if (!sessionID) return;

    if (parentID && this.pendingSessions.has(parentID)) {
      this.childToParent.set(sessionID, parentID);
      this.logger.log(`Child session created: ${sessionID} (parent: ${parentID})`);
    }

    const waitingSession = this.pendingSessions.get(sessionID);
    if (waitingSession) {
      this.resetInactivityTimer(sessionID);
      this.logger.log(`Tracked session created: ${sessionID}`);
    }
  }

  private handleSessionUpdated(event: any) {
    const props = event.properties || {};
    const { sessionID } = props;

    const pendingSession = this.pendingSessions.get(sessionID);
    if (!pendingSession) return;

    this.resetInactivityTimer(sessionID);

    this.logger.debug(`Session ${sessionID} updated`);
  }

  private handleSessionDiff(event: any) {
    const props = event.properties || {};
    const { sessionID, diff } = props;

    const pendingSession = this.pendingSessions.get(sessionID);
    if (!pendingSession) return;

    this.resetInactivityTimer(sessionID);

    this.logger.debug(`Session ${sessionID} diff: ${diff?.length || 0} files changed`);
  }

  private handleMessageError(event: any) {
    const props = event.properties || {};
    const { sessionID, error } = props;

    const pendingSession = this.pendingSessions.get(sessionID);
    if (!pendingSession) return;

    this.resetInactivityTimer(sessionID);

    this.logger.error(`Error in session ${sessionID}: ${JSON.stringify(error)}`);

    pendingSession.status = 'error';

    this.sendErrorResponse(pendingSession, error);
  }

  private async getFinalResponse(sessionID: string, pendingSession: PendingSession) {
    this.logger.log(`[DIAG] getFinalResponse called for session=${sessionID}, replyToTopic=${pendingSession.replyToTopic}`);
    try {
      pendingSession.status = 'completed';

      let responseText = pendingSession.accumulatedText;
      let tokens: any;
      let cost: any;

      this.logger.debug(`Session ${sessionID} accumulated text length: ${responseText.length}`);

      if (!responseText || responseText.length === 0) {
        this.logger.debug(`Fetching messages via SDK for session ${sessionID}`);

        try {
          const messagesResponse: any = await this.opencodeClient.getMessages(sessionID);
          const messages = messagesResponse?.data || messagesResponse || [];
          const messageList = Array.isArray(messages) ? messages : (messages.data || messages.messages || []);
          this.logger.debug(`SDK returned ${messageList?.length || 0} messages`);

          let assistantMessage: any = null;

          assistantMessage = messageList?.find(
            (m: any) => m.info?.role === 'assistant' && m.info?.parentID === pendingSession.messageID,
          );

          if (!assistantMessage) {
            assistantMessage = messageList?.find((m: any) => m.info?.role === 'assistant');
            if (assistantMessage) {
              this.logger.debug(`Found assistant message without parentID match`);
            }
          }

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
            this.logger.debug(
              `Messages structure: ${JSON.stringify(
                messageList?.map((m: any) => ({
                  id: m.info?.id,
                  role: m.info?.role,
                  parentID: m.info?.parentID,
                  partsCount: m.parts?.length,
                })),
              )}`,
            );
          }
        } catch (fetchError: any) {
          this.logger.error(`Failed to fetch messages via SDK: ${fetchError.message}`);
        }
      }

      // 群聊场景：在回复文本前加 @发送者 前缀
      if (pendingSession.originalPayload.senderId &&
          pendingSession.originalPayload.targetIds &&
          pendingSession.originalPayload.targetIds.length > 0) {
        const senderName = pendingSession.originalPayload.senderId;
        if (responseText && responseText.length > 0) {
          responseText = `@${senderName}\n${responseText}`;
        }
      }

      const responseMessage: any = {
        id: pendingSession.messageID,
        text: responseText,
        senderId: this.configService.get('mqtt.clientId') || 'opencode-agent',
        kind: 'final',
        ts: Date.now(),
        originalMessageId: pendingSession.originalPayload.id,
        processedAt: new Date().toISOString(),
        status: 'success',
        tokens,
        cost,
      };

      // 群聊场景：回复携带 targetIds，仅发给发起者
      if (pendingSession.originalPayload.senderId &&
          pendingSession.originalPayload.targetIds &&
          pendingSession.originalPayload.targetIds.length > 0) {
        responseMessage.targetIds = [pendingSession.originalPayload.senderId];
      }

      this.sendMQTTResponse(pendingSession, responseMessage);

      this.clearInactivityTimer(sessionID);

      this.logger.log(
        `Session ${sessionID} reply sent, session kept alive for reuse (${this.configService.get('mqtt.sessionTimeout') / 1000}s idle timeout)`,
      );
    } catch (error: any) {
      this.logger.error(`Failed to get final response: ${error.message}`);
      this.sendErrorResponse(pendingSession, error.message);
    }
  }

  private sendIntermediateResult(pendingSession: PendingSession, result: any) {
    const intermediateMessage: any = {
      id: pendingSession.messageID,
      text: result.content,
      senderId: this.configService.get('mqtt.clientId') || 'opencode-agent',
      kind: 'intermediate',
      ts: result.timestamp,
      originalMessageId: pendingSession.originalPayload.id,
      type: result.type,
    };

    // 仅当原始消息使用 targetIds 指定接收者（群聊场景）时，回复才携带 targetIds
    if (pendingSession.originalPayload.senderId &&
        pendingSession.originalPayload.targetIds &&
        pendingSession.originalPayload.targetIds.length > 0) {
      intermediateMessage.targetIds = [pendingSession.originalPayload.senderId];
    }

    this.sendMQTTMessage(pendingSession.replyToTopic, intermediateMessage, pendingSession.userProperties);
  }

  private sendMQTTResponse(pendingSession: PendingSession, response: any) {
    this.logger.log(`[DIAG] sendMQTTResponse: session=${pendingSession.sessionID}, replyToTopic=${pendingSession.replyToTopic}`);
    this.sendMQTTMessage(pendingSession.replyToTopic, response, pendingSession.userProperties);
  }

  private sendMQTTMessage(topic: string, message: any, userProperties: any) {
    this.logger.log(`Publishing to ${topic}: ${JSON.stringify(message)}`);

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
          },
        },
      },
    });
  }

  private sendErrorResponse(pendingSession: PendingSession, error: any) {
    const errorMessage: any = {
      id: pendingSession.messageID,
      text: `Error: ${typeof error === 'string' ? error : JSON.stringify(error)}`,
      senderId: this.configService.get('mqtt.clientId') || 'opencode-agent',
      kind: 'error',
      ts: Date.now(),
      originalMessageId: pendingSession.originalPayload.id,
      processedAt: new Date().toISOString(),
      status: 'error',
    };

    // 仅当原始消息使用 targetIds 指定接收者（群聊场景）时，回复才携带 targetIds
    if (pendingSession.originalPayload.senderId &&
        pendingSession.originalPayload.targetIds &&
        pendingSession.originalPayload.targetIds.length > 0) {
      errorMessage.targetIds = [pendingSession.originalPayload.senderId];
    }

    this.sendMQTTResponse(pendingSession, errorMessage);
  }

  private resetInactivityTimer(sessionID: string) {
    const parentID = this.childToParent.get(sessionID);
    const effectiveID = parentID || sessionID;

    const session = this.pendingSessions.get(effectiveID);
    if (!session) return;

    session.lastActivity = Date.now();
    if (session.inactivityTimer) {
      clearTimeout(session.inactivityTimer);
    }
    session.inactivityTimer = setTimeout(() => {
      if (session.status === 'pending' || session.status === 'processing') {
        this.logger.warn(`Session ${effectiveID} timed out due to inactivity for ${this.INACTIVITY_TIMEOUT / 1000}s`);
        this.sendErrorResponse(session, 'Processing timeout due to inactivity');
      }
    }, this.INACTIVITY_TIMEOUT);

    if (parentID) {
      this.logger.debug(`Child session ${sessionID} activity reset parent ${effectiveID} timeout`);
    }
  }

  private clearInactivityTimer(sessionID: string) {
    const session = this.pendingSessions.get(sessionID);
    if (session?.inactivityTimer) {
      clearTimeout(session.inactivityTimer);
      session.inactivityTimer = null;
    }
  }

  private cleanupSseTracking(sessionID: string) {
    this.clearInactivityTimer(sessionID);
    for (const [childID, parentID] of this.childToParent.entries()) {
      if (parentID === sessionID) {
        this.childToParent.delete(childID);
      }
    }
    this.pendingSessions.delete(sessionID);
  }

  public cleanupSession(sessionID: string) {
    this.logger.log(`External cleanup requested for session ${sessionID}`);
    this.cleanupSseTracking(sessionID);
  }

  public async registerPendingSession(
    sessionID: string,
    messageID: string,
    replyToTopic: string,
    userProperties: any,
    originalPayload: any,
  ): Promise<void> {
    const existing = this.pendingSessions.get(sessionID);

    if (existing) {
      existing.messageID = messageID;
      existing.replyToTopic = replyToTopic;
      existing.userProperties = userProperties;
      existing.originalPayload = originalPayload;
      existing.accumulatedText = '';
      existing.intermediateResults = [];
      existing.lastActivity = Date.now();
      existing.status = 'pending';
      this.resetInactivityTimer(sessionID);
      this.logger.log(`Updated pending session ${sessionID} for new message ${messageID}`);
      return;
    }

    const pendingSession: PendingSession = {
      sessionID,
      messageID,
      replyToTopic,
      userProperties,
      originalPayload,
      createdAt: Date.now(),
      lastActivity: Date.now(),
      inactivityTimer: null,
      status: 'pending',
      accumulatedText: '',
      intermediateResults: [],
    };

    this.pendingSessions.set(sessionID, pendingSession);
    this.resetInactivityTimer(sessionID);

    this.logger.log(`Registered pending session: ${sessionID} -> ${replyToTopic}, messageID: ${messageID}`);
  }

  public getPendingCount(): number {
    return this.pendingSessions.size;
  }

  public getPendingSessions(): PendingSession[] {
    return Array.from(this.pendingSessions.values());
  }

  private scheduleReconnect() {
    if (this.reconnectTimer) return;

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.logger.log('Reconnecting to SSE event stream...');
      this.subscribeToSSEEvents();
    }, 5000);
  }

  async onApplicationShutdown() {
    this.logger.log('Shutting down OpenCode SSE Service...');

    if (this.abortController) {
      this.abortController.abort();
      this.abortController = null;
    }

    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
    }

    for (const session of this.pendingSessions.values()) {
      if (session.inactivityTimer) {
        clearTimeout(session.inactivityTimer);
      }
    }

    this.pendingSessions.clear();
  }
}
