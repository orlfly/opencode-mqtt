import { Injectable, Logger, OnApplicationBootstrap, OnApplicationShutdown } from '@nestjs/common';
import { MqttClient, connect } from 'mqtt';
import { ConfigService } from '@nestjs/config';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { OpenCodeService } from './opencode.service.js';
import { OpencodeClientService } from './opencode-client.service.js';
import { OpenCodeSSEService } from './opencode-sse.service.js';
import { SessionManagerService } from './session-manager.service.js';
import { MqttMessage } from '../interfaces/mqtt-message.interface.js';
import { getMimeType, tryDecodeBase64Text, isTextFile } from '../utils/file.util.js';

@Injectable()
export class MqttSubscriberService implements OnApplicationBootstrap, OnApplicationShutdown {
  private readonly logger = new Logger(MqttSubscriberService.name);
  private mqttClient: MqttClient;
  private connected = false;

  constructor(
    private configService: ConfigService,
    private opencodeService: OpenCodeService,
    private opencodeClient: OpencodeClientService,
    private sseService: OpenCodeSSEService,
    private sessionManager: SessionManagerService,
    private eventEmitter: EventEmitter2,
  ) {
    this.eventEmitter.on('mqtt.publish', (data: any) => {
      this.publishMessageWithOptions(data.topic, data.message, data.options);
    });
  }

  async onApplicationBootstrap() {
    this.logger.log('Setting up MQTT subscriber service...');

    const mqttHost = this.configService.get('mqtt.host');
    const mqttPort = this.configService.get('mqtt.port');
    const mqttUsername = this.configService.get('mqtt.username');
    const mqttPassword = this.configService.get('mqtt.password');
    const mqttClientId = this.configService.get('mqtt.clientId');
    const mqttClean = this.configService.get('mqtt.clean');
    const mqttConnectTimeout = this.configService.get('mqtt.connectTimeout');
    const mqttReconnectPeriod = this.configService.get('mqtt.reconnectPeriod');
    const mqttProtocolVersion = this.configService.get('mqtt.protocolVersion');

    const brokerUrl = `mqtt://${mqttHost}:${mqttPort}`;

    this.logger.log(`Connecting to MQTT broker at ${brokerUrl}...`);

    this.mqttClient = connect(brokerUrl, {
      clientId: mqttClientId,
      clean: mqttClean,
      connectTimeout: mqttConnectTimeout,
      reconnectPeriod: mqttReconnectPeriod,
      protocolVersion: mqttProtocolVersion,
      username: mqttUsername,
      password: mqttPassword,
      properties: {
        userProperties: {
          name: this.configService.get('mqtt.properties.userProperties.name'),
          description: this.configService.get('mqtt.properties.userProperties.description'),
          emoji: this.configService.get('mqtt.properties.userProperties.emoji'),
        }
      }
    });

    this.mqttClient.setMaxListeners(10);

    this.mqttClient.on('connect', () => {
      this.connected = true;
      this.logger.log('Connected to MQTT broker');
      this.subscribeToTopics();
    });

    this.mqttClient.on('error', (error) => {
      this.logger.error(`MQTT connection error: ${error instanceof Error ? error.message : 'Unknown error'}`);
      this.connected = false;
    });

    this.mqttClient.on('reconnect', () => {
      this.logger.log('Attempting to reconnect to MQTT broker...');
    });

    this.mqttClient.on('close', () => {
      this.logger.log('MQTT connection closed');
      this.connected = false;
    });
  }

  private subscribeToTopics() {
    const privateChatTopic = this.configService.get('mqtt.privateChatTopic');

    if (privateChatTopic) {
      this.mqttClient.subscribe(privateChatTopic, (err) => {
        if (err) {
          this.logger.error(`Failed to subscribe to private chat topic ${privateChatTopic}: ${err.message}`);
        } else {
          this.logger.log(`Successfully subscribed to private chat topic: ${privateChatTopic}`);
        }
      });
    } else {
      this.logger.warn('No private chat topic configured, skipping private chat subscription');
    }

    this.setupMessageHandlers();
  }

  private setupMessageHandlers() {
    this.mqttClient.removeAllListeners('message');

    const privateChatTopicPattern = this.configService.get('mqtt.privateChatTopic');

    this.mqttClient.on('message', (topic, message, packet) => {
      try {
        const payload = JSON.parse(message.toString());

        if (privateChatTopicPattern && this.isTopicMatch(topic, privateChatTopicPattern)) {
          const kind = payload.kind;

          if (kind === 'invite') {
            this.handleGroupInvite(payload, packet).catch(error => {
              this.logger.error(`Error handling group invite: ${error instanceof Error ? error.message : 'Unknown error'}`);
            });
            return;
          }

          if (kind === 'dismissed') {
            this.handleGroupDismiss(payload, packet).catch(error => {
              this.logger.error(`Error handling group dismiss: ${error instanceof Error ? error.message : 'Unknown error'}`);
            });
            return;
          }

          if (kind === 'question_answer') {
            this.handleQuestionAnswer(payload, packet).catch(error => {
              this.logger.error(`Error handling question answer: ${error instanceof Error ? error.message : 'Unknown error'}`);
            });
            return;
          }

          if (kind === 'permission_reply') {
            this.handlePermissionReply(payload, packet).catch(error => {
              this.logger.error(`Error handling permission reply: ${error instanceof Error ? error.message : 'Unknown error'}`);
            });
            return;
          }

          this.handlePrivateChat(payload, packet).catch(error => {
            this.logger.error(`Error handling private chat message: ${error instanceof Error ? error.message : 'Unknown error'}`);
          });
          return;
        }

        this.handleGroupMessage(topic, payload, packet).catch(error => {
          this.logger.error(`Error handling group message: ${error instanceof Error ? error.message : 'Unknown error'}`);
        });
      } catch (error) {
        this.logger.error(`Error processing message on topic ${topic}: ${error instanceof Error ? error.message : 'Unknown error'}`);
      }
    });
  }

  private isTopicMatch(topic: string, pattern: string): boolean {
    if (pattern === topic) {
      return true;
    }

    if (pattern.includes('+')) {
      const patternParts = pattern.split('/');
      const topicParts = topic.split('/');

      if (patternParts.length !== topicParts.length) {
        return false;
      }

      for (let i = 0; i < patternParts.length; i++) {
        if (patternParts[i] !== '+' && patternParts[i] !== topicParts[i]) {
          return false;
        }
      }

      return true;
    }

    if (pattern.includes('#')) {
      const patternParts = pattern.split('/');
      const topicParts = topic.split('/');

      const hashIndex = patternParts.indexOf('#');
      if (hashIndex === -1) return false;

      for (let i = 0; i < hashIndex; i++) {
        if (patternParts[i] !== topicParts[i]) {
          return false;
        }
      }

      return true;
    }

    return false;
  }

  private async handlePrivateChat(payload: any, packet?: any) {
    this.logger.log(`[DIAG] handlePrivateChat entered, payload=${JSON.stringify(payload)}`);

    let replyToTopic: string = '';
    let userProperties: any = {};
    if (packet && packet.properties && packet.properties.userProperties) {
      replyToTopic = packet.properties.userProperties.reply_to || packet.properties.userProperties['reply-to'] || '';
      userProperties = packet.properties.userProperties;
    }

    let sender = payload.senderId || payload.sender || 'unknown';
    let recipient = payload.recipient || 'unknown';
    let message = '';
    let timestamp = payload.timestamp;

    // 检测权限回复消息
    if (payload.kind === 'permission_reply') {
      const requestID = payload.requestID;
      const reply = payload.reply as 'once' | 'always' | 'reject';
      if (!requestID || !reply) {
        this.logger.warn(`Invalid permission reply, missing requestID or reply: ${JSON.stringify(payload)}`);
        return;
      }
      this.logger.log(`Processing permission reply: requestID=${requestID}, reply=${reply}`);
      try {
        await this.opencodeService.replyPermission(requestID, reply);
        this.logger.log(`Permission reply sent successfully: requestID=${requestID}, reply=${reply}`);
      } catch (error: any) {
        this.logger.error(`Permission reply failed: ${error.message}`);
      }
      return;
    }

    const messageType = payload.type || 'text';

    if (messageType === 'file') {
      const fileName = payload.fileName || 'unknown';
      const fileType = payload.fileType || 'application/octet-stream';
      const fileDescription = payload.text || '';
      let fileContentInfo = '';

      if (payload.fileData) {
        const decodedText = tryDecodeBase64Text(payload.fileData);
        if (decodedText && isTextFile(fileName)) {
          const truncated = decodedText.length > 2000
            ? decodedText.substring(0, 2000) + '\n... (truncated)'
            : decodedText;
          fileContentInfo = `\n\nFile content:\n\`\`\`\n${truncated}\n\`\`\``;
        }
      }

      message = `[File shared: ${fileName} (Type: ${fileType})]` +
        (fileDescription ? ` ${fileDescription}` : '') +
        fileContentInfo;

      this.logger.log(`Private file message from ${sender}: ${fileName} (${fileType})`);
    } else {
      message = payload.text || payload.message || JSON.stringify(payload);
      if (typeof message === 'object') {
        message = JSON.stringify(message);
      }
      this.logger.log(`Private message from ${sender} to ${recipient}: ${message.substring(0, 50)}${message.length > 50 ? '...' : ''}`);
    }

    if (!replyToTopic) {
      this.logger.warn('No reply_to topic found in message properties, cannot send response');
      return;
    }

    const senderName = userProperties['name'] || sender;

    const { sessionID, messageID } = this.sessionManager.getOrCreatePrivateSession(
      sender,
      senderName,
      replyToTopic,
    );

    const result = await this.opencodeService.processMqttMessageWithSession(
      this.configService.get('mqtt.privateChatTopic'),
      payload,
      sessionID,
      messageID,
    );

    if (result.success) {
      const actualSessionID = result.sessionId;
      await this.sseService.registerPendingSession(
        actualSessionID,
        messageID,
        replyToTopic,
        userProperties,
        payload,
      );

      this.logger.log(`Session ${actualSessionID} registered for SSE tracking. Response will be sent to: ${replyToTopic}`);
    } else {
      this.logger.error(`Failed to send message to OpenCode: ${result.error}`);

      const errorMessage: any = {
        id: payload.id,
        text: `Error: Failed to process message - ${result.error}`,
        senderId: this.configService.get('mqtt.clientId') || 'opencode-agent',
        kind: 'error',
        ts: Date.now(),
        originalMessageId: payload.id,
        processedAt: new Date().toISOString(),
        status: 'error',
      };

      // 仅当原始消息使用 targetIds 指定接收者（群聊场景）时，回复才携带 targetIds
      if (payload.senderId && payload.targetIds && Array.isArray(payload.targetIds) && payload.targetIds.length > 0) {
        errorMessage.targetIds = [payload.senderId];
      }

      this.mqttClient.publish(replyToTopic, JSON.stringify(errorMessage), {
        qos: 1,
        properties: {
          userProperties: {
            name: this.configService.get('mqtt.properties.userProperties.name') || 'opencode-agent',
            description: this.configService.get('mqtt.properties.userProperties.description') || 'Advanced Developer',
            emoji: this.configService.get('mqtt.properties.userProperties.emoji') || '🤖',
          },
        },
      });
    }
  }

  private async handleGroupInvite(payload: any, packet?: any) {
    this.logger.log(`Received group invite: ${JSON.stringify(payload)}`);

    const groupTopic = payload.topic;
    if (!groupTopic) {
      this.logger.warn('Group invite message missing "topic" field');
      return;
    }

    const sender = payload.senderId || payload.sender || 'unknown';
    this.sessionManager.addGroupMember(groupTopic, sender);

    this.mqttClient.subscribe(groupTopic, (err) => {
      if (err) {
        this.logger.error(`Failed to subscribe to group topic ${groupTopic}: ${err.message}`);
        return;
      }
      this.logger.log(`Subscribed to group topic: ${groupTopic}`);

      const acceptMsg = {
        id: `mqtt-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
        text: 'invite accepted',
        senderId: this.configService.get('mqtt.clientId') || 'opencode-agent',
        kind: 'accept',
        ts: Date.now(),
        topic: groupTopic,
      };
      this.mqttClient.publish(groupTopic, JSON.stringify(acceptMsg), { qos: 1 });
      this.logger.log(`Sent invite accept to group topic: ${groupTopic}`);
    });
  }

  private async handleGroupDismiss(payload: any, packet?: any) {
    this.logger.log(`Received group dismiss: ${JSON.stringify(payload)}`);

    const groupTopic = payload.topic;
    if (!groupTopic) {
      this.logger.warn('Group dismiss message missing "topic" field');
      return;
    }

    const sessionID = this.sessionManager.dismissGroup(groupTopic);
    if (sessionID) {
      this.sseService.cleanupSession(sessionID);
    }

    this.mqttClient.unsubscribe(groupTopic, (err) => {
      if (err) {
        this.logger.error(`Failed to unsubscribe from group topic ${groupTopic}: ${err.message}`);
        return;
      }
      this.logger.log(`Unsubscribed from group topic: ${groupTopic}`);
    });
  }

  private async handleQuestionAnswer(payload: any, packet?: any) {
    const { requestID, answers, reject } = payload;

    if (!requestID) {
      this.logger.warn('Question answer message missing requestID');
      return;
    }

    if (reject) {
      this.logger.log(`Rejecting question: requestID=${requestID}`);
      try {
        await this.opencodeClient.rejectQuestion(requestID);
        this.logger.log(`Question rejected: ${requestID}`);
      } catch (error: any) {
        this.logger.error(`Failed to reject question: ${error.message}`);
      }
      return;
    }

    this.logger.log(`Answering question: requestID=${requestID}`);
    try {
      await this.opencodeClient.answerQuestion(requestID, answers || []);
      this.logger.log(`Question answer submitted: ${requestID}`);
    } catch (error: any) {
      this.logger.error(`Failed to submit question answer: ${error.message}`);
    }
  }

  private async handlePermissionReply(payload: any, packet?: any) {
    const { requestID, reply, message } = payload;

    if (!requestID || !reply) {
      this.logger.warn('Permission reply missing requestID or reply');
      return;
    }

    if (!['once', 'always', 'reject'].includes(reply)) {
      this.logger.warn(`Invalid permission reply: ${reply}`);
      return;
    }

    this.logger.log(`Received permission reply: requestID=${requestID}, reply=${reply}`);

    try {
      await this.opencodeClient.replyPermission(requestID, reply, message);
      this.logger.log(`Permission reply submitted: ${requestID} => ${reply}`);
    } catch (error: any) {
      this.logger.error(`Failed to submit permission reply: ${error.message}`);
    }
  }

  private async handleGroupMessage(topic: string, payload: any, packet?: any) {
    const groupTopic = topic;
    this.logger.log(`[DIAG] handleGroupMessage entered, topic="${topic}", payload=${JSON.stringify(payload)}`);

    const mySenderId = this.configService.get('mqtt.clientId') || 'opencode-agent';

    const sender = payload.senderId || payload.sender || 'unknown';
    if (sender === mySenderId) {
      this.logger.debug(`Ignoring self-sent group message from ${sender}`);
      return;
    }

    let shouldReply = true;
    const targetIds = payload.targetIds;
    if (!targetIds || !Array.isArray(targetIds) || targetIds.length === 0) {
      this.logger.log(`[DIAG] Broadcast group message (no targetIds), shouldReply=false`);
      shouldReply = false;
    } else if (!targetIds.some((id: string) => id.includes(mySenderId))) {
      this.logger.log(`[DIAG] Group message targetIds not meant for this client (targetIds=${JSON.stringify(targetIds)}, mySenderId=${mySenderId}), shouldReply=false`);
      shouldReply = false;
    } else {
      this.logger.log(`[DIAG] Group message targets this client, shouldReply=true`);
    }

    let replyToTopic = groupTopic;
    let userProperties: any = {};
    if (packet && packet.properties && packet.properties.userProperties) {
      userProperties = packet.properties.userProperties;
    }

    let message = '';
    const messageType = payload.type || 'text';

    if (messageType === 'file') {
      const fileName = payload.fileName || 'unknown';
      const fileType = payload.fileType || 'application/octet-stream';
      const fileDescription = payload.text || '';
      let fileContentInfo = '';

      if (payload.fileData) {
        const decodedText = tryDecodeBase64Text(payload.fileData);
        if (decodedText && isTextFile(fileName)) {
          const truncated = decodedText.length > 2000
            ? decodedText.substring(0, 2000) + '\n... (truncated)'
            : decodedText;
          fileContentInfo = `\n\nFile content:\n\`\`\`\n${truncated}\n\`\`\``;
        }
      }

      message = `[File shared: ${fileName} (Type: ${fileType})]` +
        (fileDescription ? ` ${fileDescription}` : '') +
        fileContentInfo;

      this.logger.log(`Group file message from ${sender} on ${groupTopic}: shouldReply=${shouldReply}, ${fileName} (${fileType})`);
    } else {
      message = payload.text || payload.message || JSON.stringify(payload);
      if (typeof message === 'object') {
        message = JSON.stringify(message);
      }
      this.logger.log(`Group message from ${sender} on ${groupTopic}: shouldReply=${shouldReply}, ${message.substring(0, 50)}${message.length > 50 ? '...' : ''}`);
    }

    if (!shouldReply) {
      this.logger.log(`[DIAG] shouldReply=false, skipping OpenCode entirely`);
      return;
    }

    const senderName = userProperties['name'] || sender;

    const { sessionID, messageID } = this.sessionManager.getOrCreateGroupSession(
      groupTopic,
      sender,
      senderName,
      replyToTopic,
    );

    const result = await this.opencodeService.processMqttMessageWithSession(
      groupTopic,
      payload,
      sessionID,
      messageID,
    );

    this.logger.log(`[DIAG] processMqttMessageWithSession result: success=${result.success}, sessionId=${result.sessionId}`);

    if (result.success) {
      const actualSessionID = result.sessionId;

      this.logger.log(`[DIAG] About to registerPendingSession for session ${actualSessionID}`);
      await this.sseService.registerPendingSession(
        actualSessionID,
        messageID,
        replyToTopic,
        userProperties,
        payload,
      );
      this.logger.log(`Session ${actualSessionID} registered for SSE tracking. Group response will be sent to: ${replyToTopic}`);
    } else {
      this.logger.error(`Failed to send group message to OpenCode: ${result.error}`);

      const errorMessage = {
        id: payload.id,
        text: `Error: Failed to process group message - ${result.error}`,
        senderId: mySenderId,
        kind: 'error',
        ts: Date.now(),
        originalMessageId: payload.id,
        processedAt: new Date().toISOString(),
        status: 'error',
      };

      this.mqttClient.publish(replyToTopic, JSON.stringify(errorMessage), {
        qos: 1,
        properties: {
          userProperties: {
            name: this.configService.get('mqtt.properties.userProperties.name') || 'opencode-agent',
            description: this.configService.get('mqtt.properties.userProperties.description') || 'Advanced Developer',
            emoji: this.configService.get('mqtt.properties.userProperties.emoji') || '🤖',
          },
        },
      });
    }
  }

  async onApplicationShutdown() {
    if (this.mqttClient && this.connected) {
      this.mqttClient.end(true);
      this.logger.log('MQTT client disconnected');
    }
  }

  public subscribeToTopic(topic: string) {
    if (this.mqttClient && this.connected) {
      this.mqttClient.subscribe(topic, (err) => {
        if (err) {
          this.logger.error(`Failed to subscribe to ${topic}: ${err.message}`);
        } else {
          this.logger.log(`Successfully subscribed to ${topic}`);
        }
      });
    } else {
      this.logger.warn(`Cannot subscribe to ${topic} - MQTT client not connected`);
    }
  }

  public unsubscribeFromTopic(topic: string) {
    if (this.mqttClient && this.connected) {
      this.mqttClient.unsubscribe(topic, (err) => {
        if (err) {
          this.logger.error(`Failed to unsubscribe from ${topic}: ${err.message}`);
        } else {
          this.logger.log(`Successfully unsubscribed from ${topic}`);
        }
      });
    }
  }

  public publishMessage(topic: string, message: any) {
    if (this.mqttClient && this.connected) {
      const payload = typeof message === 'object' ? JSON.stringify(message) : message;
      this.mqttClient.publish(topic, payload, { qos: 1 }, (err) => {
        if (err) {
          this.logger.error(`Failed to publish to ${topic}: ${err.message}`);
        } else {
          this.logger.debug(`Published message to ${topic}`);
        }
      });
    } else {
      this.logger.warn(`Cannot publish to ${topic} - MQTT client not connected`);
    }
  }

  private publishMessageWithOptions(topic: string, message: string, options: any) {
    if (this.mqttClient && this.connected) {
      this.mqttClient.publish(topic, message, options, (err) => {
        if (err) {
          this.logger.error(`Failed to publish to ${topic}: ${err.message}`);
        } else {
          this.logger.debug(`Published message to ${topic} with options`);
        }
      });
    } else {
      this.logger.warn(`Cannot publish to ${topic} - MQTT client not connected`);
    }
  }

  public publishFileMessage(
    topic: string,
    fileName: string,
    fileType: string,
    fileData: string,
    targetIds?: string[],
    text?: string,
  ) {
    const mqttMessage: MqttMessage = {
      id: this.generateMessageId(),
      senderId: this.configService.get('mqtt.clientId') || 'opencode-agent',
      timestamp: Date.now(),
      type: 'file',
      fileName,
      fileType,
      fileData,
      ...(text ? { text } : {}),
      ...(targetIds ? { targetIds } : {}),
    };

    const payload = JSON.stringify(mqttMessage);
    const userProperties = {
      name: this.configService.get('mqtt.properties.userProperties.name') || 'opencode-agent',
      description: this.configService.get('mqtt.properties.userProperties.description') || 'Advanced Developer',
      emoji: this.configService.get('mqtt.properties.userProperties.emoji') || '🤖',
    };

    this.logger.log(`Publishing file message to ${topic}: ${fileName} (${fileType})`);

    this.publishMessageWithOptions(topic, payload, {
      qos: 1,
      properties: {
        userProperties,
      },
    });
  }

  private generateMessageId(): string {
    return `mqtt-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  }
}
