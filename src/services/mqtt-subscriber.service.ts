import { Injectable, Logger, OnApplicationBootstrap, OnApplicationShutdown } from '@nestjs/common';
import { MqttClient, connect } from 'mqtt';
import { ConfigService } from '@nestjs/config';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { OpenCodeService } from './opencode.service';
import { OpenCodeSSEService } from './opencode-sse.service';
import { MqttMessage } from '../interfaces/mqtt-message.interface';
import { getMimeType, tryDecodeBase64Text, isTextFile } from '../utils/file.util';

@Injectable()
export class MqttSubscriberService implements OnApplicationBootstrap, OnApplicationShutdown {
  private readonly logger = new Logger(MqttSubscriberService.name);
  private mqttClient: MqttClient;
  private connected = false;



  constructor(
    private configService: ConfigService,
    private opencodeService: OpenCodeService,
    private sseService: OpenCodeSSEService,
    private eventEmitter: EventEmitter2,
  ) {
    // Listen for MQTT publish requests from SSE service
    this.eventEmitter.on('mqtt.publish', (data: any) => {
      this.publishMessageWithOptions(data.topic, data.message, data.options);
    });
  }

  async onApplicationBootstrap() {
    this.logger.log('Setting up MQTT subscriber service...');
    
    // Get MQTT configuration
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
    
    // Create MQTT client
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

    // Set max listeners to prevent memory leaks
    this.mqttClient.setMaxListeners(10);

    // Handle connection events
    this.mqttClient.on('connect', () => {
      this.connected = true;
      this.logger.log('Connected to MQTT broker');
      
      // Subscribe to all required topics after successful connection
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

    // Set up message handlers
    this.setupMessageHandlers();
  }

  private setupMessageHandlers() {
    this.mqttClient.removeAllListeners('message');

    const privateChatTopicPattern = this.configService.get('mqtt.privateChatTopic');

    this.mqttClient.on('message', (topic, message, packet) => {
      try {
        const payload = JSON.parse(message.toString());

        // 1. Private chat topic messages (including control messages for group management)
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

          this.handlePrivateChat(payload, packet).catch(error => {
            this.logger.error(`Error handling private chat message: ${error instanceof Error ? error.message : 'Unknown error'}`);
          });
          return;
        }

        // 2. Everything else is a group message
        this.handleGroupMessage(topic, payload, packet).catch(error => {
          this.logger.error(`Error handling group message: ${error instanceof Error ? error.message : 'Unknown error'}`);
        });
      } catch (error) {
        this.logger.error(`Error processing message on topic ${topic}: ${error instanceof Error ? error.message : 'Unknown error'}`);
      }
    });
  }

  // Helper method to check if a topic matches a pattern (supporting wildcards)
  private isTopicMatch(topic: string, pattern: string): boolean {
    // Simple placeholder - check if topic exactly matches the pattern
    // In a more sophisticated implementation, we would handle MQTT wildcards (+ and #)
    if (pattern === topic) {
      return true;
    }
    
    // Also handle wildcard patterns like 'private/chat/+' which would match 'private/chat/user123'
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
    
    // For '#' wildcard, handle differently
    if (pattern.includes('#')) {
      const patternParts = pattern.split('/');
      const topicParts = topic.split('/');
      
      // Find position of '#' wildcard
      const hashIndex = patternParts.indexOf('#');
      if (hashIndex === -1) return false;
      
      // Check parts before '#' match
      for (let i = 0; i < hashIndex; i++) {
        if (patternParts[i] !== topicParts[i]) {
          return false;
        }
      }
      
      return true; // Everything after '#' matches
    }
    
    return false;
  }

  // Handle private chat messages
  private async handlePrivateChat(payload: any, packet?: any) {
    this.logger.log(`[DIAG] handlePrivateChat entered, payload=${JSON.stringify(payload)}`);
    
    // Parse userProperties from the MQTT packet to get reply_to topic
    let replyToTopic = null;
    let userProperties: any = {};
    if (packet && packet.properties && packet.properties.userProperties) {
      replyToTopic = packet.properties.userProperties.reply_to || packet.properties.userProperties['reply-to'];
      userProperties = packet.properties.userProperties;
    }
    
    let sender = payload.senderId || payload.sender || 'unknown';
    let recipient = payload.recipient || 'unknown';
    let message = '';
    let timestamp = payload.timestamp;
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

    // Generate session ID and message ID
    // OpenCode requires session ID format: ses_xxxxxx (with underscore, not dash)
    const sessionID = `ses_mqtt_${Date.now()}`;
    const messageID = `msg_${Date.now()}`;

    this.logger.log(`Processing message via OpenCode promptAsync. Session: ${sessionID}, MessageID: ${messageID}`);

    // Send the message to OpenCode using promptAsync
    const result = await this.opencodeService.processMqttMessageWithSession(
      this.configService.get('mqtt.privateChatTopic'),
      payload,
      sessionID,
      messageID
    );

    if (result.success) {
      // Register the session with SSE service for result tracking
      // Use the actual session ID returned by OpenCode (may differ from requested ID)
      const actualSessionID = result.sessionId;
      await this.sseService.registerPendingSession(
        actualSessionID,
        messageID,
        replyToTopic,
        userProperties,
        payload
      );

      this.logger.log(`Session ${actualSessionID} registered for SSE tracking. Response will be sent to: ${replyToTopic}`);
    } else {
      this.logger.error(`Failed to send message to OpenCode: ${result.error}`);
      
      // Send error response directly
      const errorMessage: any = {
        id: payload.id,
        text: `Error: Failed to process message - ${result.error}`,
        senderId: this.configService.get('mqtt.clientId') || 'opencode-agent',
        kind: 'error',
        ts: Date.now(),
        originalMessageId: payload.id,
        processedAt: new Date().toISOString(),
        status: 'error'
      };

      if (payload.senderId) {
        errorMessage.targetIds = [payload.senderId];
      }

      this.mqttClient.publish(replyToTopic, JSON.stringify(errorMessage), {
        qos: 1,
        properties: {
          userProperties: {
            name: this.configService.get('mqtt.properties.userProperties.name') || 'opencode-agent',
            description: this.configService.get('mqtt.properties.userProperties.description') || 'Advanced Developer',
            emoji: this.configService.get('mqtt.properties.userProperties.emoji') || '🤖',
          }
        }
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

    this.mqttClient.unsubscribe(groupTopic, (err) => {
      if (err) {
        this.logger.error(`Failed to unsubscribe from group topic ${groupTopic}: ${err.message}`);
        return;
      }
      this.logger.log(`Unsubscribed from group topic: ${groupTopic}`);
    });
  }

  private async handleGroupMessage(topic: string, payload: any, packet?: any) {
    const groupTopic = topic;
    this.logger.log(`[DIAG] handleGroupMessage entered, topic="${topic}", payload=${JSON.stringify(payload)}`);

    const mySenderId = this.configService.get('mqtt.clientId') || 'opencode-agent';

    // Skip self-sent messages
    const sender = payload.senderId || payload.sender || 'unknown';
    if (sender === mySenderId) {
      this.logger.debug(`Ignoring self-sent group message from ${sender}`);
      return;
    }

    // targetIds: determine if we should reply or just record context
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

    // Reply target (used only when shouldReply is true)
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

    const sessionID = `ses_mqtt_grp_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const messageID = `msg_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;

    this.logger.log(`[DIAG] Calling processMqttMessageWithSession, replyToTopic=${replyToTopic}`);

    const result = await this.opencodeService.processMqttMessageWithSession(
      groupTopic,
      payload,
      sessionID,
      messageID
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
        payload
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
      this.mqttClient.end(true); // Force close
      this.logger.log('MQTT client disconnected');
    }
  }

  // Method to dynamically subscribe to additional topics at runtime
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

  // Method to dynamically unsubscribe from topics at runtime
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

  // Method to publish messages
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

  // Method to publish messages with full options (used by SSE service)
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