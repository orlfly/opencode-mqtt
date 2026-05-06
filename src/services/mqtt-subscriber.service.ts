import { Injectable, Logger, OnApplicationBootstrap, OnApplicationShutdown } from '@nestjs/common';
import { MqttClient, connect } from 'mqtt';
import { ConfigService } from '@nestjs/config';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { OpenCodeService } from './opencode.service';
import { OpenCodeSSEService } from './opencode-sse.service';

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
    // Get private chat topic from config
    const privateChatTopic = this.configService.get('mqtt.privateChatTopic') || 'private/chat/+';
    
    // Subscribe only to the private chat topic after connection is established
    this.mqttClient.subscribe(privateChatTopic, (err) => {
      if (err) {
        this.logger.error(`Failed to subscribe to ${privateChatTopic}: ${err.message}`);
      } else {
        this.logger.log(`Successfully subscribed to private chat topic: ${privateChatTopic}`);
      }
    });

    // Set up message handlers
    this.setupMessageHandlers();
  }

  private setupMessageHandlers() {
    // Remove existing message listener to prevent duplicates on reconnection
    this.mqttClient.removeAllListeners('message');
    
    this.mqttClient.on('message', (topic, message, packet) => {
      try {
        const payload = JSON.parse(message.toString());
        
        // Only handle private chat messages as per requirement
        const privateChatTopicPattern = this.configService.get('mqtt.privateChatTopic') || 'private/chat/+';
        
        // Check if topic matches the configured private chat topic pattern
        // Support both exact match and wildcard patterns
        if (this.isTopicMatch(topic, privateChatTopicPattern)) {
          this.handlePrivateChat(payload, packet).catch(error => {
            const errorMessage = error instanceof Error ? error.message : 'Unknown error';
            this.logger.error(`Error handling private chat message: ${errorMessage}`);
          });
        } else {
          this.logger.warn(`Received message on unhandled topic: ${topic}`);
        }
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
    this.logger.log(`Received private chat message: ${JSON.stringify(payload)}`);
    
    // Parse userProperties from the MQTT packet to get reply_to topic
    let replyToTopic = null;
    let userProperties: any = {};
    if (packet && packet.properties && packet.properties.userProperties) {
      replyToTopic = packet.properties.userProperties.reply_to || packet.properties.userProperties['reply-to'];
      userProperties = packet.properties.userProperties;
    }
    
    // Extract message details
    let sender = payload.senderId || payload.sender || 'unknown';
    let recipient = payload.recipient || 'unknown';
    let message = payload.text || payload.message || JSON.stringify(payload);
    let timestamp = payload.timestamp;

    if (typeof message === 'object') {
      message = JSON.stringify(message);
    }

    this.logger.log(`Private message from ${sender} to ${recipient}: ${message.substring(0, 50)}${message.length > 50 ? '...' : ''}`);
    
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
      this.configService.get('mqtt.privateChatTopic') || 'private/chat/+',
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
      const errorMessage = {
        id: payload.id,
        text: `Error: Failed to process message - ${result.error}`,
        senderId: this.configService.get('mqtt.clientId') || 'opencode-agent',
        kind: 'error',
        ts: Date.now(),
        originalMessageId: payload.id,
        processedAt: new Date().toISOString(),
        status: 'error'
      };

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
}