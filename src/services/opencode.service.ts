import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { OpencodeClientService } from './opencode-client.service.js';

@Injectable()
export class OpenCodeService {
  private readonly logger = new Logger(OpenCodeService.name);
  private readonly model: { providerID: string; modelID: string } | undefined;

  constructor(
    private configService: ConfigService,
    private opencodeClient: OpencodeClientService,
  ) {
    this.model = this.opencodeClient.model;
  }

  private getModelConfig(): { providerID: string; modelID: string } | undefined {
    return this.model;
  }

  async processMessage(message: string, context?: any): Promise<any> {
    this.logger.log(`Starting OpenCode message processing with promptAsync: ${message.substring(0, 50)}...`);
    const startTime = Date.now();

    try {
      const sessionID = process.env.DEFAULT_SESSION_ID || `ses_mqtt_${Date.now()}`;
      const actualSessionID = await this.opencodeClient.createSession(sessionID);

      const parts = [{ type: 'text', text: message }];
      const messageID = `msg_${Date.now()}`;

      await this.opencodeClient.promptAsync(actualSessionID, messageID, parts, this.getModelConfig());

      this.logger.log(`OpenCode promptAsync responded successfully in ${Date.now() - startTime}ms`);

      return {
        success: true,
        processedMessage: message,
        response: {},
        processedBy: 'OpenCode AI Service (promptAsync)',
        timestamp: new Date().toISOString(),
        processingTimeMs: Date.now() - startTime,
        sessionId: actualSessionID,
        workspaceDir: this.opencodeClient.workspaceDir,
      };
    } catch (error: any) {
      const errorMessage = error?.message || 'Unknown error';
      this.logger.warn(`OpenCode promptAsync request failed after ${Date.now() - startTime}ms: ${errorMessage}`);

      return {
        success: false,
        processedMessage: message,
        fallbackResponse: `OpenCode fallback: ${message.substring(0, 100)}...`,
        processedBy: 'Fallback Processor',
        timestamp: new Date().toISOString(),
        processingTimeMs: Date.now() - startTime,
        error: errorMessage,
      };
    }
  }

  async processMqttMessageWithSession(topic: string, payload: any, sessionID: string, messageID: string): Promise<any> {
    this.logger.log(`Starting MQTT message processing with promptAsync for topic: ${topic}`);
    const startTime = Date.now();

    const payloadStr = JSON.stringify(payload);
    const payloadSummary = payloadStr.length > 200 ? `${payloadStr.substring(0, 200)}...` : payloadStr;
    this.logger.debug(`Processing MQTT payload for topic ${topic}: ${payloadSummary}`);

    try {
      const actualSessionID = await this.opencodeClient.createSession(sessionID);

      const messageText = payload.text || payload.message || JSON.stringify(payload);
      const parts = [{ type: 'text', text: messageText }];

      await this.opencodeClient.promptAsync(actualSessionID, messageID, parts, this.getModelConfig());

      this.logger.log(`OpenCode MQTT message processing completed successfully in ${Date.now() - startTime}ms for topic: ${topic}`);

      return {
        success: true,
        sessionId: actualSessionID,
        requestedSessionId: sessionID,
        messageID,
        workspaceDir: this.opencodeClient.workspaceDir,
        processedAt: new Date().toISOString(),
        processingTimeMs: Date.now() - startTime,
      };
    } catch (error: any) {
      const errorMessage = error?.message || 'Unknown error';
      this.logger.warn(`OpenCode MQTT processing failed after ${Date.now() - startTime}ms for topic ${topic}: ${errorMessage}`);

      return {
        success: false,
        sessionId: sessionID,
        messageID,
        error: errorMessage,
        processingTimeMs: Date.now() - startTime,
      };
    }
  }

  async processMqttMessage(topic: string, payload: any): Promise<any> {
    this.logger.log(`Starting MQTT message processing with promptAsync for topic: ${topic}`);
    const startTime = Date.now();

    const payloadStr = JSON.stringify(payload);
    const payloadSummary = payloadStr.length > 200 ? `${payloadStr.substring(0, 200)}...` : payloadStr;
    this.logger.debug(`Processing MQTT payload for topic ${topic}: ${payloadSummary}`);

    try {
      const sessionID = `ses_mqtt_${topic.replace(/\//g, '_')}_${Date.now()}`;
      const actualSessionID = await this.opencodeClient.createSession(sessionID);

      const messageText = payload.text || payload.message || JSON.stringify(payload);
      const messageID = `msg_${Date.now()}`;
      const parts = [{ type: 'text', text: messageText }];

      await this.opencodeClient.promptAsync(actualSessionID, messageID, parts, this.getModelConfig());

      this.logger.log(`OpenCode MQTT message processing completed successfully in ${Date.now() - startTime}ms for topic: ${topic}`);

      return {
        originalPayload: payload,
        topic,
        processed: true,
        analysis: {
          status: 'accepted_for_processing',
          message: `Message sent to OpenCode for processing. Session: ${actualSessionID}, MessageID: ${messageID}`,
          nextSteps: 'Results will be available asynchronously via callbacks or polling',
        },
        processedAt: new Date().toISOString(),
        processedBy: 'OpenCode AI Service (promptAsync)',
        processingTimeMs: Date.now() - startTime,
        sessionId: actualSessionID,
        messageID,
        workspaceDir: this.opencodeClient.workspaceDir,
      };
    } catch (error: any) {
      const errorMessage = error?.message || 'Unknown error';
      this.logger.warn(`OpenCode MQTT message processing failed after ${Date.now() - startTime}ms for topic ${topic}: ${errorMessage}`);

      return {
        originalPayload: payload,
        topic,
        processed: false,
        analysis: {
          analysis: `OpenCode analysis of MQTT message on topic '${topic}'`,
          actionRequired: false,
          priority: 'low',
          recommendations: ['Enable OpenCode integration for intelligent analysis'],
        },
        processedAt: new Date().toISOString(),
        processedBy: 'Fallback Processor',
        processingTimeMs: Date.now() - startTime,
        error: errorMessage,
      };
    }
  }

  async sendNotification(message: string, options?: any): Promise<any> {
    this.logger.log(`Starting notification generation via OpenCode with promptAsync: ${message.substring(0, 50)}...`);
    const startTime = Date.now();

    try {
      const sessionID = `ses_notification_${Date.now()}`;
      const actualSessionID = await this.opencodeClient.createSession(sessionID);

      const messageID = `msg_${Date.now()}`;
      const parts = [
        { type: 'text', text: `Notification request: ${message}` },
        { type: 'text', text: 'Generate an appropriate notification with proper urgency and routing.' },
      ];

      const response = await this.opencodeClient.promptAsync(actualSessionID, messageID, parts, this.getModelConfig());

      this.logger.log(`OpenCode notification generation completed successfully in ${Date.now() - startTime}ms`);

      const responseData = (response as any)?.data || {};

      return {
        success: true,
        message,
        processedBy: 'OpenCode AI Service (promptAsync)',
        sentAt: new Date().toISOString(),
        processingTimeMs: Date.now() - startTime,
        sessionId: actualSessionID,
        workspaceDir: this.opencodeClient.workspaceDir,
        ...responseData,
      };
    } catch (error: any) {
      const errorMessage = error?.message || 'Unknown error';
      this.logger.warn(`OpenCode notification failed after ${Date.now() - startTime}ms: ${errorMessage}`);

      return {
        success: false,
        message,
        processedBy: 'Fallback Processor',
        sentAt: new Date().toISOString(),
        processingTimeMs: Date.now() - startTime,
        error: errorMessage,
        fallback: true,
      };
    }
  }

  async healthCheck(): Promise<{ status: 'healthy' | 'unhealthy'; message: string }> {
    this.logger.log(`Performing health check on OpenCode service at ${this.opencodeClient.baseUrl}`);
    const startTime = Date.now();

    try {
      const response: any = await this.opencodeClient.healthCheck();
      const duration = Date.now() - startTime;

      if (response?.response?.status === 200 || response?.data) {
        this.logger.log(`OpenCode service health check passed in ${duration}ms`);
        return {
          status: 'healthy',
          message: 'OpenCode service is accessible and responding',
        };
      }

      const msg = `OpenCode service responded with unexpected status`;
      this.logger.warn(`OpenCode service health check failed: ${msg} (${duration}ms)`);
      return { status: 'unhealthy', message: msg };
    } catch (error: any) {
      const errorMessage = error?.message || 'Unknown error';
      const duration = Date.now() - startTime;
      this.logger.warn(`OpenCode service health check failed after ${duration}ms: ${errorMessage}`);
      return {
        status: 'unhealthy',
        message: `OpenCode service not accessible: ${errorMessage}`,
      };
    }
  }
}
