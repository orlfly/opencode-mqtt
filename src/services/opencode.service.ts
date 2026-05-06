import { Injectable, Logger } from '@nestjs/common';
import axios from 'axios';
import { ConfigService } from '@nestjs/config';

@Injectable()
export class OpenCodeService {
  private readonly logger = new Logger(OpenCodeService.name);
  private readonly opencodeBaseUrl: string;
  private readonly opencodeApiKey: string;
  private readonly workspaceDir: string;
  private readonly model: { providerID: string; modelID: string } | undefined;

  constructor(private configService: ConfigService) {
    this.opencodeBaseUrl = this.configService.get('mqtt.opencodeBaseUrl') || 'http://localhost:4096';
    this.opencodeApiKey = this.configService.get('mqtt.opencodeApiKey') || '';
    this.workspaceDir = this.configService.get('mqtt.opencodeWorkspaceDir') || process.cwd();
    
    // Parse model configuration (format: provider_id/model_id)
    const modelConfig = this.configService.get<string>('mqtt.opencodeModel');
    if (modelConfig) {
      const parts = modelConfig.split('/');
      if (parts.length === 2 && parts[0] && parts[1]) {
        this.model = {
          providerID: parts[0],
          modelID: parts[1]
        };
        this.logger.log(`OpenCode model configured: ${modelConfig}`);
      } else {
        this.logger.warn(`Invalid OPENCODE_MODEL format: ${modelConfig}. Expected format: provider_id/model_id (e.g., anthropic/claude-sonnet-4-5)`);
      }
    } else {
      this.logger.log('No OPENCODE_MODEL configured, using OpenCode default model');
    }
    
    if (this.opencodeApiKey) {
      this.logger.log(`OpenCode Service initialized with base URL: ${this.opencodeBaseUrl} (authenticated), workspace: ${this.workspaceDir}`);
    } else {
      this.logger.log(`OpenCode Service initialized with base URL: ${this.opencodeBaseUrl} (no authentication), workspace: ${this.workspaceDir}`);
    }
  }

  /**
   * Get model configuration for prompt requests
   */
  private getModelConfig(): { providerID: string; modelID: string } | undefined {
    return this.model;
  }

  /**
   * Process a user message using OpenCode AI with promptAsync
   */
  async processMessage(message: string, context?: any): Promise<any> {
    this.logger.log(`Starting OpenCode message processing with promptAsync: ${message.substring(0, 50)}...`);
    const startTime = Date.now();
    
    try {
      // Use promptAsync to process the message asynchronously
      // The default session will be used if none is provided
      // Session ID must start with "ses" as required by OpenCode API
      const sessionID = process.env.DEFAULT_SESSION_ID || `ses_mqtt_${Date.now()}`;
      
      // Create session with workspace directory and get actual session ID
      const actualSessionID = await this.createSessionWithWorkspace(sessionID);
      
      const requestBody: any = {
        messageID: `msg_${Date.now()}`,
        parts: [
          {
            type: 'text',
            text: message,
          }
        ],
        noReply: false, // We want to get a reply
      };

      // Add model configuration if specified
      const modelConfig = this.getModelConfig();
      if (modelConfig) {
        requestBody.model = modelConfig;
      }

      this.logger.debug(`Sending request to OpenCode promptAsync API: ${this.opencodeBaseUrl}/session/${actualSessionID}/prompt_async`);
      
      const headers: any = {
        'Content-Type': 'application/json',
        'User-Agent': 'opencode-mqtt-service/1.0'
      };
      
      // Only add Authorization header if API key is provided
      if (this.opencodeApiKey) {
        headers['Authorization'] = `Bearer ${this.opencodeApiKey}`;
      }

      const response = await axios.post(`${this.opencodeBaseUrl}/session/${actualSessionID}/prompt_async`, requestBody, {
        headers,
        timeout: 30000 // 30 second timeout
      });

      this.logger.log(`OpenCode promptAsync responded successfully in ${Date.now() - startTime}ms for message: ${message.substring(0, 30)}...`);

      return {
        success: true,
        processedMessage: message,
        response: response.data,
        processedBy: 'OpenCode AI Service (promptAsync)',
        timestamp: new Date().toISOString(),
        processingTimeMs: Date.now() - startTime,
        sessionId: actualSessionID,
        workspaceDir: this.workspaceDir
      };
    } catch (error: any) {
      const errorMessage = error?.message || 'Unknown error';
      this.logger.warn(`OpenCode promptAsync request failed after ${Date.now() - startTime}ms: ${errorMessage}. Using fallback processing.`);
      
      // Fallback processing if OpenCode API is not available
      return {
        success: false,
        processedMessage: message,
        fallbackResponse: `OpenCode fallback: ${message.substring(0, 100)}...`,
        processedBy: 'Fallback Processor',
        timestamp: new Date().toISOString(),
        processingTimeMs: Date.now() - startTime,
        error: errorMessage
      };
    }
  }

  /**
   * Process MQTT message through OpenCode for enhanced intelligence with promptAsync
   * Using a specific session ID and message ID for tracking
   */
  async processMqttMessageWithSession(topic: string, payload: any, sessionID: string, messageID: string): Promise<any> {
    this.logger.log(`Starting MQTT message processing with promptAsync for topic: ${topic}`);
    const startTime = Date.now();
    
    // Log the payload for debugging (truncate if too large)
    const payloadStr = JSON.stringify(payload);
    const payloadSummary = payloadStr.length > 200 ? `${payloadStr.substring(0, 200)}...` : payloadStr;
    this.logger.debug(`Processing MQTT payload for topic ${topic}: ${payloadSummary}`);

    try {
      // Step 1: Create session with specified workspace directory
      const actualSessionID = await this.createSessionWithWorkspace(sessionID);

      // Step 2: Extract the text content from the payload as the primary message to OpenCode
      const messageText = payload.text || payload.message || JSON.stringify(payload);
      
      const requestBody: any = {
        messageID: messageID,
        parts: [
          {
            type: 'text',
            text: messageText
          }
        ],
        noReply: false, // We want to get a reply
      };

      // Add model configuration if specified
      const modelConfig = this.getModelConfig();
      if (modelConfig) {
        requestBody.model = modelConfig;
        this.logger.debug(`Using model: ${modelConfig.providerID}/${modelConfig.modelID}`);
      }

      this.logger.debug(`Sending MQTT message to OpenCode promptAsync API: ${this.opencodeBaseUrl}/session/${actualSessionID}/prompt_async`);
      
      const headers: any = {
        'Content-Type': 'application/json'
      };
      
      if (this.opencodeApiKey) {
        headers['Authorization'] = `Bearer ${this.opencodeApiKey}`;
      }

      const response = await axios.post(`${this.opencodeBaseUrl}/session/${actualSessionID}/prompt_async`, requestBody, {
        headers,
        timeout: 30000
      });

      this.logger.log(`OpenCode MQTT message processing with promptAsync completed successfully in ${Date.now() - startTime}ms for topic: ${topic}`);

      return {
        success: true,
        sessionId: actualSessionID,
        requestedSessionId: sessionID,
        messageID: messageID,
        workspaceDir: this.workspaceDir,
        processedAt: new Date().toISOString(),
        processingTimeMs: Date.now() - startTime
      };
    } catch (error: any) {
      const errorMessage = error?.message || 'Unknown error';
      this.logger.warn(`OpenCode MQTT message processing with promptAsync failed after ${Date.now() - startTime}ms for topic ${topic}: ${errorMessage}`);
      
      return {
        success: false,
        sessionId: sessionID,
        messageID: messageID,
        error: errorMessage,
        processingTimeMs: Date.now() - startTime
      };
    }
  }

  /**
   * Create a session with the specified workspace directory
   * Returns the actual session ID (may be auto-generated by OpenCode)
   */
  private async createSessionWithWorkspace(sessionID: string): Promise<string> {
    this.logger.debug(`Creating session ${sessionID} with workspace directory: ${this.workspaceDir}`);
    
    const headers: any = {
      'Content-Type': 'application/json'
    };
    
    if (this.opencodeApiKey) {
      headers['Authorization'] = `Bearer ${this.opencodeApiKey}`;
    }

    try {
      // Create session with custom ID and directory
      // OpenCode API: POST /session with body { id: sessionID }
      const response = await axios.post(
        `${this.opencodeBaseUrl}/session`,
        {
          id: sessionID
        },
        {
          headers,
          params: {
            directory: this.workspaceDir
          },
          timeout: 10000
        }
      );

      // Get the actual session ID from response (OpenCode may auto-generate)
      const actualSessionID = response.data?.id || sessionID;
      this.logger.debug(`Session created successfully. Requested ID: ${sessionID}, Actual ID: ${actualSessionID}, Directory: ${this.workspaceDir}`);
      
      return actualSessionID;
    } catch (error: any) {
      // If session already exists (409 conflict), return the requested ID
      if (error.response && error.response.status === 409) {
        this.logger.debug(`Session ${sessionID} already exists, using requested ID`);
        return sessionID;
      }
      // Re-throw other errors
      throw error;
    }
  }

  /**
   * Process MQTT message through OpenCode for enhanced intelligence with promptAsync
   */
  async processMqttMessage(topic: string, payload: any): Promise<any> {
    this.logger.log(`Starting MQTT message processing with promptAsync for topic: ${topic}`);
    const startTime = Date.now();
    
    // Log the payload for debugging (truncate if too large)
    const payloadStr = JSON.stringify(payload);
    const payloadSummary = payloadStr.length > 200 ? `${payloadStr.substring(0, 200)}...` : payloadStr;
    this.logger.debug(`Processing MQTT payload for topic ${topic}: ${payloadSummary}`);

    try {
      // Create a structured message for OpenCode analysis using promptAsync
      const sessionID = `ses_mqtt_${topic.replace(/\//g, '_')}_${Date.now()}`;
      
      // Create session with workspace directory and get actual session ID
      const actualSessionID = await this.createSessionWithWorkspace(sessionID);
      
      // Simplified message based on the received payload
      // Use the text content from the payload as the primary message to OpenCode
      const messageText = payload.text || payload.message || JSON.stringify(payload);
      
      const requestBody: any = {
        messageID: `msg_${Date.now()}`,
        parts: [
          {
            type: 'text',
            text: messageText
          }
        ],
        noReply: false, // We want to get a reply with the analysis
      };

      // Add model configuration if specified
      const modelConfig = this.getModelConfig();
      if (modelConfig) {
        requestBody.model = modelConfig;
      }

      this.logger.debug(`Sending MQTT message to OpenCode promptAsync API: ${this.opencodeBaseUrl}/session/${actualSessionID}/prompt_async`);
      
      const headers: any = {
        'Content-Type': 'application/json'
      };
      
      // Only add Authorization header if API key is provided
      if (this.opencodeApiKey) {
        headers['Authorization'] = `Bearer ${this.opencodeApiKey}`;
      }

      const response = await axios.post(`${this.opencodeBaseUrl}/session/${actualSessionID}/prompt_async`, requestBody, {
        headers,
        timeout: 30000
      });

      this.logger.log(`OpenCode MQTT message processing with promptAsync completed successfully in ${Date.now() - startTime}ms for topic: ${topic}`);

      // Since prompt_async returns immediately (status 204), return a meaningful result
      // The actual processing happens async and the result will come through other means
      const processedResult = {
        originalPayload: payload,
        topic,
        processed: true,
        analysis: {
          // Since prompt_async returns 204, we may not get detailed analysis right away
          // Just indicate that the message was accepted for processing
          status: 'accepted_for_processing',
          message: `Message sent to OpenCode for processing. Session: ${actualSessionID}, MessageID: ${requestBody.messageID}`,
          // We could enhance this to poll for results later or use callback mechanisms
          nextSteps: 'Results will be available asynchronously via callbacks or polling'
        },
        processedAt: new Date().toISOString(),
        processedBy: 'OpenCode AI Service (promptAsync)',
        processingTimeMs: Date.now() - startTime,
        sessionId: actualSessionID,
        messageID: requestBody.messageID,
        workspaceDir: this.workspaceDir
      };

      return processedResult;
    } catch (error: any) {
      const errorMessage = error?.message || 'Unknown error';
      this.logger.warn(`OpenCode MQTT message processing with promptAsync failed after ${Date.now() - startTime}ms for topic ${topic}: ${errorMessage}. Using fallback.`);
      
      // Fallback analysis
      return {
        originalPayload: payload,
        topic,
        processed: false,
        analysis: {
          analysis: `OpenCode analysis of MQTT message on topic '${topic}'`,
          actionRequired: false,
          priority: "low",
          recommendations: ["Enable OpenCode integration for intelligent analysis"]
        },
        processedAt: new Date().toISOString(),
        processedBy: 'Fallback Processor',
        processingTimeMs: Date.now() - startTime,
        error: errorMessage
      };
    }
  }

  /**
   * Send a notification via OpenCode using promptAsync
   */
  async sendNotification(message: string, options?: any): Promise<any> {
    this.logger.log(`Starting notification generation via OpenCode with promptAsync: ${message.substring(0, 50)}...`);
    const startTime = Date.now();
    
    try {
      // Session ID must start with "ses_" as required by OpenCode API
      const sessionID = `ses_notification_${Date.now()}`;
      
      // Create session with workspace directory and get actual session ID
      const actualSessionID = await this.createSessionWithWorkspace(sessionID);
      
      const requestBody: any = {
        messageID: `msg_${Date.now()}`,
        parts: [
          {
            type: 'text',
            text: `Notification request: ${message}`
          },
          {
            type: 'text',
            text: "Generate an appropriate notification with proper urgency and routing."
          }
        ],
        noReply: false,
      };

      // Add model configuration if specified
      const modelConfig = this.getModelConfig();
      if (modelConfig) {
        requestBody.model = modelConfig;
      }

      this.logger.debug(`Sending notification request to OpenCode promptAsync API: ${this.opencodeBaseUrl}/session/${actualSessionID}/prompt_async`);
      
      const headers: any = {
        'Content-Type': 'application/json'
      };
      
      // Only add Authorization header if API key is provided
      if (this.opencodeApiKey) {
        headers['Authorization'] = `Bearer ${this.opencodeApiKey}`;
      }

      const response = await axios.post(`${this.opencodeBaseUrl}/session/${actualSessionID}/prompt_async`, requestBody, {
        headers,
        timeout: 30000
      });

      this.logger.log(`OpenCode notification generation with promptAsync completed successfully in ${Date.now() - startTime}ms`);
      
      return {
        success: true,
        message,
        processedBy: 'OpenCode AI Service (promptAsync)',
        sentAt: new Date().toISOString(),
        processingTimeMs: Date.now() - startTime,
        sessionId: actualSessionID,
        workspaceDir: this.workspaceDir,
        ...response.data
      };
    } catch (error: any) {
      const errorMessage = error?.message || 'Unknown error';
      this.logger.warn(`OpenCode notification with promptAsync failed after ${Date.now() - startTime}ms: ${errorMessage}. Using fallback.`);
      
      return {
        success: false,
        message,
        processedBy: 'Fallback Processor',
        sentAt: new Date().toISOString(),
        processingTimeMs: Date.now() - startTime,
        error: errorMessage,
        fallback: true
      };
    }
  }

  /**
   * Health check for OpenCode service
   */
  async healthCheck(): Promise<{ status: 'healthy' | 'unhealthy'; message: string }> {
    this.logger.log(`Performing health check on OpenCode service at ${this.opencodeBaseUrl}`);
    const startTime = Date.now();
    
    try {
      const headers: any = {};
      
      // Only add Authorization header if API key is provided
      if (this.opencodeApiKey) {
        headers['Authorization'] = `Bearer ${this.opencodeApiKey}`;
      }

      const response = await axios.get(`${this.opencodeBaseUrl}/health`, {
        headers,
        timeout: 10000
      });

      const duration = Date.now() - startTime;
      if (response.status === 200) {
        this.logger.log(`OpenCode service health check passed in ${duration}ms`);
        return {
          status: 'healthy',
          message: 'OpenCode service is accessible and responding'
        };
      } else {
        const message = `OpenCode service responded with unexpected status: ${response.status}`;
        this.logger.warn(`OpenCode service health check failed: ${message} (${duration}ms)`);
        return {
          status: 'unhealthy',
          message
        };
      }
    } catch (error: any) {
      const errorMessage = error?.message || 'Unknown error';
      const duration = Date.now() - startTime;
      this.logger.warn(`OpenCode service health check failed after ${duration}ms: ${errorMessage}`);
      return {
        status: 'unhealthy',
        message: `OpenCode service not accessible: ${errorMessage}`
      };
    }
  }
}