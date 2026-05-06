import { Injectable, Logger } from '@nestjs/common';
import axios from 'axios';

@Injectable()
export class OpenCodeIntegrationService {
  private readonly logger = new Logger(OpenCodeIntegrationService.name);
  private readonly opencodeBaseUrl: string;
  private readonly opencodeApiKey: string;

  constructor() {
    this.opencodeBaseUrl = process.env.OPENCODE_BASE_URL || 'http://localhost:3000';
    this.opencodeApiKey = process.env.OPENCODE_API_KEY || '';
    
    this.logger.log(`OpenCode Integration Service initialized with base URL: ${this.opencodeBaseUrl}`);
  }

  /**
   * Process a user message using OpenCode AI
   */
  async processMessage(message: string, context?: any): Promise<any> {
    this.logger.log(`Processing message with OpenCode: ${message.substring(0, 50)}...`);
    
    try {
      // Prepare request to OpenCode API
      const requestData = {
        query: message,
        context: context || {},
        options: {
          model: process.env.OPENCODE_MODEL || 'gpt-4',
        }
      };

      // Make API call to OpenCode instance
      const response = await axios.post(`${this.opencodeBaseUrl}/api/query`, requestData, {
        headers: {
          'Authorization': `Bearer ${this.opencodeApiKey}`,
          'Content-Type': 'application/json'
        }
      });

      return {
        success: true,
        processedMessage: message,
        response: response.data,
        processedBy: 'OpenCode AI Service',
        timestamp: new Date().toISOString()
      };
    } catch (error: any) {
      this.logger.warn(`OpenCode API request failed: ${error?.message || 'Unknown error'}. Using fallback processing.`);
      
      // Fallback processing if OpenCode API is not available
      return {
        success: false,
        processedMessage: message,
        fallbackResponse: `Processed by fallback: ${message.substring(0, 100)}...`,
        processedBy: 'Fallback Processor',
        timestamp: new Date().toISOString(),
        error: error?.message || 'Unknown error occurred'
      };
    }
  }

  /**
   * Process MQTT message through OpenCode for enhanced intelligence
   */
  async processMqttMessage(topic: string, payload: any): Promise<any> {
    this.logger.log(`Processing MQTT message from topic ${topic} with OpenCode`);
    
    try {
      // Create structured data for OpenCode analysis
      const analysisRequest = {
        type: 'mqtt_analysis',
        data: {
          topic,
          payload: typeof payload === 'object' ? JSON.stringify(payload) : payload,
          timestamp: new Date().toISOString()
        },
        instructions: `
          Analyze this MQTT message:
          - Determine the message category (alert, status, telemetry, command, etc.)
          - Assess importance/priority level
          - Identify any potential anomalies
          - Suggest appropriate actions if needed
          - Recommend follow-up steps
          
          Return your analysis in this JSON format:
          {
            "category": "status|telemetry|alert|command|other",
            "priority": "low|medium|high|critical",
            "anomaly_detected": true|false,
            "actions_required": ["list", "of", "potential", "actions"],
            "recommendations": ["suggested", "follow-up", "steps"]
          }
        `
      };

      const response = await axios.post(`${this.opencodeBaseUrl}/api/analyze`, analysisRequest, {
        headers: {
          'Authorization': `Bearer ${this.opencodeApiKey}`,
          'Content-Type': 'application/json'
        }
      });

      return {
        originalPayload: payload,
        topic,
        processed: true,
        analysis: response.data,
        processedAt: new Date().toISOString(),
        processedBy: 'OpenCode AI Service'
      };
    } catch (error: any) {
      this.logger.warn(`OpenCode MQTT analysis failed: ${error?.message || 'Unknown error'}. Using fallback.`);
      
      // Fallback analysis
      return {
        originalPayload: payload,
        topic,
        processed: false,
        analysis: {
          category: 'unknown',
          priority: 'low',
          anomaly_detected: false,
          actions_required: ['manual_review'],
          recommendations: ['enable_opencode_integration']
        },
        processedAt: new Date().toISOString(),
        processedBy: 'Fallback Processor',
        error: error?.message || 'Unknown error occurred'
      };
    }
  }

  /**
   * Send a notification via OpenCode
   */
  async sendNotification(message: string, options?: any): Promise<any> {
    this.logger.log(`Sending notification via OpenCode: ${message}`);
    
    try {
      const notificationRequest = {
        message,
        options: options || {},
        routing: {
          priority: 'normal',
          targets: [] // Will be determined by OpenCode
        }
      };

      const response = await axios.post(`${this.opencodeBaseUrl}/api/notify`, notificationRequest, {
        headers: {
          'Authorization': `Bearer ${this.opencodeApiKey}`,
          'Content-Type': 'application/json'
        }
      });

      return {
        success: true,
        messageId: response.data.id || null,
        message,
        processedBy: 'OpenCode AI Service',
        sentAt: new Date().toISOString(),
        ...response.data
      };
    } catch (error: any) {
      this.logger.warn(`OpenCode notification failed: ${error?.message || 'Unknown error'}. Using fallback.`);
      
      return {
        success: false,
        message,
        processedBy: 'Fallback Processor',
        sentAt: new Date().toISOString(),
        error: error?.message || 'Unknown error occurred',
        fallback: true
      };
    }
  }

  /**
   * Health check for OpenCode service
   */
  async healthCheck(): Promise<{ status: 'healthy' | 'unhealthy'; message: string }> {
    try {
      const response = await axios.get(`${this.opencodeBaseUrl}/health`, {
        headers: {
          'Authorization': `Bearer ${this.opencodeApiKey}`,
        }
      });

      if (response.status === 200) {
        return {
          status: 'healthy',
          message: 'OpenCode service is accessible and responding'
        };
      } else {
        return {
          status: 'unhealthy',
          message: `OpenCode service responded with status: ${response.status}`
        };
      }
    } catch (error: any) {
      return {
        status: 'unhealthy',
        message: `OpenCode service not accessible: ${error?.message || 'Unknown error'}`
      };
    }
  }
}