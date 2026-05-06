import { Injectable, Logger } from '@nestjs/common';
import axios from 'axios';

interface MessageInfo {
  id: string;
  sessionID: string;
  role: 'user' | 'assistant';
  time: {
    created: number;
    completed?: number;
  };
  parentID?: string;
  error?: any;
  finish?: string;
}

interface Part {
  type: string;
  text?: string;
}

interface SessionMessage {
  info: MessageInfo;
  parts: Array<Part>;
}

@Injectable()
export class OpenCodePollingService {
  private readonly logger = new Logger(OpenCodePollingService.name);
  private readonly opencodeBaseUrl: string;
  private readonly opencodeApiKey: string;

  constructor() {
    this.opencodeBaseUrl = process.env.OPENCODE_BASE_URL || 'http://localhost:4096';
    this.opencodeApiKey = process.env.OPENCODE_API_KEY || '';
    
    this.logger.log(`OpenCode Polling Service initialized with base URL: ${this.opencodeBaseUrl}`);
  }

  /**
   * 获取请求头
   */
  private getHeaders(): any {
    const headers: any = {
      'Content-Type': 'application/json',
      'User-Agent': 'opencode-mqtt-service/1.0'
    };
    
    if (this.opencodeApiKey) {
      headers['Authorization'] = `Bearer ${this.opencodeApiKey}`;
    }
    
    return headers;
  }

  /**
   * 发送异步消息到OpenCode
   */
  async sendAsyncMessage(sessionID: string, messageText: string): Promise<{ messageID: string }> {
    this.logger.log(`Sending async message to session: ${sessionID}`);
    
    const messageID = `msg-${Date.now()}`;
    
    const requestBody = {
      messageID,
      parts: [
        {
          type: 'text',
          text: messageText
        }
      ],
      noReply: false
    };

    try {
      const response = await axios.post(
        `${this.opencodeBaseUrl}/session/${sessionID}/prompt_async`,
        requestBody,
        {
          headers: this.getHeaders(),
          timeout: 10000
        }
      );

      // HTTP 204 表示请求已接受
      if (response.status === 204) {
        this.logger.debug(`Message ${messageID} accepted for async processing`);
        return { messageID };
      } else {
        throw new Error(`Unexpected response status: ${response.status}`);
      }
    } catch (error: any) {
      this.logger.error(`Failed to send async message: ${error.message}`);
      throw error;
    }
  }

  /**
   * 获取会话中的所有消息
   */
  async getSessionMessages(sessionID: string, limit?: number): Promise<SessionMessage[]> {
    this.logger.debug(`Fetching messages for session: ${sessionID}`);
    
    try {
      const params: any = {};
      if (limit) {
        params.limit = limit;
      }

      const response = await axios.get<SessionMessage[]>(
        `${this.opencodeBaseUrl}/session/${sessionID}/message`,
        {
          headers: this.getHeaders(),
          params,
          timeout: 10000
        }
      );

      this.logger.debug(`Retrieved ${response.data.length} messages for session ${sessionID}`);
      return response.data;
    } catch (error: any) {
      this.logger.error(`Failed to get session messages: ${error.message}`);
      throw error;
    }
  }

  /**
   * 查找特定的消息（根据messageID或parentID）
   */
  async findMessage(sessionID: string, messageID: string): Promise<SessionMessage | null> {
    const messages = await this.getSessionMessages(sessionID);
    
    // 查找用户消息
    const userMessage = messages.find(m => m.info.id === messageID);
    if (!userMessage) {
      return null;
    }

    // 查找对应的AI响应消息（parentID == userMessage.id）
    const assistantMessage = messages.find(m => 
      m.info.role === 'assistant' && m.info.parentID === userMessage.info.id
    );

    return assistantMessage || null;
  }

  /**
   * 轮询等待AI响应完成
   */
  async waitForCompletion(
    sessionID: string, 
    messageID: string, 
    maxWaitMs: number = 30000,
    pollIntervalMs: number = 1000
  ): Promise<SessionMessage> {
    this.logger.log(`Waiting for completion of message ${messageID} in session ${sessionID}`);
    
    const startTime = Date.now();
    
    while (Date.now() - startTime < maxWaitMs) {
      try {
        const messages = await this.getSessionMessages(sessionID);
        
        // 找到用户消息
        const userMessage = messages.find(m => m.info.id === messageID);
        
        if (userMessage) {
          // 查找对应的AI响应
          const assistantMessage = messages.find(m => 
            m.info.role === 'assistant' && m.info.parentID === userMessage.info.id
          );
          
          if (assistantMessage) {
            // 检查是否已完成
            if (assistantMessage.info.time.completed) {
              this.logger.log(`AI response completed for message ${messageID}`);
              
              // 检查是否有错误
              if (assistantMessage.info.error) {
                this.logger.warn(`AI response has error: ${JSON.stringify(assistantMessage.info.error)}`);
              }
              
              return assistantMessage;
            } else {
              this.logger.debug(`AI response in progress, waiting...`);
            }
          }
        }
        
        // 等待下一次轮询
        await new Promise(resolve => setTimeout(resolve, pollIntervalMs));
        
      } catch (error: any) {
        this.logger.warn(`Polling error: ${error.message}, continuing...`);
        await new Promise(resolve => setTimeout(resolve, pollIntervalMs));
      }
    }
    
    throw new Error(`Timeout waiting for completion after ${maxWaitMs}ms`);
  }

  /**
   * 提取文本响应内容
   */
  extractTextContent(message: SessionMessage): string {
    let fullText = '';
    
    for (const part of message.parts) {
      if (part.type === 'text' && part.text) {
        fullText += part.text;
      }
    }
    
    return fullText;
  }

  /**
   * 完整的处理流程：发送消息 + 等待结果
   */
  async processMessageAndWait(
    messageText: string,
    sessionID: string,
    maxWaitMs: number = 30000
  ): Promise<{
    success: boolean;
    userMessageID: string;
    assistantMessage?: SessionMessage;
    responseText?: string;
    error?: string;
    processingTimeMs: number;
  }> {
    const startTime = Date.now();
    
    this.logger.log(`Starting full processing flow for session ${sessionID}`);
    
    try {
      // 1. 发送异步消息
      const { messageID } = await this.sendAsyncMessage(sessionID, messageText);
      
      // 2. 等待AI处理完成
      const assistantMessage = await this.waitForCompletion(
        sessionID, 
        messageID, 
        maxWaitMs
      );
      
      // 3. 提取响应文本
      const responseText = this.extractTextContent(assistantMessage);
      
      const processingTimeMs = Date.now() - startTime;
      
      this.logger.log(`Message processing completed in ${processingTimeMs}ms`);
      
      return {
        success: true,
        userMessageID: messageID,
        assistantMessage,
        responseText,
        processingTimeMs
      };
      
    } catch (error: any) {
      const processingTimeMs = Date.now() - startTime;
      this.logger.error(`Processing failed: ${error.message}`);
      
      return {
        success: false,
        userMessageID: '',
        error: error.message,
        processingTimeMs
      };
    }
  }
}