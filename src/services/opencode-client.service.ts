import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { createOpencodeClient, OpencodeClient } from '@opencode-ai/sdk';

@Injectable()
export class OpencodeClientService {
  private readonly logger = new Logger(OpencodeClientService.name);

  readonly client: OpencodeClient;
  readonly baseUrl: string;
  readonly apiKey: string;
  readonly workspaceDir: string;
  readonly model: { providerID: string; modelID: string } | undefined;

  constructor(private configService: ConfigService) {
    this.baseUrl = this.configService.get('mqtt.opencodeBaseUrl') || 'http://localhost:4096';
    this.apiKey = this.configService.get('mqtt.opencodeApiKey') || '';
    this.workspaceDir = this.configService.get('mqtt.opencodeWorkspaceDir') || process.cwd();

    const modelConfig = this.configService.get<string>('mqtt.opencodeModel');
    if (modelConfig) {
      const parts = modelConfig.split('/');
      if (parts.length === 2 && parts[0] && parts[1]) {
        this.model = { providerID: parts[0], modelID: parts[1] };
        this.logger.log(`OpenCode model configured: ${modelConfig}`);
      } else {
        this.logger.warn(`Invalid OPENCODE_MODEL format: ${modelConfig}. Expected: provider_id/model_id`);
      }
    } else {
      this.logger.log('No OPENCODE_MODEL configured, using OpenCode default model');
    }

    const headers: Record<string, string> = {};
    if (this.apiKey) {
      headers['Authorization'] = `Bearer ${this.apiKey}`;
    }

    this.client = createOpencodeClient({
      baseUrl: this.baseUrl,
      headers,
      directory: this.workspaceDir,
    });

    if (this.apiKey) {
      this.logger.log(`OpencodeClient initialized: ${this.baseUrl} (authenticated), workspace: ${this.workspaceDir}`);
    } else {
      this.logger.log(`OpencodeClient initialized: ${this.baseUrl} (no authentication), workspace: ${this.workspaceDir}`);
    }
  }

  async createSession(sessionID: string): Promise<string> {
    try {
      const response: any = await this.client.session.create({
        body: { id: sessionID } as any,
        query: { directory: this.workspaceDir },
      });
      const actualSessionID = response?.data?.id || sessionID;
      this.logger.debug(`Session created: ${actualSessionID}`);
      return actualSessionID;
    } catch (error: any) {
      if (error?.response?.status === 409 || error?.status === 409) {
        this.logger.debug(`Session ${sessionID} already exists, reusing`);
        return sessionID;
      }
      throw error;
    }
  }

  async promptAsync(
    sessionID: string,
    messageID: string,
    parts: any[],
    model?: { providerID: string; modelID: string },
  ): Promise<any> {
    const body: any = {
      messageID,
      parts,
      noReply: false,
    };
    if (model) {
      body.model = model;
    }
    return this.client.session.promptAsync({
      path: { id: sessionID } as any,
      body,
    });
  }

  async getMessages(sessionID: string): Promise<any> {
    return this.client.session.messages({
      path: { id: sessionID } as any,
      query: { directory: this.workspaceDir },
    });
  }

  async healthCheck(): Promise<any> {
    const headers: Record<string, string> = {};
    if (this.apiKey) {
      headers['Authorization'] = `Bearer ${this.apiKey}`;
    }
    const response = await fetch(`${this.baseUrl}/health`, { headers });
    return response;
  }
}
