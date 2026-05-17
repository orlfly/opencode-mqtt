import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { EventEmitterModule } from '@nestjs/event-emitter';
import mqttConfig from './config/mqtt.config.js';
import { validateEnvConfig } from './config/env.validation.js';
import { AppService } from './app.service.js';
import { OpenCodeService } from './services/opencode.service.js';
import { OpencodeClientService } from './services/opencode-client.service.js';
import { MqttSubscriberService } from './services/mqtt-subscriber.service.js';
import { OpenCodeSSEService } from './services/opencode-sse.service.js';
import { SessionManagerService } from './services/session-manager.service.js';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: ['.env.local', '.env'],
      load: [mqttConfig],
      validate: validateEnvConfig,
    }),
    EventEmitterModule.forRoot(),
  ],
  providers: [AppService, OpencodeClientService, OpenCodeService, SessionManagerService, MqttSubscriberService, OpenCodeSSEService],
})
export class AppModule {}