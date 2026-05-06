import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { EventEmitterModule } from '@nestjs/event-emitter';
import mqttConfig from './config/mqtt.config';
import { validateEnvConfig } from './config/env.validation';
import { AppService } from './app.service';
import { OpenCodeService } from './services/opencode.service';
import { MqttSubscriberService } from './services/mqtt-subscriber.service';
import { OpenCodeSSEService } from './services/opencode-sse.service';

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
  providers: [AppService, OpenCodeService, MqttSubscriberService, OpenCodeSSEService],
})
export class AppModule {}