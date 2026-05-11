import { NestFactory } from '@nestjs/core';
import { ConfigService } from '@nestjs/config';
import { AppModule } from './app.module';
import { validateHost, validatePort, validateClientId } from './utils/validation.util';
import { validateEnvConfig } from './config/env.validation';
import { MqttSubscriberService } from './services/mqtt-subscriber.service';

async function bootstrap() {
  const app = await NestFactory.createApplicationContext(AppModule);

  const configService = app.get(ConfigService);

  // Get MQTT configuration from the registered config
  const mqttHost = configService.get('mqtt.host');
  const mqttPort = configService.get('mqtt.port');
  const mqttUsername = configService.get('mqtt.username');
  const mqttPassword = configService.get('mqtt.password');
  const mqttClientId = configService.get('mqtt.clientId');
  const mqttClean = configService.get('mqtt.clean');
  const mqttConnectTimeout = configService.get('mqtt.connectTimeout');
  const mqttReconnectPeriod = configService.get('mqtt.reconnectPeriod');
  const mqttProtocolVersion = configService.get('mqtt.protocolVersion');
  const mqttUserProperties = configService.get('mqtt.properties.userProperties');
  const mqttPrivateChatTopic = configService.get('mqtt.privateChatTopic');

  // Validate configuration
  if (!validateHost(mqttHost)) {
    throw new Error(`Invalid MQTT host: ${mqttHost}`);
  }
  
  if (!validatePort(mqttPort)) {
    throw new Error(`Invalid MQTT port: ${mqttPort}`);
  }
  
  if (mqttClientId && !validateClientId(mqttClientId)) {
    throw new Error(`Invalid MQTT client ID: ${mqttClientId}`);
  }

  console.log(`Configured to connect to MQTT broker at ${mqttHost}:${mqttPort}...`);
  console.log(`Client ID: ${mqttClientId}`);
  console.log(`Using MQTT v${mqttProtocolVersion} protocol`);
  console.log(`User properties: name="${mqttUserProperties.name}", description="${mqttUserProperties.description}", emoji="${mqttUserProperties.emoji}"`);
  if (mqttPrivateChatTopic) {
    console.log(`Private chat topic: ${mqttPrivateChatTopic} (group invite/dismiss also use this topic)`);
  } else {
    console.log('Private chat topic: not configured');
  }

  const mqttSubscriberService = app.get(MqttSubscriberService);

  console.log('MQTT Subscriber Service initialized.');
  if (mqttPrivateChatTopic) {
    console.log(`  - Private chat: ${mqttPrivateChatTopic}`);
  } else {
    console.log('  - Private chat: not configured');
  }
  if (mqttUsername) {
    console.log(`Authenticated as: ${mqttUsername}`);
  } else {
    console.log(`Anonymous connection`);
  }

  // Keep the application running to maintain MQTT connection
  await app.init();
}

bootstrap();