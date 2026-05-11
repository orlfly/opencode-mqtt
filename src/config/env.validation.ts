import { plainToClass } from 'class-transformer';
import { IsEnum, IsNumberString, IsOptional, IsString, validateSync } from 'class-validator';

enum Environment {
  Development = 'development',
  Production = 'production',
  Test = 'test',
}

class EnvironmentVariables {
  @IsEnum(Environment)
  @IsOptional()
  NODE_ENV?: Environment = Environment.Development;

  @IsString()
  @IsOptional()
  MQTT_HOST?: string = 'localhost';

  @IsNumberString()
  @IsOptional()
  MQTT_PORT?: string = '1883';

  @IsString()
  @IsOptional()
  MQTT_USERNAME?: string;

  @IsString()
  @IsOptional()
  MQTT_PASSWORD?: string;

  @IsString()
  @IsOptional()
  MQTT_CLIENT_ID?: string;

  @IsString()
  @IsOptional()
  MQTT_PROTOCOL?: string = 'mqtt'; // mqtt, mqtts

  @IsNumberString()
  @IsOptional()
  MQTT_CONNECT_TIMEOUT?: string = '60000';

  @IsNumberString()
  @IsOptional()
  MQTT_RECONNECT_PERIOD?: string = '5000';
  
  @IsString()
  @IsOptional()
  MQTT_USER_PROPERTIES_NAME?: string = 'nestjs-mqtt-service';

  @IsString()
  @IsOptional()
  MQTT_USER_PROPERTIES_DESCRIPTION?: string = 'NestJS MQTT Microservice Client';

  @IsString()
  @IsOptional()
  MQTT_USER_PROPERTIES_EMOJI?: string = '🤖';

  @IsString()
  @IsOptional()
  MQTT_PRIVATE_CHAT_TOPIC?: string;

  @IsString()
  @IsOptional()
  MQTT_GROUP_INVITE_TOPIC?: string;

  @IsString()
  @IsOptional()
  MQTT_GROUP_DISBAND_TOPIC?: string;

  @IsString()
  @IsOptional()
  MQTT_GROUP_CHAT_TOPIC?: string;
}

export function validateEnvConfig(config: Record<string, unknown>) {
  const validatedConfig = plainToClass(
    EnvironmentVariables,
    config,
  );

  const errors = validateSync(validatedConfig, {
    skipMissingProperties: false,
  });

  if (errors.length > 0) {
    throw new Error(errors.map(error => Object.values(error.constraints || {}).join(', ')).join('; '));
  }
  return validatedConfig;
}