import { registerAs } from '@nestjs/config';

export default registerAs('mqtt', () => ({
  host: process.env.MQTT_HOST || 'localhost',
  port: parseInt(process.env.MQTT_PORT, 10) || 1883,
  username: process.env.MQTT_USERNAME || undefined,
  password: process.env.MQTT_PASSWORD || undefined,
  clientId: process.env.MQTT_CLIENT_ID || `nestjs-mqtt-svc-${Math.random().toString(16).substr(2, 8)}`,
  clean: process.env.MQTT_CLEAN_SESSION === 'true' || true,
  connectTimeout: parseInt(process.env.MQTT_CONNECT_TIMEOUT, 10) || 60000, // 60 seconds
  reconnectPeriod: parseInt(process.env.MQTT_RECONNECT_PERIOD, 10) || 5000, // 5 seconds
  // MQTT v5.0 specific properties
  protocolVersion: 5, // Use MQTT v5.0
  properties: {
    userProperties: {
      name: process.env.MQTT_USER_PROPERTIES_NAME || 'nestjs-mqtt-service',
      description: process.env.MQTT_USER_PROPERTIES_DESCRIPTION || 'NestJS MQTT Microservice Client',
      emoji: process.env.MQTT_USER_PROPERTIES_EMOJI || '🤖',
    }
  },
  // Private chat topic
  privateChatTopic: process.env.MQTT_PRIVATE_CHAT_TOPIC,
  // Group chat topics
  groupInviteTopic: process.env.MQTT_GROUP_INVITE_TOPIC || 'group/invite/+',
  groupDisbandTopic: process.env.MQTT_GROUP_DISBAND_TOPIC || 'group/disband/+',
  groupChatTopic: process.env.MQTT_GROUP_CHAT_TOPIC || 'group/+/chat/+',
  // OpenCode configuration
  opencodeBaseUrl: process.env.OPENCODE_BASE_URL || 'http://localhost:4096',
  opencodeApiKey: process.env.OPENCODE_API_KEY || '',
  opencodeWorkspaceDir: process.env.OPENCODE_WORKSPACE_DIR || process.cwd(),
  opencodeModel: process.env.OPENCODE_MODEL || undefined,  // Format: provider_id/model_id (e.g., anthropic/claude-sonnet-4-5)
  // MQTT session timeout: reuse sessions per sender/group, close after inactivity
  sessionTimeout: parseInt(process.env.MQTT_SESSION_TIMEOUT, 10) || 300000,
}));