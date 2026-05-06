# NestJS MQTT Microservice with OpenCode AI Integration

A NestJS microservice that handles MQTT message subscriptions using the built-in MQTT transport, enhanced with OpenCode AI integration for intelligent message processing.

## Features

- Subscribes to MQTT topics using decorators (`@MessagePattern`)
- Handles various topic patterns including wildcards (`+` and `#`)
- Processes different types of messages (test messages, user events, sensor data, device status)
- **NEW**: Integrated OpenCode AI for intelligent message analysis and processing
- **NEW**: Smart notifications and anomaly detection powered by OpenCode AI
- **NEW**: Context-aware message processing and recommendations

## Prerequisites

- Node.js v16 or higher
- An MQTT broker (e.g., Mosquitto, AWS IoT, Azure IoT Hub, etc.)

## Setup

1. Install dependencies:
```bash
npm install
```

2. Ensure EMQX broker is running:
   Make sure your EMQX broker is running on localhost:1883.
   If you need to use custom credentials, update the .env file accordingly.

3. Configure MQTT credentials:
   EMQX requires authentication. Update the .env file with valid credentials:
   ```bash
   # For default EMQX installation
   MQTT_USERNAME=admin
   MQTT_PASSWORD=public
   ```
   
   To find your EMQX credentials or create a new user, run:
   ```bash
   node test-emqx-connection.js
   ```

4. Run the microservice:
```bash
npm run start:dev
```

## Configuration

The microservice uses environment-based configuration management. All MQTT settings are controlled through environment variables in `.env` file.

### Environment Variables

Copy the example configuration:
```bash
cp .env.example .env
```

Then edit the `.env` file to match your MQTT broker settings:

```bash
# MQTT Broker Configuration
MQTT_HOST=your-mqtt-broker-host
MQTT_PORT=1883
MQTT_USERNAME=your-username
MQTT_PASSWORD=your-password
MQTT_CLIENT_ID=unique-client-id-for-this-service
MQTT_CLEAN_SESSION=true
MQTT_CONNECT_TIMEOUT=60000
MQTT_RECONNECT_PERIOD=5000
```

### Configuration Options

- `MQTT_HOST`: Address of your MQTT broker (default: localhost)
- `MQTT_PORT`: Port number (default: 1883)
- `MQTT_USERNAME`: Username for authentication (optional)
- `MQTT_PASSWORD`: Password for authentication (optional)
- `MQTT_CLIENT_ID`: Unique identifier for this client instance
- `MQTT_CLEAN_SESSION`: Whether to use clean sessions (default: true)
- `MQTT_CONNECT_TIMEOUT`: Timeout for connection attempts in ms (default: 60000)
- `MQTT_RECONNECT_PERIOD`: Delay between reconnection attempts in ms (default: 5000)
- `MQTT_USER_PROPERTIES_NAME`: Name property for MQTT v5.0 user properties (default: nestjs-mqtt-service)
- `MQTT_USER_PROPERTIES_DESCRIPTION`: Description property for MQTT v5.0 user properties (default: NestJS MQTT Microservice Client)
- `MQTT_USER_PROPERTIES_EMOJI`: Emoji property for MQTT v5.0 user properties (default: 🤖)
- `MQTT_PRIVATE_CHAT_TOPIC`: Topic pattern for private chat messages (default: private/chat/+)

For production environments, ensure credentials are securely managed and avoid committing sensitive information to version control.

## Testing

To publish test messages to the MQTT broker:

```bash
npm run publish:test
```

This will publish sample messages to different topics that the microservice subscribes to.

## Topic Patterns

The microservice subscribes to the following topics:

- `test/topic` - Basic test messages
- `user/events` - User login/logout events
- `sensor/+/temperature` - Temperature readings from sensors (wildcard)
- `device/+/status/+` - Device status updates (wildcard)
- `private/chat/+` - Private chat messages (configured via MQTT_PRIVATE_CHAT_TOPIC)

## MQTT v5.0 Features

The microservice utilizes MQTT protocol version 5.0 with the following features:

- **User Properties**: The client connects with custom user properties including:
  - `name`: Service identifier
  - `description`: Service description
  - `emoji`: Visual representation of the service
  
- **Private Chat Handling**: The service subscribes to a private chat topic (default: `private/chat/+`) to receive and process private messages between clients.

## Private Chat Message Processing

The microservice includes a dedicated handler for private chat messages that:
- Logs incoming private messages
- Processes sender, recipient, and message content
- Returns acknowledgment with processing metadata

## Architecture

- `main.ts` - Entry point that initializes the microservice with MQTT transport
- `app.module.ts` - Main application module
- `app.service.ts` - Service containing message handlers
- `services/opencode.service.ts` - OpenCode AI integration service
- `test-publisher.ts` - Test utility to publish messages to the broker
- `docker-compose.yml` - Configuration to run a local MQTT broker

## OpenCode AI Integration

This microservice includes OpenCode AI integration to provide intelligent processing of MQTT messages:

- **Message Analysis**: AI-powered analysis of MQTT messages for insights and anomaly detection
- **Smart Notifications**: Automatic generation of contextual notifications based on message content
- **Intelligent Routing**: Smart routing and handling of messages based on content analysis
- **Context Awareness**: Understanding of message context for appropriate processing

The integration uses the official OpenCode SDK and gracefully falls back to standard processing when the AI service is unavailable.

Configuration options include:

```bash
# OpenCode Configuration
OPENCODE_API_KEY=your-opencode-api-key-here
OPENCODE_BASE_URL=https://api.opencode.ai
OPENCODE_MODEL=gpt-4
```

## Troubleshooting

### Authentication Issues
If you encounter "Bad username or password" errors, this means the MQTT broker requires authentication.
- Update the credentials in `src/main.ts`
- Or configure your MQTT broker to allow anonymous connections

### Connection Refused
- Verify the MQTT broker is running
- Check the host and port settings in `src/main.ts`
- Ensure no firewall is blocking the connection

### For Development with Anonymous Access
To run Mosquitto with anonymous access, create this configuration file (`mosquitto.conf`):
```
listener 1883
allow_anonymous true
```

Then run: `mosquitto -c mosquitto.conf`