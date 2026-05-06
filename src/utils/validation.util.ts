/**
 * Utility functions for validating configuration values
 */

export function validatePort(port: number): boolean {
  return Number.isInteger(port) && port > 0 && port <= 65535;
}

export function validateHost(host: string): boolean {
  if (!host || typeof host !== 'string') {
    return false;
  }
  // Basic validation for hostname/IP address
  const hostnameRegex = /^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9])$/;
  const ipRegex = /^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/;
  return hostnameRegex.test(host) || ipRegex.test(host) || host === 'localhost';
}

export function validateClientId(clientId: string): boolean {
  if (!clientId || typeof clientId !== 'string') {
    return false;
  }
  // MQTT spec says client ID must be 1-23 characters
  if (clientId.length < 1 || clientId.length > 23) {
    return false;
  }
  // Should not contain invalid characters
  const invalidChars = /[+#]/;
  return !invalidChars.test(clientId);
}