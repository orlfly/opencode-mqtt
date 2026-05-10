export interface MqttMessage {
  id: string;
  text?: string;
  senderId: string;
  timestamp: number | string;
  targetIds?: string[];
  type?: 'text' | 'file';
  fileName?: string;
  fileType?: string;
  fileData?: string;
  recipient?: string;
}
