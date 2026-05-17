import { Injectable, Logger } from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';

@Injectable()
export class SessionManagerService {
  private readonly logger = new Logger(SessionManagerService.name);

  private readonly replyTopicMap: Map<string, string> = new Map();
  private readonly displayNameMap: Map<string, string> = new Map();
  private readonly groupMembersMap: Map<string, Set<string>> = new Map();

  constructor(private eventEmitter: EventEmitter2) {}

  private sanitizeKey(input: string): string {
    return input.replace(/[^a-zA-Z0-9_-]/g, '_').substring(0, 64);
  }

  getOrCreatePrivateSession(
    senderId: string,
    senderName: string,
    replyTopic: string,
  ): { sessionID: string; messageID: string } {
    const messageID = `msg_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const sessionID = `ses_mqtt_${this.sanitizeKey(senderId)}`;

    this.replyTopicMap.set(senderId, replyTopic);
    if (senderName && senderName !== senderId) {
      this.displayNameMap.set(senderName, senderId);
    }

    this.logger.log(`Private session: ${sessionID} for sender ${senderId}, reply: ${replyTopic}`);
    return { sessionID, messageID };
  }

  getOrCreateGroupSession(
    groupTopic: string,
    senderId: string,
    senderName: string,
    replyTopic: string,
  ): { sessionID: string; messageID: string } {
    const messageID = `msg_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const sessionID = `ses_mqtt_grp_${this.sanitizeKey(groupTopic)}`;

    this.replyTopicMap.set(senderId, replyTopic);
    if (senderName && senderName !== senderId) {
      this.displayNameMap.set(senderName, senderId);
    }

    if (!this.groupMembersMap.has(groupTopic)) {
      this.groupMembersMap.set(groupTopic, new Set());
    }
    this.groupMembersMap.get(groupTopic)!.add(senderId);

    this.logger.log(`Group session: ${sessionID} for group ${groupTopic}, sender ${senderId}`);
    return { sessionID, messageID };
  }

  addGroupMember(groupTopic: string, senderId: string): void {
    if (!this.groupMembersMap.has(groupTopic)) {
      this.groupMembersMap.set(groupTopic, new Set());
    }
    this.groupMembersMap.get(groupTopic)!.add(senderId);
  }

  dismissGroup(groupTopic: string): string | undefined {
    const sessionID = `ses_mqtt_grp_${this.sanitizeKey(groupTopic)}`;

    const members = this.groupMembersMap.get(groupTopic);
    if (members) {
      for (const memberId of members) {
        this.replyTopicMap.delete(memberId);
      }
      this.groupMembersMap.delete(groupTopic);
    }

    this.eventEmitter.emit('session.closed', { sessionID });
    this.logger.log(`Dismissed group session ${sessionID} for group ${groupTopic}`);
    return sessionID;
  }

  getReplyTopic(senderId: string): string | undefined {
    return this.replyTopicMap.get(senderId);
  }

  cleanupAll(): void {
    this.replyTopicMap.clear();
    this.displayNameMap.clear();
    this.groupMembersMap.clear();
  }
}
