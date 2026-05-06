import { Injectable } from '@nestjs/common';
import { OpenCodeService } from './services/opencode.service';

@Injectable()
export class AppService {
  constructor(private readonly opencodeService: OpenCodeService) {}
}