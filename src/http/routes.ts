import { Router, Request, Response, NextFunction } from 'express';
import { CommandService } from '../application/commandService.js';
import {
  CommandNotFoundError,
  DomainError,
  InvalidLeaseError,
  InvalidStateTransitionError,
  NoClaimableTaskError,
} from '../domain/errors.js';
import { CommandPayload } from '../domain/types.js';

export function createRouter(service: CommandService): Router {
  const router = Router();

  router.get('/health', (_req, res) => {
    res.json({ status: 'ok', timestamp: Date.now() });
  });

  router.post('/commands', async (req, res, next) => {
    try {
      const idempotencyKey = req.header('Idempotency-Key');
      if (!idempotencyKey) {
        res.status(400).json({ error: 'Idempotency-Key header is required' });
        return;
      }
      const body = req.body as {
        payload?: CommandPayload;
        maxAttempts?: number;
      };
      if (!body.payload || !body.payload.deviceId || !body.payload.action) {
        res
          .status(400)
          .json({ error: 'payload.deviceId and payload.action are required' });
        return;
      }
      const { command, created } = await service.submit({
        idempotencyKey,
        payload: body.payload,
        maxAttempts: body.maxAttempts,
      });
      res.status(created ? 201 : 200).json(serializeCommand(command));
    } catch (err) {
      next(err);
    }
  });

  router.get('/commands/:id', async (req, res, next) => {
    try {
      const command = await service.getCommand(req.params.id);
      if (!command) {
        res.status(404).json({ error: 'command not found' });
        return;
      }
      res.json(serializeCommand(command));
    } catch (err) {
      next(err);
    }
  });

  router.get('/commands/by-key/:key', async (req, res, next) => {
    try {
      const command = await service.getCommandByIdempotencyKey(req.params.key);
      if (!command) {
        res.status(404).json({ error: 'command not found' });
        return;
      }
      res.json(serializeCommand(command));
    } catch (err) {
      next(err);
    }
  });

  router.get('/commands/:id/events', async (req, res, next) => {
    try {
      const events = await service.getEvents(req.params.id);
      res.json({ events: events.map(serializeEvent) });
    } catch (err) {
      next(err);
    }
  });

  router.get('/commands', async (req, res, next) => {
    try {
      const limit = Math.min(Number(req.query.limit) || 100, 1000);
      const events = await service.getAllEvents(limit);
      const commandIds = [...new Set(events.map((e) => e.commandId))];
      const list = [];
      for (const id of commandIds) {
        const c = await service.getCommand(id);
        if (c) list.push(serializeCommand(c));
      }
      res.json({ commands: list });
    } catch (err) {
      next(err);
    }
  });

  router.post('/gateway/claim', async (req, res, next) => {
    try {
      const body = req.body as { gatewayId?: string; leaseDurationMs?: number; deviceId?: string };
      if (!body.gatewayId) {
        res.status(400).json({ error: 'gatewayId is required' });
        return;
      }
      const leaseDurationMs = body.leaseDurationMs ?? 30000;
      const command = await service.claim({
        gatewayId: body.gatewayId,
        leaseDurationMs,
        deviceId: body.deviceId,
      });
      res.json(serializeCommand(command));
    } catch (err) {
      next(err);
    }
  });

  router.post('/gateway/renew', async (req, res, next) => {
    try {
      const body = req.body as {
        commandId?: string;
        leaseId?: string;
        gatewayId?: string;
        leaseDurationMs?: number;
      };
      if (!body.commandId || !body.leaseId || !body.gatewayId) {
        res
          .status(400)
          .json({ error: 'commandId, leaseId, gatewayId are required' });
        return;
      }
      const command = await service.renew({
        commandId: body.commandId,
        leaseId: body.leaseId,
        gatewayId: body.gatewayId,
        leaseDurationMs: body.leaseDurationMs ?? 30000,
      });
      res.json(serializeCommand(command));
    } catch (err) {
      next(err);
    }
  });

  router.post('/gateway/report-delivery', async (req, res, next) => {
    try {
      const body = req.body as {
        commandId?: string;
        leaseId?: string;
        gatewayId?: string;
        deviceMessage?: string;
      };
      if (!body.commandId || !body.leaseId || !body.gatewayId) {
        res
          .status(400)
          .json({ error: 'commandId, leaseId, gatewayId are required' });
        return;
      }
      const command = await service.reportDelivery({
        commandId: body.commandId,
        leaseId: body.leaseId,
        gatewayId: body.gatewayId,
        deviceMessage: body.deviceMessage,
      });
      res.json(serializeCommand(command));
    } catch (err) {
      next(err);
    }
  });

  router.post('/gateway/confirm', async (req, res, next) => {
    try {
      const body = req.body as {
        commandId?: string;
        leaseId?: string;
        gatewayId?: string;
        confirmationCode?: string;
        deviceTimestamp?: number;
      };
      if (
        !body.commandId ||
        !body.leaseId ||
        !body.gatewayId ||
        !body.confirmationCode
      ) {
        res.status(400).json({
          error:
            'commandId, leaseId, gatewayId, confirmationCode are required',
        });
        return;
      }
      const { command, accepted } = await service.confirm({
        commandId: body.commandId,
        leaseId: body.leaseId,
        gatewayId: body.gatewayId,
        confirmationCode: body.confirmationCode,
        deviceTimestamp: body.deviceTimestamp ?? Date.now(),
      });
      res.json({ accepted, command: serializeCommand(command) });
    } catch (err) {
      next(err);
    }
  });

  router.get('/events', async (_req, res, next) => {
    try {
      const events = await service.getAllEvents(500);
      res.json({ events: events.map(serializeEvent) });
    } catch (err) {
      next(err);
    }
  });

  router.post('/admin/scan-expired', async (_req, res, next) => {
    try {
      const results = await service.scanExpiredLeases(100);
      res.json({ expired: results });
    } catch (err) {
      next(err);
    }
  });

  router.use(
    (err: unknown, _req: Request, res: Response, _next: NextFunction) => {
      if (err instanceof NoClaimableTaskError) {
        res.status(204).send();
        return;
      }
      if (err instanceof CommandNotFoundError) {
        res.status(404).json({ error: err.message, code: err.code });
        return;
      }
      if (err instanceof InvalidLeaseError) {
        res.status(409).json({
          error: err.message,
          code: err.code,
          details: err.details,
        });
        return;
      }
      if (err instanceof InvalidStateTransitionError) {
        res.status(409).json({
          error: err.message,
          code: err.code,
          details: err.details,
        });
        return;
      }
      if (err instanceof DomainError) {
        res.status(400).json({ error: err.message, code: err.code });
        return;
      }
      const message = err instanceof Error ? err.message : 'unknown error';
      res.status(500).json({ error: 'internal server error', detail: message });
    }
  );

  return router;
}

function serializeCommand(c: {
  commandId: string;
  idempotencyKey: string;
  payload: unknown;
  status: string;
  currentLeaseId: string | null;
  attempt: number;
  maxAttempts: number;
  leaseExpiresAt: number | null;
  gatewayId: string | null;
  confirmationCode: string | null;
  deviceTimestamp: number | null;
  failureReason: string | null;
  createdAt: number;
  updatedAt: number;
}) {
  return {
    commandId: c.commandId,
    idempotencyKey: c.idempotencyKey,
    payload: c.payload,
    status: c.status,
    currentLeaseId: c.currentLeaseId,
    attempt: c.attempt,
    maxAttempts: c.maxAttempts,
    leaseExpiresAt: c.leaseExpiresAt,
    gatewayId: c.gatewayId,
    confirmationCode: c.confirmationCode,
    deviceTimestamp: c.deviceTimestamp,
    failureReason: c.failureReason,
    createdAt: c.createdAt,
    updatedAt: c.updatedAt,
  };
}

function serializeEvent(e: {
  eventId: string;
  commandId: string;
  eventType: string;
  leaseId: string | null;
  attempt: number | null;
  gatewayId: string | null;
  payload: unknown;
  causedBy: string;
  timestamp: number;
}) {
  return {
    eventId: e.eventId,
    commandId: e.commandId,
    eventType: e.eventType,
    leaseId: e.leaseId,
    attempt: e.attempt,
    gatewayId: e.gatewayId,
    payload: e.payload,
    causedBy: e.causedBy,
    timestamp: e.timestamp,
  };
}
