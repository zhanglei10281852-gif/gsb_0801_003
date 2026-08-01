# edge-dispatch

A local, single-process backend that reliably delivers device commands
(calibration, process switching, …) to **edge gateways that go offline
frequently**, and can answer the operational question that matters on a factory
floor: *did this instruction actually run on the device, or not?*

It is built to survive the two worst failure moments:

1. the service crashes **after persisting** but **before returning** its
   response, and
2. a gateway leases a task and then **disappears**.

In both cases a restart or retry must **not** turn one business request into two
device actions, retries and re-leases must reuse a **stable execution identity**,
only a **persisted device confirmation** may mark a command succeeded, and an
**expired task must never be silently revived** by a late message.

Stack: **Node.js 20+, TypeScript, SQLite** (via `better-sqlite3`). No Docker, no
Redis, no remote services. Everything runs from a local SQLite file.

---

## Quick start

```bash
npm install      # installs deps (compiles the better-sqlite3 native addon)
npm test         # deterministic domain + service + HTTP/SQLite tests
npm run build    # tsc -> dist/
npm start        # runs the compiled service (dist/main.js)
npm run e2e      # builds, starts the compiled service, drives the simulator,
                 # and runs a real crash-recovery check; prints PASS/FAIL
```

Fixed entry points (all defined in [package.json](file:///e:/newGsb/questions/GSB-003/Thor/package.json)):

| Command | What it does |
| --- | --- |
| `npm install` | install dependencies |
| `npm test` | run all deterministic tests (no network, controllable clock) |
| `npm run build` | compile TypeScript to `dist/` |
| `npm start` | start the compiled backend service |
| `npm run e2e` | end-to-end acceptance: real compiled server + simulator + crash recovery |
| `npm run sim -- <scenario>` | run one simulator scenario against a running server |

### Configuration (environment variables)

| Var | Default | Meaning |
| --- | --- | --- |
| `PORT` | `8080` | HTTP port |
| `DB_FILE` | `data/dispatch.sqlite` | SQLite file (created if missing) |
| `LEASE_MS` | `10000` | lease duration granted to a gateway |
| `MAX_ATTEMPTS` | `5` | how many times a command may be (re)leased before it FAILS |
| `SWEEP_MS` | `1000` | how often the background reaper requeues/fails expired leases |

---

## Architecture — why failures are deterministically testable

The domain state machine is **decoupled** from HTTP, gateway transport, and
storage. Non-determinism (time, id generation) is injected, so any failure
interleaving can be reproduced exactly in a unit test.

```
          upstream (scheduler)                 edge gateway
                  │                                  │
                  ▼                                  ▼
        ┌───────────────────────── HTTP adapter ──────────────────────┐
        │  src/http/server.ts  (Node core http, no framework)          │
        └───────────────────────────────┬──────────────────────────────┘
                                         │  calls only the application service
                                         ▼
        ┌──────────────── application service (orchestration) ─────────┐
        │  src/app/dispatchService.ts                                  │
        │  - one atomic transaction per operation                      │
        │  - reads state, runs a PURE transition, persists cmd+events  │
        └───────────────┬───────────────────────────┬──────────────────┘
                        │ ports (interfaces)         │
        ┌───────────────▼──────────┐      ┌──────────▼───────────────────┐
        │ Repository / Clock / Id  │      │ PURE domain state machine     │
        │ src/ports.ts             │      │ src/domain/stateMachine.ts    │
        │  - SqliteRepository      │      │  - no I/O, no clock            │
        │  - InMemoryRepository    │      │  - returns (command, events)   │
        │  - systemClock / uuidIds │      │                                │
        └──────────────────────────┘      └────────────────────────────────┘
```

- **Domain** ([src/domain/stateMachine.ts](file:///e:/newGsb/questions/GSB-003/Thor/src/domain/stateMachine.ts)):
  pure functions `create / lease / renew / confirm / expire`. Each takes the
  current command plus inputs (including an injected `now`) and returns the next
  command, the causal events to append, and a transport-agnostic outcome. This
  is where every correctness invariant lives.
- **Application** ([src/app/dispatchService.ts](file:///e:/newGsb/questions/GSB-003/Thor/src/app/dispatchService.ts)):
  wraps each domain transition in a single atomic repository transaction and
  persists the command **and** its events together.
- **Ports** ([src/ports.ts](file:///e:/newGsb/questions/GSB-003/Thor/src/ports.ts)):
  `Repository`, `Clock`, `IdGenerator`. Swapped for fakes in tests
  (`InMemoryRepository`, `ManualClock`, sequential ids) so timings are exact.
- **Adapters**: [SqliteRepository](file:///e:/newGsb/questions/GSB-003/Thor/src/adapters/sqliteRepository.ts)
  (durable, WAL + `synchronous=FULL`), [InMemoryRepository](file:///e:/newGsb/questions/GSB-003/Thor/src/adapters/inMemoryRepository.ts)
  (tests), and the [system clock / uuid](file:///e:/newGsb/questions/GSB-003/Thor/src/adapters/clock.ts) source.

### The lifecycle

```
 submit                lease                 confirm(success)
──────────▶ PENDING ─────────────▶ LEASED ────────────────────▶ SUCCEEDED (terminal)
              ▲                    │  │
              │ requeue (budget    │  │ confirm(failure)
              │ remains)           │  └──────────────────────────▶ FAILED (terminal)
              │                    │
              └──── expire ────────┘  (lease deadline passed; reaper or next lease)
                                       exhausted budget ─────────▶ FAILED (terminal)
```

### Two identities that make it safe

- **`executionId`** — the *stable execution identity*. Minted once at submit
  time, immutable, and carried on every (re)lease. A re-lease after a crash
  hands the gateway the **same** `executionId`, so the device (which deduplicates
  on it) never performs the action twice.
- **`attemptEpoch`** — a monotonic *fencing token*, bumped on every lease
  acquisition. A renew/confirm that does not match the **current, still-valid**
  lease is rejected. This is what prevents a stale/expired lease holder from
  reviving or completing a command that has moved on.

### Causal record, not just the final value

Every state change appends immutable rows to an `events` table:
`SUBMITTED, SUBMIT_DEDUPED, LEASED, RENEWED, RENEW_REJECTED, LEASE_EXPIRED,
REQUEUED, CONFIRMED_SUCCESS, CONFIRMED_FAILURE, CONFIRM_IGNORED, EXHAUSTED`.
Operators can reconstruct *why* a command is where it is (each timeout, retry and
ignored late message) via `GET /commands/:id/events` and `GET /ops/events`.

---

## HTTP API

**Upstream (scheduling system)**

- `POST /commands` — submit a command. Body:
  `{ "idempotencyKey": "...", "deviceId": "...", "kind": "calibrate", "params": {}, "maxAttempts": 5 }`.
  Returns `201` on first create, `200` with `"deduped": true` on any resubmit of
  the same `idempotencyKey` (returns the existing command — **never** a second one).
- `GET /commands/:id` — current state of one command.
- `GET /commands?key=<idempotencyKey>` — look up by business key.
- `GET /commands/:id/events` — full causal history for that command.

**Gateway (edge)**

- `POST /gateway/lease` — `{ "leaseholder": "gw-1", "deviceId": "optional" }`.
  Returns `200` with `{ commandId, executionId, leaseId, payload, leaseExpiresAt }`
  or `204` when nothing is available. `deviceId` optionally restricts leasing to
  the devices a gateway serves.
- `POST /gateway/renew` — `{ commandId, leaseId }`. `200` if extended, `409` if
  the lease is stale/expired.
- `POST /gateway/confirm` — `{ commandId, leaseId, outcome: "success"|"failure", deviceReceipt? }`.
  `200` accepted (moves to terminal state), `409` ignored (stale/expired lease or
  already terminal). Confirming an already-terminal command is idempotent.

**Ops**

- `GET /ops/commands?status=PENDING|LEASED|SUCCEEDED|FAILED` — list.
- `GET /ops/events?limit=100` — recent causal events across all commands.
- `POST /ops/sweep` — force an expiry sweep now (also runs every `SWEEP_MS`).
- `GET /healthz` — liveness.

---

## Simulator

A scriptable gateway/device simulator ([src/sim](file:///e:/newGsb/questions/GSB-003/Thor/src/sim))
drives a **running** service over HTTP so you can reproduce disconnection,
duplicate submission, and out-of-order / duplicate confirmations. The simulated
device executes **at most once per `executionId`**, exactly like a real
controller that ignores a re-delivered command.

```bash
npm start                                   # terminal 1
npm run sim -- happy                        # terminal 2
npm run sim -- drop-confirm --url http://127.0.0.1:8080 --lease-ms 1500
```

| Scenario | Reproduces | Asserts |
| --- | --- | --- |
| `happy` | normal path | submit → lease → confirm → SUCCEEDED, one device action |
| `duplicate-submit` | upstream retries the same key | exactly one command, later submits deduped |
| `drop-confirm` | device acts, then network drops the confirmation | re-lease reuses the same `executionId`; device acts **once**; ends SUCCEEDED |
| `vanish` | gateway leases then disappears | reaper requeues; another gateway finishes; one device action |
| `stale-confirm` | late confirmation from an expired lease | confirmation rejected; command **not** revived |
| `duplicate-confirm` | at-least-once confirmation relay | idempotent; one terminal transition |

Each scenario prints a report and exits `0` on pass, non-zero on fail.

### End-to-end acceptance (`npm run e2e`)

[src/e2e/acceptance.ts](file:///e:/newGsb/questions/GSB-003/Thor/src/e2e/acceptance.ts)
compiles the project, starts the **compiled** server (`dist/main.js`) as a child
process against a fresh temp SQLite file, runs every simulator scenario as
separate processes, prints a sample of the causal event log, and then performs a
**crash-recovery check**: it `SIGKILL`s the server after a command is persisted,
restarts it against the same database file, and verifies the command survived
with the same `executionId`, that a resubmit dedupes, and that it can still be
completed exactly once. Exits non-zero if anything fails.

---

## Delivery guarantees

- **At-most-once device action per business request.** The upstream
  `idempotencyKey` is `UNIQUE`; a resubmit returns the existing command. The
  stable `executionId` is minted once and reused across every re-lease, and the
  device deduplicates on it. A crash between persistence and response, or a
  retried submit, cannot create a second action.
- **At-least-once delivery to the gateway.** A command that is leased but not
  confirmed before its lease expires is requeued (up to `MAX_ATTEMPTS`) and
  re-leased with the same `executionId`. Work is not lost when a gateway drops
  offline.
- **Success only on persisted proof.** A command becomes `SUCCEEDED` **only** via
  a device confirmation that matches the current, unexpired lease and is
  committed to SQLite. No optimistic or inferred success.
- **No zombie revival.** `attemptEpoch`/`leaseId` fencing means a renew or
  confirm from an expired or superseded lease is rejected and audited
  (`CONFIRM_IGNORED` / `RENEW_REJECTED`), never applied.
- **Durability.** SQLite runs in WAL mode with `synchronous = FULL`, so an
  acknowledged (committed) write survives a process crash or power loss.
  Command mutations and their causal events commit in a **single transaction**.
- **Explainable causality.** Every transition appends an event; the final status
  is always backed by an ordered, inspectable history.

## What this design does NOT promise (honest limits)

- **Not exactly-once end-to-end in the strict distributed sense.** True
  exactly-once device *effect* requires the device itself to deduplicate on
  `executionId`. The service guarantees a stable identity and at-most-once
  *dispatch decision*; the simulated device demonstrates the required device-side
  dedup. A device that ignores `executionId` can still double-execute — that is
  outside this service's control.
- **Single-node only.** Correctness relies on SQLite's single-writer
  transactions. There is no multi-node replication or leader election; running
  two instances on the same file concurrently is **not** supported.
- **The lease timeout is a liveness heuristic, not proof of failure.** If a
  gateway is merely slow (not dead) and lets its lease lapse, the command may be
  re-leased. Because delivery is at-least-once and the device dedups on
  `executionId`, this is safe, but it means a genuinely-still-working gateway can
  have its lease taken over. Tune `LEASE_MS` and use `renew` accordingly.
- **A dropped confirmation costs one lease interval.** If the device succeeded
  but the confirmation is lost, the service cannot know until the lease expires
  and the task is re-leased/re-confirmed. During that window the command shows
  `LEASED`, not `SUCCEEDED`.
- **`FAILED` after `MAX_ATTEMPTS` is "unknown outcome", not "device rejected".**
  Retry exhaustion means we never received a confirmation; the physical device
  state is undetermined and needs operator follow-up.
- **No authentication / authorization / transport encryption.** This is a local
  reference backend; put it behind your own trust boundary.

## Recovery boundaries (what a restart guarantees)

- The SQLite file is the single source of truth. On startup there is **no
  in-memory state to rebuild**; the process simply reopens the file.
- A command that was `SUCCEEDED`/`FAILED` before the crash stays terminal.
- A command left `LEASED` whose lease has expired is requeued by the reaper (or
  taken over on the next lease). One that is still within its lease window is
  left alone until it expires.
- A command left `PENDING` is simply available to lease again.
- Only writes that **committed** before the crash survive. A request whose
  transaction had not committed is lost — which is exactly why upstream must
  retry with the same `idempotencyKey` (safe) and gateways must re-lease
  (safe, identity preserved).

---

## Layout

```
src/
  domain/          pure state machine + types (no I/O)
  ports.ts         Repository / Clock / IdGenerator interfaces
  adapters/        sqliteRepository, inMemoryRepository, clock/id
  app/             DispatchService (orchestration, transactions)
  http/            Node core http adapter
  sim/             httpClient, device, gateway, scenario CLI
  e2e/             acceptance runner (compiled server + simulator + crash test)
  main.ts          composition root (wiring + reaper + shutdown)
test/
  domain.test.ts   pure transitions, manual timestamps
  service.test.ts  crash/timeout interleavings via in-memory repo + manual clock
  http.test.ts     real SQLite + http server integration
```
