# edge-dispatch

A local, single-process backend that reliably delivers device commands
(calibration, process switching, …) to **edge gateways that go offline
frequently**, and can answer the operational question that matters on a factory
floor: *did this instruction actually run on the device, or not?*

It is built to survive the worst failure moments on a line with **redundant
(active/standby) gateways**:

1. the service crashes **after persisting** but **before returning** its
   response,
2. a gateway leases a task and then **disappears**, and
3. a **network partition** where a taken-over primary is still sending, while a
   standby has already taken over the line.

In all cases a restart, retry, or takeover must **not** turn one business
request into two device actions; retries, re-leases and takeovers must reuse a
**stable execution identity**; only a **persisted device confirmation** may mark
a command succeeded; and an **expired or superseded task must never be silently
revived** by a late message.

Redundant gateways are coordinated by a per-line **ownership lease with a
monotonic generation (代际)**: a standby may take over only after the primary's
heartbeat lapses, takeover bumps the generation, and every command operation is
fenced by generation so an old primary cannot lease work or push a confirmation
after it has been superseded.

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
| `OWNERSHIP_TTL_MS` | `15000` | line-ownership heartbeat TTL; a standby may take over once it lapses |

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
        │ Repository / Clock / Id  │      │ PURE domain state machines    │
        │ src/ports.ts             │      │ src/domain/stateMachine.ts    │
        │  - SqliteRepository      │      │ src/domain/ownership.ts       │
        │    (commands, events,    │      │  - no I/O, no clock            │
        │     line_ownership)      │      │  - command transitions + 代际  │
        │  - InMemoryRepository    │      │    generation fencing          │
        │  - systemClock / uuidIds │      │  - returns (state, events)     │
        └──────────────────────────┘      └────────────────────────────────┘
```

- **Domain** ([stateMachine.ts](file:///e:/newGsb/questions/GSB-003/Thor/src/domain/stateMachine.ts)
  + [ownership.ts](file:///e:/newGsb/questions/GSB-003/Thor/src/domain/ownership.ts)):
  pure functions `create / lease / renew / confirm / expire` for commands and
  `acquire / claim` for line ownership. Each takes current state plus inputs
  (including an injected `now` and, for command ops, the caller vs. current
  generation) and returns the next state, the causal events to append, and a
  transport-agnostic outcome. This is where every correctness invariant lives,
  including `attemptEpoch` and `ownerGeneration` fencing.
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

### Three identities that make it safe

- **`executionId`** — the *stable execution identity*. Minted once at submit
  time, immutable, and carried on every (re)lease **and every takeover**. A
  re-lease after a crash or a takeover by a standby hands the gateway the
  **same** `executionId`, so the device (which deduplicates on it) never performs
  the action twice.
- **`attemptEpoch`** — a monotonic per-command *fencing token*, bumped on every
  lease acquisition. A renew/confirm that does not match the **current,
  still-valid** lease is rejected. This prevents a stale/expired lease holder
  from reviving or completing a command that has moved on.
- **`ownerGeneration` (代际)** — a monotonic *per-line ownership fencing token*.
  A line is owned by one gateway at a time via a heartbeat lease; a standby may
  take over only once the owner's heartbeat lapses, and takeover increments the
  generation. Every lease/renew/confirm carries the caller's generation, and the
  service reads the line's authoritative generation **inside the same
  transaction**: any operation stamped with an older generation is fenced. This
  is the split-brain guard — a partitioned old primary can neither lease new work
  nor push a late confirmation after it has been superseded, even while it still
  locally believes it holds a valid command lease. A newer generation may also
  **preempt** an in-flight command still held by a superseded generation, reusing
  the same `executionId`.

### Causal record, not just the final value

Every state change appends immutable rows to an `events` table. Events are
**command-scoped** or **line-scoped** (ownership) and carry `attemptEpoch` and
`generation` so a takeover's before/after causality is fully reconstructable:

- command: `SUBMITTED, SUBMIT_DEDUPED, LEASED, RENEWED, RENEW_REJECTED,
  LEASE_EXPIRED, REQUEUED, CONFIRMED_SUCCESS, CONFIRMED_FAILURE, CONFIRM_IGNORED,
  EXHAUSTED, LEASE_FENCED, PREEMPTED`
- line: `OWNERSHIP_ACQUIRED, OWNERSHIP_RENEWED, OWNERSHIP_TAKEOVER,
  OWNERSHIP_REJECTED`

Operators reconstruct *why* a command is where it is (each timeout, retry,
takeover, fenced/ignored late message) via `GET /commands/:id/events`,
`GET /ops/lines/:lineId/events`, and `GET /ops/events`.

---

## HTTP API

**Upstream (scheduling system)**

- `POST /commands` — submit a command. Body:
  `{ "idempotencyKey": "...", "deviceId": "...", "kind": "calibrate", "params": {}, "maxAttempts": 5, "lineId": "optional" }`.
  Returns `201` on first create, `200` with `"deduped": true` on any resubmit of
  the same `idempotencyKey` (returns the existing command — **never** a second
  one). `lineId` defaults to `"default"`.
- `GET /commands/:id` — current state of one command.
- `GET /commands?key=<idempotencyKey>` — look up by business key.
- `GET /commands/:id/events` — full causal history for that command.

**Gateway (edge, redundant)**

- `POST /gateway/ownership` — `{ "lineId": "...", "gateway": "gw-1" }`. Claim,
  heartbeat, or take over a line. Returns `200` with
  `{ outcome: "acquired"|"renewed"|"takeover", generation, owner, expiresAt }`,
  or `409` with `outcome: "rejected"` when a healthy owner already holds the
  line. The gateway must stamp the returned `generation` on subsequent
  operations.
- `POST /gateway/lease` — `{ "leaseholder": "gw-1", "lineId?", "deviceId?", "generation?" }`.
  Returns `200` with `{ commandId, executionId, leaseId, lineId, ownerGeneration, preempted, payload, leaseExpiresAt }`
  or `204` when nothing is available or the caller's generation is fenced.
- `POST /gateway/renew` — `{ commandId, leaseId, generation? }`. `200` if
  extended, `409` if the lease is stale/expired or the generation is stale.
- `POST /gateway/confirm` — `{ commandId, leaseId, outcome: "success"|"failure", deviceReceipt?, generation? }`.
  `200` accepted (moves to terminal state), `409` ignored (stale/expired lease,
  stale generation, or already terminal). Confirming an already-terminal command
  is idempotent.

**Ops**

- `GET /ops/commands?status=PENDING|LEASED|SUCCEEDED|FAILED` — list.
- `GET /ops/events?limit=100` — recent causal events across all subjects.
- `GET /ops/ownership` — current ownership record (owner + generation) per line.
- `GET /ops/lines/:lineId/events` — a line's ownership causal history.
- `POST /ops/sweep` — force an expiry sweep now (also runs every `SWEEP_MS`).
- `GET /healthz` — liveness.

---

## Simulator

A scriptable gateway/device simulator ([src/sim](file:///e:/newGsb/questions/GSB-003/Thor/src/sim))
drives a **running** service over HTTP so you can reproduce disconnection,
duplicate submission, out-of-order / duplicate confirmations, and
redundant-gateway takeover / split-brain. The simulated device executes **at most
once per `executionId`**, exactly like a real controller that ignores a
re-delivered command. A gateway can claim line ownership and stamp its generation
on every operation; `forceGeneration` lets a scenario model a partitioned gateway
that never learned it was taken over.

```bash
npm start                                   # terminal 1
npm run sim -- happy                        # terminal 2
npm run sim -- takeover --url http://127.0.0.1:8080 --lease-ms 1500
```

| Scenario | Reproduces | Asserts |
| --- | --- | --- |
| `happy` | normal path | submit → lease → confirm → SUCCEEDED, one device action |
| `duplicate-submit` | upstream retries the same key | exactly one command, later submits deduped |
| `drop-confirm` | device acts, then network drops the confirmation | re-lease reuses the same `executionId`; device acts **once**; ends SUCCEEDED |
| `vanish` | gateway leases then disappears | reaper requeues; another gateway finishes; one device action |
| `stale-confirm` | late confirmation from an expired lease | confirmation rejected; command **not** revived |
| `duplicate-confirm` | at-least-once confirmation relay | idempotent; one terminal transition |
| `takeover` | primary owns a line, leases, vanishes; standby takes over | standby refused while primary healthy, then takeover bumps 代际; same `executionId`; one device action |
| `split-brain` | partitioned old primary keeps its stale generation | old primary's lease attempt fenced (`204`) and its late confirm fenced (`stale_generation`); device acted once; standby completes it |
| `ownership-expiry` | standby claims a healthy vs. lapsed line | rejected while healthy, heartbeat keeps generation stable, takeover after lapse bumps generation |

Each scenario prints a report and exits `0` on pass, non-zero on fail.

### End-to-end acceptance (`npm run e2e`)

[src/e2e/acceptance.ts](file:///e:/newGsb/questions/GSB-003/Thor/src/e2e/acceptance.ts)
compiles the project, starts the **compiled** server (`dist/main.js`) as a child
process against a fresh temp SQLite file, runs every simulator scenario as
separate processes, prints a sample of the causal event log, and then performs
two recovery checks:

- **crash-recovery** — `SIGKILL`s the server after a command is persisted,
  restarts it against the same database file, and verifies the command survived
  with the same `executionId`, that a resubmit dedupes, and that it can still be
  completed exactly once.
- **ownership-recovery** — after a takeover bumps the generation, `SIGKILL`s and
  restarts the server, and verifies the persisted generation is unchanged (an old
  generation stays fenced across restarts).

Exits non-zero if anything fails.

---

## Delivery guarantees

- **At-most-once device action per business request.** The upstream
  `idempotencyKey` is `UNIQUE`; a resubmit returns the existing command. The
  stable `executionId` is minted once and reused across every re-lease **and
  every takeover**, and the device deduplicates on it. A crash between
  persistence and response, a retried submit, or a standby takeover cannot create
  a second action.
- **At-least-once delivery to the gateway.** A command that is leased but not
  confirmed before its lease expires is requeued (up to `MAX_ATTEMPTS`) and
  re-leased with the same `executionId`. Work is not lost when a gateway drops
  offline.
- **Success only on persisted proof.** A command becomes `SUCCEEDED` **only** via
  a device confirmation that matches the current, unexpired lease, carries a
  non-stale generation, and is committed to SQLite. No optimistic or inferred
  success.
- **No zombie revival, and no split-brain progress.** `attemptEpoch`/`leaseId`
  fencing rejects a renew/confirm from an expired or superseded per-command
  lease; `ownerGeneration` fencing rejects any lease/renew/confirm from a
  superseded line generation — even one holding an otherwise-valid, still-live
  lease. Both are audited (`CONFIRM_IGNORED` / `RENEW_REJECTED` / `LEASE_FENCED`),
  never applied.
- **Ordered, monotonic takeover.** A standby takes over a line only after the
  owner's heartbeat lapses; generation strictly increases and is persisted, so a
  restarted or partitioned old owner can never regain authority for its old
  generation.
- **Durability.** SQLite runs in WAL mode with `synchronous = FULL`, so an
  acknowledged (committed) write survives a process crash or power loss.
  Command/ownership mutations and their causal events commit in a **single
  transaction**.
- **Explainable causality.** Every transition appends an event; the final status
  is always backed by an ordered, inspectable history that includes ownership
  takeovers and fenced messages.

## What this design does NOT promise (honest limits)

- **Not exactly-once end-to-end in the strict distributed sense.** True
  exactly-once device *effect* requires the device itself to deduplicate on
  `executionId`. The service guarantees a stable identity and at-most-once
  *dispatch decision*; the simulated device demonstrates the required device-side
  dedup. A device that ignores `executionId` can still double-execute — that is
  outside this service's control.
- **Single-node only.** Correctness relies on SQLite's single-writer
  transactions. There is no multi-node replication or leader election; running
  two instances on the same file concurrently is **not** supported. Note this is
  about the *dispatch service*; the redundant *gateways* it coordinates are fully
  supported via line ownership.
- **Ownership is fenced at the service, not at the device wire.** A superseded
  gateway is prevented from advancing service state, but if it has a direct
  physical link it could still actuate hardware. The stable `executionId` means a
  correct device dedups a re-sent action; true actuator-level mutual exclusion
  (e.g. a hardware interlock) is out of scope.
- **The lease/heartbeat timeouts are liveness heuristics, not proof of failure.**
  A merely-slow (not dead) gateway that lets its lease or ownership heartbeat
  lapse can be taken over. Because delivery is at-least-once and both the device
  (`executionId`) and the service (`generation`) dedup/fence, this is safe, but a
  still-working primary can lose the line. Tune `LEASE_MS` / `OWNERSHIP_TTL_MS`
  and heartbeat/renew accordingly.
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
- **Line ownership and its generation survive the restart.** The current
  generation is persisted, so an old-generation gateway that reconnects after a
  restart is still fenced; the standby that took over remains authoritative.
- Only writes that **committed** before the crash survive. A request whose
  transaction had not committed is lost — which is exactly why upstream must
  retry with the same `idempotencyKey` (safe), gateways must re-lease (safe,
  identity preserved), and a gateway re-establishes ownership via
  `POST /gateway/ownership` on reconnect (safe, generation-fenced).

---

## Layout

```
src/
  domain/          pure state machines + types (no I/O)
    types.ts         Command, LineOwnership, DomainEvent, transitions
    stateMachine.ts  command transitions + attemptEpoch/generation fencing
    ownership.ts     line ownership: acquire / heartbeat / takeover (代际)
  ports.ts         Repository / Clock / IdGenerator interfaces
  adapters/        sqliteRepository, inMemoryRepository, clock/id
  app/             DispatchService (orchestration, transactions, ownership)
  http/            Node core http adapter
  sim/             httpClient, device, gateway (ownership-aware), scenario CLI
  e2e/             acceptance runner (compiled server + simulator + crash/ownership recovery)
  main.ts          composition root (wiring + reaper + shutdown)
test/
  domain.test.ts     pure command transitions, manual timestamps
  ownership.test.ts  ownership代际 + generation fencing / takeover / split-brain / preemption
  service.test.ts    crash/timeout interleavings via in-memory repo + manual clock
  http.test.ts       real SQLite + http server integration (incl. ownership + fencing)
```
