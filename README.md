# Edge Command Dispatch

一个可在 Windows 本地运行的边缘设备指令可靠下发后端服务。上游系统用业务幂等键提交校准、切换工艺、复位等指令；边缘网关通过租约领取任务、续约并回传设备确认；运维可通过事件流追溯每次提交、投递、租约过期、重试、设备确认和最终终态。

技术栈：Node.js 20、TypeScript、SQLite（`better-sqlite3`）。运行时不依赖 Docker、Redis 或远程托管服务。

## 快速开始

```powershell
npm run install:deps
npm run build
npm start
```

默认监听 `http://127.0.0.1:8080`，SQLite 数据库位于 `./data/edge-commands.db`。

运行测试与端到端验收：

```powershell
npm test
npm run acceptance
```

`npm run acceptance` 会先编译，再真实启动编译后的 HTTP 服务，用临时数据库驱动模拟器完成断网、重复提交、租约过期、重复/乱序确认、服务崩溃重启恢复等验收检查。

启动后可用模拟器：

```powershell
npm run simulate -- scenario all
npm run simulate -- submit --key order-123 --device line-1 --type CALIBRATE
npm run simulate -- poll --gateway gw-1
npm run simulate -- query --key order-123
npm run simulate -- events
```

## 架构与可靠性模型

核心分层如下：

- 领域状态机：[src/domain/command.ts](file:///e:/newGsb/questions/GSB-003/Tony/src/domain/command.ts)
- 领域类型：[src/domain/types.ts](file:///e:/newGsb/questions/GSB-003/Tony/src/domain/types.ts)
- SQLite 适配器：[src/adapters/sqlite-store.ts](file:///e:/newGsb/questions/GSB-003/Tony/src/adapters/sqlite-store.ts)
- 内存适配器（确定性测试）：[src/adapters/memory-store.ts](file:///e:/newGsb/questions/GSB-003/Tony/src/adapters/memory-store.ts)
- 应用服务：[src/application/command-service.ts](file:///e:/newGsb/questions/GSB-003/Tony/src/application/command-service.ts)
- HTTP 服务：[src/server/http-server.ts](file:///e:/newGsb/questions/GSB-003/Tony/src/server/http-server.ts)
- 网关/设备模拟器：[tools/simulator](file:///e:/newGsb/questions/GSB-003/Tony/tools/simulator)

领域逻辑是纯函数：`decide*` 只根据当前状态和输入产生事件，`apply` 把事件折叠成状态。它不直接访问 HTTP、SQLite 或系统时钟，因此所有故障时序都可用注入时间和事件 ID 进行确定性测试。

### 状态机

```text
PENDING --claim(leaseId, generation+1, attempt+1)--> CLAIMED
CLAIMED --renew(matching generation)----------------> CLAIMED
CLAIMED --ack success(matching generation)----------> DELIVERED (terminal)
CLAIMED --ack failure/nack(matching generation)-----> FAILED    (terminal)
PENDING/CLAIMED --cancel----------------------------> CANCELLED (terminal)
PENDING/CLAIMED --replace---------------------------> CANCELLED (terminal) + 新 PENDING
CLAIMED --lease expiry------------------------------> PENDING
PENDING --max attempts or max age-------------------> TIMED_OUT (terminal)
```

关键设计：

- `commandId` 在首次接受业务幂等键时生成并持久化，后续重复提交、网关失联重试、主备接管都沿用同一个稳定执行身份。已经发给设备的重试不会生成第二条业务指令。
- **所有权代际 `generation`（fencing token）**：每次成功领取（包括首轮领取和接管后的重新领取）都会让该指令的 `generation` 单调加 1。同一产线挂两台冗余网关时，主网关心跳中断、租约到期后备用网关接管，旧主网关的代际立即过期。
- 每次网关领取生成新的 `leaseId`，网关必须在后续续约和确认中同时携带匹配的 `gatewayId + leaseId + generation`。旧代际的续约和确认即使迟到，也会以 `STALE_GENERATION` 被拒绝，不能推进状态或复活终态。
- 只有设备确认被写入事件存储后，状态才能进入 `DELIVERED` 或 `FAILED`。
- **紧急撤回**：上游可撤回尚未完成（`PENDING`/`CLAIMED`）的指令，进入终态 `CANCELLED` 并记录 `CommandCancelled`。撤回会清除当前租约，失联网关重连后的迟到确认会被终态/旧代际拒绝。
- **替代指令**：上游可对未完成指令提交一条明确取代它的安全指令。旧指令在同一 SQLite 事务中原子地进入 `CANCELLED`（事件 `CommandSuperseded`，指向新指令），新指令以 `CommandSubmitted` 建立并通过 `supersedesCommandId` 反向指回旧指令，形成可查询的双向因果链。替代是幂等的：使用相同的新指令幂等键重试不会产生第二条替代指令。
- **已确认动作不能伪装成撤回成功**：`DELIVERED`/`FAILED`/`TIMED_OUT`/`CANCELLED` 的指令撤回或替代会被拒绝（如 `ALREADY_DELIVERED`/`OLD_ALREADY_DELIVERED`），状态保持不变。
- 终态（`DELIVERED`、`FAILED`、`TIMED_OUT`、`CANCELLED`）不可被迟到 ACK、重复 ACK、新的领取或旧租约/旧代际复活。
- 状态不仅保存最终值，还保存完整事件流；每个事件都有 `causedBy` 和 `causationId`，`CommandClaimed` 记录新旧代际与新旧网关，`CommandSuperseded` 记录被谁取代，`DeviceAckRecorded` 记录实际成功的代际，可还原接管、取代、拒绝和迟到回执前后的因果关系。

### 事件类型

- `CommandSubmitted`（替代产生的新指令会带 `supersedesCommandId`）
- `CommandClaimed`
- `LeaseRenewed`
- `LeaseExpired`
- `DeviceAckRecorded`
- `DeviceNackRecorded`
- `CommandCancelled`
- `CommandSuperseded`
- `CommandTimedOut`

可通过 `GET /v1/upstream/commands/{commandId}/events` 查看单条指令事件流，通过 `GET /v1/admin/events` 查看全局事件。

## HTTP API

### 上游提交指令

```http
POST /v1/upstream/commands
Idempotency-Key: biz-order-123
Content-Type: application/json

{
  "deviceId": "line-1",
  "payload": { "type": "SWITCH_RECIPE", "params": { "recipe": "A" } }
}
```

首次返回 `202 Accepted`，重复幂等键返回 `200 OK` 且响应中 `duplicate: true`，`commandId` 保持不变。

### 查询进度

```http
GET /v1/upstream/commands/{commandId}
GET /v1/upstream/commands/{commandId}/events
GET /v1/upstream/commands?idempotencyKey=biz-order-123
```

命令视图包含 `supersededByCommandId`（本命令被哪条新指令取代）和 `supersedesCommandId`（本指令取代了哪条旧指令），可沿这两个字段串起取代因果链。

### 紧急撤回

```http
POST /v1/upstream/commands/{commandId}/cancel
Content-Type: application/json

{ "reason": "OPERATOR_ABORT" }
```

只能撤回 `PENDING`/`CLAIMED` 状态的指令。对已 `DELIVERED` 的指令返回 `409 ALREADY_DELIVERED`，不会把已执行动作伪装成撤回成功。

### 替代指令

```http
POST /v1/upstream/commands/{oldCommandId}/replace
Idempotency-Key: safe-order-456
Content-Type: application/json

{ "payload": { "type": "SAFE_STOP", "params": {} }, "reason": "UNSAFE_RECIPE" }
```

原子完成两件事：旧指令进入 `CANCELLED`（`CommandSuperseded` 指向新指令），新指令进入 `PENDING` 并反向指回旧指令。对已交付的旧指令返回 `409 OLD_ALREADY_DELIVERED`。使用相同的新指令幂等键重试是幂等的，返回同一条替代指令。

### 网关领取任务

```http
POST /v1/gateway/claim
X-Gateway-Id: gw-1
Content-Type: application/json

{ "leaseDurationMs": 30000 }
```

返回 `200` 时包含任务和租约：

```json
{
  "command": {
    "commandId": "...",
    "payload": { "type": "CALIBRATE" },
    "attempt": 1,
    "generation": 1
  },
  "lease": { "leaseId": "...", "generation": 1, "expiresAt": 1780000000000 }
}
```

没有可领取任务时返回 `204`。后续续约和确认必须原样回传 `leaseId` 和 `generation`。

### 网关续约

```http
POST /v1/gateway/commands/{commandId}/renew
X-Gateway-Id: gw-1
X-Lease-Id: <leaseId>
X-Generation: 1
```

只有当前匹配、未过期且代际一致的租约可续约。主备接管后，旧主网关用旧代际续约将返回 `STALE_GENERATION`。

### 网关回传设备确认

```http
POST /v1/gateway/commands/{commandId}/ack
X-Gateway-Id: gw-1
X-Lease-Id: <leaseId>
X-Generation: 1
Content-Type: application/json

{ "success": true, "ackCode": "EXECUTED", "ackPayload": { "result": "ok" } }
```

确认必须携带领取时拿到的 `generation`。备用网关在租约到期后接管会生成更大的代际；旧主网关分区恢复后用旧代际发送的迟到确认会被以 `STALE_GENERATION` 拒绝，不能把状态推进为成功。`generation` 也可放在请求体中。`success: false` 会记录 `DeviceNackRecorded` 并进入 `FAILED`。

### 运维与测试辅助

- `POST /v1/admin/sweep`：立即扫描过期租约和超时任务。
- `GET /v1/admin/events?limit=200`：查看事件审计流。
- `POST /v1/admin/fault/crash-after-commit`：仅当 `FAULT_INJECTION=true` 时启用，让下一次写入在 SQLite 事务提交后、HTTP 响应前崩溃。

## 配置

通过环境变量配置：

| 变量                | 默认值                    | 说明                                     |
| ------------------- | ------------------------- | ---------------------------------------- |
| `PORT`              | `8080`                    | HTTP 端口                                |
| `HOST`              | `127.0.0.1`               | 监听地址                                 |
| `EDGE_DB_PATH`      | `./data/edge-commands.db` | SQLite 文件路径                          |
| `LEASE_DURATION_MS` | `30000`                   | 默认租约时长                             |
| `MAX_ATTEMPTS`      | `3`                       | 最大投递尝试次数；0 表示不限制           |
| `MAX_AGE_MS`        | 未设置                    | 指令最大存活时间，超过后进入 `TIMED_OUT` |
| `SWEEP_INTERVAL_MS` | `5000`                    | 后台租约/超时扫描间隔                    |
| `CLAIM_BATCH_SIZE`  | `20`                      | 单次领取扫描候选数                       |
| `FAULT_INJECTION`   | `false`                   | 是否启用崩溃注入端点                     |

## 模拟器能复现的故障

内置场景位于 [tools/simulator/scenarios.ts](file:///e:/newGsb/questions/GSB-003/Tony/tools/simulator/scenarios.ts)：

- `happy`：提交、领取、确认、成功。
- `duplicate-submit`：同一业务幂等键重复提交，返回同一 `commandId`。
- `lease-retry`：网关领取后失联，租约过期后另一网关重新领取，同一 `commandId` 的 `attempt` 增加，旧租约确认被拒绝。
- `failover`：同产线双网关，主网关持有代际 1 时备用网关无法接管；主网关租约到期后备用网关以代际 2 接管，旧主网关的迟到续约与确认都被 `STALE_GENERATION` 隔离，最终只有代际 2 的确认被持久化为成功。
- `duplicate-ack`：设备重复确认，第二次被拒绝。
- `wrong-lease`：错误或乱序的 `leaseId` 无法续约或确认。
- `nack`：设备否定确认进入 `FAILED`。
- `cancel`：撤回未完成指令进入 `CANCELLED`。
- `cancel-delivered-rejected`：已 `DELIVERED` 的动作不能被撤回，拒绝伪装成撤回成功。
- `replace`：用安全指令原子取代未完成指令，双向因果链接、新指令可被领取确认，替代重试幂等。
- `three-round`：第 1 轮主网关代际 1、第 2 轮备用网关代际 2 接管并隔离第 1 轮迟到确认、第 3 轮上游用安全指令取代并隔离第 2 轮迟到确认，最终新稳定身份交付，旧指令保留完整因果链。

端到端验收还会真实启动多个服务实例：一个在提交事件持久化后、响应前崩溃并重启复用同一数据库；一个在主网关领取代际 1 后重启进程，验证代际随 SQLite 持久化保留；以及一个在替代事务提交后、响应前崩溃并重启，验证旧指令保持 `CANCELLED`、新指令及其双向链接存活、旧代际继续被隔离、替代重试幂等。

## 交付保证

1. **业务请求幂等**：同一业务幂等键在服务崩溃、网络重试、HTTP 超时后重复提交，最终映射到同一个 `commandId`。
2. **稳定执行身份**：重试、主备接管和重新领取不创建新指令，只在同一 `commandId` 下增加 `attempt` 并生成新的租约/代际事件。
3. **代际隔离（fencing）**：每次接管使 `generation` 单调递增，旧网关的续约与确认因 `STALE_GENERATION` 被拒绝；进程重启后代际从 SQLite 恢复，旧代际不会因迟到消息复活指令。
4. **确认即落盘**：只有携带当前代际的 `DeviceAckRecorded` 或 `DeviceNackRecorded` 已进入 SQLite 事务，状态才会被标记为成功或失败。
5. **撤回/取代不伪造成功**：撤回和替代只能作用于未完成指令；已被设备确认执行的动作会被明确拒绝，状态保持 `DELIVERED`，不会被伪装成撤回成功。
6. **取代原子且可追溯**：旧指令的 `CommandSuperseded` 与新指令的 `CommandSubmitted` 在同一事务写入，双向链接构成可查询因果链；替代重试幂等。
7. **终态不可复活**：成功、失败、超时、撤回后的迟到/重复/错误租约或旧代际消息都会被拒绝。
8. **因果可追溯**：每次投递、续约、过期、接管、取代、重试、确认和超时都作为事件保存，`CommandClaimed` 记录新旧代际与网关，`CommandSuperseded` 记录取代关系，包含来源和因果关联。
9. **本地可运行**：只需 Node.js 20、npm install 和 SQLite 文件，不依赖外部服务。

## 无法做到的语义和恢复边界

1. **无法证明设备物理动作只发生一次**。系统保证服务端只会把一次匹配租约的设备确认记录为成功；如果网关在收到服务端 ACK 响应前重复向物理设备发送指令，设备侧仍可能重复执行。设备需要具备自身幂等能力，或通过指令参数携带业务唯一键进行去重。
2. **“已执行但确认丢失”无法自动判定成功**。如果设备已经执行，但网关在回传 ACK 前永久损坏，服务只能在租约过期后重试或最终超时。它不会根据旁证猜测成功。
3. **租约是时间约定，不是分布式强事务锁**。网关时钟漂移、长时间 STW 暂停或进程冻结可能导致租约判断偏差。生产环境应使用 NTP，并将租约时长设置得明显大于网络抖动与处理时间。
4. **SQLite 文件损坏或磁盘丢失不在恢复范围内**。本交付使用本地 SQLite WAL 和 `synchronous=FULL` 保证应用层崩溃一致性，但不替代备份、磁盘 RAID 或灾备。
5. **本地单节点不提供高可用**。如果服务进程所在机器宕机，恢复依赖同一磁盘上的 SQLite 文件。多节点复制和故障切换不是本次交付范围。
6. **否定确认默认终态失败**。`success:false` 会进入 `FAILED`，不会自动重试。如果业务需要区分“设备忙可重试”和“不可恢复失败”，应在确认码和上游策略层扩展。
7. **后台扫描不是实时强超时**。过期状态可在下次领取时原子推进，也可由后台扫描或 `POST /v1/admin/sweep` 推进；扫描间隔内不会立即改变状态。

## npm 入口

- `npm run install:deps`：安装依赖。
- `npm run build`：TypeScript 编译到 `dist/`。
- `npm start`：启动编译后的服务。
- `npm test`：运行领域单元测试。
- `npm run test:e2e`：运行端到端验收（需先 `npm run build`）。
- `npm run acceptance`：编译并运行端到端验收。
- `npm run simulate`：运行脚本化网关/设备模拟器。
- `npm run clean`：清理构建产物和本地数据目录。
