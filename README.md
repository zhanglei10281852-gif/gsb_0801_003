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
PENDING --claim(leaseId, attempt+1)--> CLAIMED
CLAIMED --renew---------------------> CLAIMED
CLAIMED --ack success----------------> DELIVERED (terminal)
CLAIMED --ack failure/nack-----------> FAILED    (terminal)
CLAIMED --lease expiry---------------> PENDING
PENDING --max attempts or max age----> TIMED_OUT (terminal)
```

关键设计：

- `commandId` 在首次接受业务幂等键时生成并持久化，后续重复提交、网关失联重试、重新领取都沿用同一个稳定执行身份。
- 每次网关领取生成新的 `leaseId`，网关必须在后续续约和确认中携带匹配的 `gatewayId + leaseId`。
- 只有设备确认被写入事件存储后，状态才能进入 `DELIVERED` 或 `FAILED`。
- 终态（`DELIVERED`、`FAILED`、`TIMED_OUT`）不可被迟到 ACK、重复 ACK、新的领取或旧租约复活。
- 状态不仅保存最终值，还保存完整事件流；每个事件都有 `causedBy` 和 `causationId`，可解释状态变化因果。

### 事件类型

- `CommandSubmitted`
- `CommandClaimed`
- `LeaseRenewed`
- `LeaseExpired`
- `DeviceAckRecorded`
- `DeviceNackRecorded`
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
  "command": { "commandId": "...", "payload": { "type": "CALIBRATE" }, "attempt": 1 },
  "lease": { "leaseId": "...", "expiresAt": 1780000000000 }
}
```

没有可领取任务时返回 `204`。

### 网关续约

```http
POST /v1/gateway/commands/{commandId}/renew
X-Gateway-Id: gw-1
X-Lease-Id: <leaseId>
```

只有当前匹配且未过期的租约可续约。

### 网关回传设备确认

```http
POST /v1/gateway/commands/{commandId}/ack
X-Gateway-Id: gw-1
X-Lease-Id: <leaseId>
Content-Type: application/json

{ "success": true, "ackCode": "EXECUTED", "ackPayload": { "result": "ok" } }
```

`success: false` 会记录 `DeviceNackRecorded` 并进入 `FAILED`。

### 运维与测试辅助

- `POST /v1/admin/sweep`：立即扫描过期租约和超时任务。
- `GET /v1/admin/events?limit=200`：查看事件审计流。
- `POST /v1/admin/fault/crash-after-commit`：仅当 `FAULT_INJECTION=true` 时启用，让下一次写入在 SQLite 事务提交后、HTTP 响应前崩溃。

## 配置

通过环境变量配置：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PORT` | `8080` | HTTP 端口 |
| `HOST` | `127.0.0.1` | 监听地址 |
| `EDGE_DB_PATH` | `./data/edge-commands.db` | SQLite 文件路径 |
| `LEASE_DURATION_MS` | `30000` | 默认租约时长 |
| `MAX_ATTEMPTS` | `3` | 最大投递尝试次数；0 表示不限制 |
| `MAX_AGE_MS` | 未设置 | 指令最大存活时间，超过后进入 `TIMED_OUT` |
| `SWEEP_INTERVAL_MS` | `5000` | 后台租约/超时扫描间隔 |
| `CLAIM_BATCH_SIZE` | `20` | 单次领取扫描候选数 |
| `FAULT_INJECTION` | `false` | 是否启用崩溃注入端点 |

## 模拟器能复现的故障

内置场景位于 [tools/simulator/scenarios.ts](file:///e:/newGsb/questions/GSB-003/Tony/tools/simulator/scenarios.ts)：

- `happy`：提交、领取、确认、成功。
- `duplicate-submit`：同一业务幂等键重复提交，返回同一 `commandId`。
- `lease-retry`：网关领取后失联，租约过期后另一网关重新领取，同一 `commandId` 的 `attempt` 增加，旧租约确认被拒绝。
- `duplicate-ack`：设备重复确认，第二次被拒绝。
- `wrong-lease`：错误或乱序的 `leaseId` 无法续约或确认。
- `nack`：设备否定确认进入 `FAILED`。

端到端验收还会真实启动两个服务实例：第一个在提交事件持久化后、响应前崩溃；第二个复用同一 SQLite 文件重启，验证业务请求不会生成第二个设备动作。

## 交付保证

1. **业务请求幂等**：同一业务幂等键在服务崩溃、网络重试、HTTP 超时后重复提交，最终映射到同一个 `commandId`。
2. **稳定执行身份**：重试和重新领取不创建新指令，只在同一 `commandId` 下增加 `attempt` 并生成新的租约事件。
3. **确认即落盘**：只有 `DeviceAckRecorded` 或 `DeviceNackRecorded` 已进入 SQLite 事务，状态才会被标记为成功或失败。
4. **终态不可复活**：成功、失败、超时后的迟到/重复/错误租约消息都会被拒绝。
5. **因果可追溯**：每次投递、续约、过期、重试、确认和超时都作为事件保存，包含来源和因果关联。
6. **本地可运行**：只需 Node.js 20、npm install 和 SQLite 文件，不依赖外部服务。

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
