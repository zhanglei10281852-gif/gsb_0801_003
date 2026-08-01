# Edge Command Dispatch

面向经常断网的边缘网关场景的可靠设备指令下发服务。上游通过业务幂等键提交校准、切换工艺等指令并查询进度；边缘网关领取待发任务、续约租约并回传设备确认；运维可通过因果事件日志追溯每次投递、超时和重试。

- **运行时**：Node.js ≥ 20、TypeScript、SQLite（`better-sqlite3`）
- **不依赖**：Docker、Redis、任何远程托管服务
- **架构原则**：领域状态机与 HTTP、网关传输、存储适配器完全解耦，故障时序可被确定性单元测试

---

## 固定入口

| 用途                                              | 命令                                               |
| ------------------------------------------------- | -------------------------------------------------- |
| 安装依赖                                          | `npm install`                                      |
| 运行单元测试（纯领域/应用层，内存适配器，确定性） | `npm test`                                         |
| 编译 TypeScript 到 `dist/`                        | `npm run build`                                    |
| 启动服务（默认 `http://localhost:3000`）          | `npm start`                                        |
| 端到端验收（编译后真实启动服务 + 驱动模拟器）     | `npm run e2e`                                      |
| 独立模拟器 CLI（连接已运行的服务）                | `npm run simulator -- --url http://localhost:3000` |

环境变量：`PORT`（默认 3000）、`DB_PATH`（默认 `./data/commands.db`）。

---

## 架构分层

```
src/
├── domain/                 # 纯领域层，无 I/O 依赖
│   ├── types.ts            # 命令、事件、输入输出类型
│   ├── errors.ts           # 领域错误
│   └── stateMachine.ts     # 纯函数状态机（submit/claim/renew/confirm/expire）
├── application/
│   ├── ports.ts            # 仓储/事件存储/UoW 接口
│   └── commandService.ts   # 用例编排（事务脚本），依赖端口而非具体实现
├── infrastructure/
│   ├── sqlite/             # SQLite 适配器（生产持久化）
│   ├── memory/             # 内存适配器（确定性测试）
│   └── systemPrimitives.ts # SystemClock / UuidGenerator
├── http/
│   ├── routes.ts           # Express 路由（上游 / 网关 / 运维）
│   └── server.ts           # 服务组装 + 后台租约扫描器
├── simulator/
│   ├── apiClient.ts        # HTTP 客户端
│   ├── deviceSimulator.ts  # 设备模拟器（可重复/延迟/丢弃确认）
│   ├── gatewaySimulator.ts # 边缘网关模拟器（可断网/续约/转发确认）
│   └── cli.ts              # 独立模拟器入口
├── e2e/
│   ├── serverHarness.ts    # 启动编译后子进程服务
│   └── acceptance.ts       # 9 个端到端验收场景
└── index.ts                # 主入口
```

领域状态机是纯函数：输入当前命令 + 动作 + 可注入的 `Clock`/`IdGenerator`，输出新状态和事件列表。它不接触数据库、HTTP 或时钟的真实实现，因此所有故障时序都可以在单元测试中用 `FakeClock` 确定性复现。

---

## 状态机

```
PENDING ──claim──▶ CLAIMED(leaseId, attempt)
  ▲                  │
  │                  ├── renew ──▶ CLAIMED(延长租约)
  │                  ├── reportDelivery ──▶ CLAIMED(仅记录事件)
  │                  ├── confirm(有效租约) ──▶ SUCCEEDED  (终态)
  │                  └── leaseExpired ──┐
  │                                     ├─ 尝试次数未到上限 ──▶ PENDING
  │                                     └─ 尝试次数已到上限 ──▶ FAILED (终态)
  └── claim(收割过期租约) ──▶ CLAIMED(新 leaseId, attempt++)
```

关键规则：

- **稳定执行身份**：重试和重新领取始终沿用同一个 `commandId`，只递增 `attempt` 并换发新的 `leaseId`。
- **只有持久化的设备确认能成功**：`confirm` 在单个 SQLite 事务中同时写入 `DeviceConfirmed` + `CommandSucceeded` 事件并更新状态；事务未提交则状态不变。
- **过期任务不会被迟到消息复活**：确认必须携带当前有效的 `leaseId` 且租约未过期；旧租约、过期租约、已成功/已失败命令的迟到确认一律产生 `StaleMessageRejected` 事件但不改变最终状态。
- **因果事件日志**：每次状态变化都产生带 `causedBy`、`leaseId`、`attempt`、`gatewayId`、`timestamp` 的事件，而非只保存最终值。

### 事件类型

`CommandSubmitted` · `CommandClaimed` · `CommandReclaimed` · `LeaseRenewed` · `DeliveryReported` · `DeviceConfirmed` · `CommandSucceeded` · `LeaseExpired` · `CommandFailed` · `LeaseOperationRejected` · `StaleMessageRejected`

### 所有权代际（HA fencing）

每条命令在被领取时获得一个代际标识：`attempt`（代际号，从 1 递增）+ `leaseId`（该代际的唯一 fencing token）。主备网关接管时：

- 主网关心跳中断 → 租约到期 → 备用网关领取，代际号 +1，换发新 `leaseId`
- **稳定执行身份不变**：`commandId` 始终沿用首轮提交时生成的值，重试不会产生新的设备动作身份
- 旧代际的续约、投递上报、确认一律被拒：
  - 旧 `leaseId` 的续约/上报 → 409 + `LeaseOperationRejected` 事件（记录 `currentGeneration`、`rejectedLeaseId`、`rejectedGatewayId`）
  - 旧 `leaseId` 的确认 → 200 但 `accepted=false` + `StaleMessageRejected` 事件，状态不推进
  - 两网关并发争抢同一过期租约 → SQLite `BEGIN IMMEDIATE` 串行化，赢家获得新代际，输家得到 409 + 审计事件
- `CommandReclaimed` 事件携带完整代际转换信息：`oldGatewayId`、`oldGeneration`、`newGeneration`、`oldLeaseId`、`newLeaseId`

---

## HTTP API

所有路径前缀 `/api/v1`。

### 上游（调度系统）

| 方法 | 路径                    | 说明                                                                                                                          |
| ---- | ----------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| POST | `/commands`             | 提交指令。必须带 `Idempotency-Key` 请求头；body 含 `payload`、可选 `maxAttempts`。首次返回 201，重复幂等键返回 200 + 同一命令 |
| GET  | `/commands/:id`         | 按 commandId 查询状态                                                                                                         |
| GET  | `/commands/by-key/:key` | 按幂等键查询状态                                                                                                              |
| GET  | `/commands/:id/events`  | 查询该命令的完整因果事件链                                                                                                    |
| GET  | `/commands`             | 列出近期命令                                                                                                                  |

### 网关

| 方法 | 路径                       | 说明                                                                                                                 |
| ---- | -------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| POST | `/gateway/claim`           | 领取任务，body：`gatewayId`、`leaseDurationMs`、可选 `deviceId`。返回 200+任务 或 204（无任务）                      |
| POST | `/gateway/renew`           | 续约，body：`commandId`、`leaseId`、`gatewayId`、`leaseDurationMs`                                                   |
| POST | `/gateway/report-delivery` | 上报已投递给设备，body：`commandId`、`leaseId`、`gatewayId`                                                          |
| POST | `/gateway/confirm`         | 回传设备确认，body：`commandId`、`leaseId`、`gatewayId`、`confirmationCode`、`deviceTimestamp`。返回 `accepted` 字段 |

### 运维

| 方法 | 路径                  | 说明                                            |
| ---- | --------------------- | ----------------------------------------------- |
| GET  | `/events`             | 全局事件日志（最多 500 条）                     |
| POST | `/admin/scan-expired` | 手动触发一次过期租约扫描（后台每 2 秒自动扫描） |
| GET  | `/health`             | 健康检查                                        |

---

## 交付保证

以下保证由自动化测试（35 个单元测试 + 13 个端到端场景）覆盖：

1. **幂等提交**：相同 `Idempotency-Key` 的并发或重复提交只生成一个 `commandId`，即"持久化后、返回响应前崩溃"的场景——上游重试拿到的是同一条命令，不会变成两个设备动作。
2. **稳定执行身份**：网关失联后租约过期，重新领取沿用原 `commandId`，只产生新的 `leaseId` 和递增的代际号 `attempt`。主备接管、进程重启均不改变执行身份。
3. **设备确认持久化才标成功**：`DeviceConfirmed` 事件和 `SUCCEEDED` 状态在同一事务中落盘；未提交的确认在崩溃后不存在。
4. **迟到确认不能复活过期任务**：
   - 旧 `leaseId` 的确认（网关被重新领取后）→ 拒绝；
   - 租约已过期的确认 → 拒绝；
   - 已 `SUCCEEDED` 后的重复确认 → 幂等返回成功但记录 `StaleMessageRejected`；
   - 已 `FAILED` 后的确认 → 拒绝。
5. **最大尝试次数**：超过 `maxAttempts` 后命令进入 `FAILED` 终态，不再被领取。
6. **因果可追溯**：每次投递、续约、过期、重试、拒绝都有带因果说明（`causedBy`）的事件记录，事件按插入顺序排列。
7. **网关断网恢复**：网关离线期间停止续约，租约到期后任务可被其他网关领取；离线网关缓冲的确认在恢复后若租约已失效会被拒绝。
8. **双网关 HA fencing**：主备网关场景下，备用网关在主网关租约过期后接管并获得新代际；旧代际的续约、投递上报、确认全部被拒并写入 `LeaseOperationRejected` 审计事件；并发争抢由 SQLite 写事务串行化，只有一个网关赢得代际。
9. **进程重启恢复**：服务进程重启后，SQLite 中处于 `CLAIMED` 状态的任务及其租约完整保留；租约到期后备用网关可接管，`commandId` 和代际号不变。

---

## 无法做到的语义（明确边界）

本系统在没有设备端配合和分布式共识的前提下，**不能**提供以下保证：

1. **精确一次（exactly-once）设备执行**：系统提供的是**至少一次投递 + 幂等命令身份**。在网关投递到设备后、收到确认前失联，无法判断设备是否已实际执行。重新领取后会再次投递同一 `commandId` 的指令。要做到设备侧精确一次，**设备本身必须基于 `commandId` 做幂等去重**。
2. **网络分区期间的设备侧写入拦截**：本系统用租约时间 + `leaseId` 代次（epoch）在服务端 fencing 旧网关的状态推进，但不向设备侧下发 fencing token。如果旧网关在网络分区期间仍能直连设备并写入指令，服务端无法阻止设备执行——设备可能收到重复指令。要防止设备侧重复执行，**设备必须基于 `commandId` 幂等去重**。
3. **自动主备选举**：本系统不内置网关 leader election。备用网关在主网关租约过期后通过领取任务自然接管，但"主网关是否真的已死"由租约超时判定，不依赖外部共识。极端情况下（主网关只是慢而非死）可能出现双网关都认为自己活跃的 split-brain 窗口，此时服务端 fencing 保证只有一个代际能推进状态，但设备侧可能收到双份投递。
4. **拜占庭/恶意网关**：系统假设网关如实转发设备确认。恶意网关可以伪造确认码。`confirmationCode` 只做因果记录，不做密码学验证。
5. **时钟同步**：租约基于服务端墙上时钟（`Date.now()`）。网关和设备的时间戳被记录但不参与过期判定。服务端时钟回拨可能导致租约异常延长或缩短。
6. **跨进程并发领取的线性一致读**：SQLite 单写入者串行化写事务，但读接口可能读到稍旧的状态投影（最终一致于事件日志）。状态和事件在同一事务中写入，因此不会出现"有事件无状态"或反之。

---

## 恢复边界

| 故障点                                | 行为                                                                                                                 |
| ------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| 服务在**事务提交前**崩溃              | 无任何写入，上游重试产生新命令（正常路径）                                                                           |
| 服务在**事务提交后、HTTP 响应前**崩溃 | 数据已持久化；上游用相同幂等键重试，返回同一命令，状态为提交时的状态                                                 |
| 网关**领取后失联**                    | 租约到期后后台扫描器或下次领取将任务重置为 `PENDING`（未超尝试次数）或 `FAILED`（已超），可被重新领取                |
| 网关**投递后、确认前失联**            | 设备是否执行未知；任务重新领取后会再次投递同一 `commandId`，依赖设备幂等                                             |
| 网关**确认在传输中丢失**              | 设备实际已执行但服务端未收到确认；租约过期后重新投递，设备应幂等处理                                                 |
| 服务进程重启                          | SQLite（WAL + `synchronous=FULL`）保证已提交事务不丢；`CLAIMED` 状态及租约完整保留，后台扫描器启动后自动回收过期租约 |
| 服务进程在**租约有效期内重启**        | 重启后任务仍为 `CLAIMED`，持有原 `leaseId` 和代际号；旧网关若在租约到期前沿约仍被接受，租约到期后备用网关可接管      |
| 主网关**租约到期、备用网关接管**      | 代际号 +1，新 `leaseId` 下发；旧代际的续约/上报/确认全部被拒并记录 `LeaseOperationRejected`，`commandId` 不变        |
| SQLite 文件损坏                       | 不在恢复范围内；建议通过文件系统快照/备份保护                                                                        |

---

## 模拟器

模拟器包含两部分，均可脚本化驱动：

- **`DeviceSimulator`**：可配置 `confirmDelayMs`（延迟确认）、`duplicateConfirm`（重复确认）、`dropConfirm`（丢弃确认）。
- **`GatewaySimulator`**：轮询领取、续约、上报投递、转发确认；可 `goOffline()`/`goOnline()` 模拟断网，可注入重复/延迟/迟到确认。`claim` 自动按设备 ID 过滤。

端到端验收（`npm run e2e`）会真实启动编译后的服务子进程，并驱动以下场景：

1. 正常投递全链路与因果事件链
2. 重复幂等键提交（崩溃后恢复）
3. 网关失联 → 租约过期 → 稳定身份重新领取 → 旧网关确认被拒
4. 设备重复确认（幂等，唯一成功）
5. 租约过期后、重新领取前的迟到确认被拒
6. 最大尝试次数耗尽 → FAILED
7. 并发重复提交 → 仅一次设备投递
8. 乱序/旧代次确认无法覆盖成功状态
9. **双网关 HA 接管**：主网关失联 → 备用网关接管 → 旧代际续约/上报/确认全部被拒并审计 → 稳定 commandId
10. **Split-brain**：旧网关在接管后继续写入，全部 fencing，无僵尸成功
11. **接管审计因果链**：事件链还原代际转换的完整因果关系
12. **进程重启恢复**：租约期内重启服务，状态从 SQLite 恢复，到期后备用网关接管
13. 运维审计事件完整性

独立运行模拟器：

```bash
# 终端 1：启动服务
npm start

# 终端 2：启动一个会周期性断网的网关
npm run simulator -- --gateway gw-edge-1 --scenario flaky

# 终端 2（另一种）：网关会重复转发确认
npm run simulator -- --gateway gw-edge-2 --scenario dup-ack
```

可用 `--url`、`--lease-ms`、`--poll-ms` 调整连接和时序参数。

---

## 开发与测试

```bash
npm install        # 安装依赖
npm test           # 单元测试（内存适配器，毫秒级，确定性）
npm run build      # 编译
npm run e2e        # 编译 + 启动真实服务 + 9 个验收场景
```

单元测试使用 `FakeClock`（手动推进时间）和顺序 ID 生成器，所有超时、重试、过期时序都是确定性的，不依赖真实等待。端到端测试使用真实时钟和真实 HTTP 端口，但通过显式状态轮询和足够的租约余量保证稳定。
