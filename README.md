# 边缘网关设备指令可靠下发服务

面向"工厂产线旁、经常断网的边缘网关"场景:上游调度系统提交设备指令(校准、切换工艺等),
网关在线时领取任务、续约租约、回传设备确认;运维可完整追溯每一次投递、超时与重试。

- 运行时:Node.js ≥ 20 + TypeScript + SQLite(better-sqlite3,WAL)
- 不依赖 Docker / Redis / 任何远程托管服务
- 领域状态机是纯函数,与 HTTP、网关传输、存储适配器完全解耦,故障时序可确定性测试

## 固定入口

```bash
npm install        # 安装依赖
npm test           # 单元测试(状态机) + 崩溃恢复集成测试
npm run build      # 编译到 dist/
npm start          # 启动编译后的服务(dist/src/server.js)
npm run e2e        # 端到端验收:真实启动编译后服务 + 驱动编译后模拟器
npm run simulate   # 对运行中的服务运行内建故障复现场景
npm run simulate -- scenarios/offline-redelivery.json   # 运行自定义场景脚本
```

## 快速体验

```bash
npm install
npm run e2e        # 一条命令看完全部验收(崩溃注入/断网/重复提交/乱序与重复确认)
```

手动对跑(两个终端):

```bash
# 终端 1:用较短的租约参数启动,便于观察失联
npm run build
$env:LEASE_TTL_MS=2000; $env:MAX_ATTEMPTS=3; npm start

# 终端 2:运行模拟器(复现断网、重复提交、迟到/重复/乱序确认)
$env:SIM_SLEEP_MS=3000; npm run simulate
```

## 架构与分层

```
src/domain/    纯领域层:types.ts(领域模型) + machine.ts(状态机纯函数)
               输入=持久化状态+消息+注入的时钟/ID/配置,输出=写集合+因果事件+回复
               不 import 任何 IO;崩溃、超时、迟到消息在测试中靠注入时钟确定性复现
src/storage/   SQLite 适配器:schema + 行映射 + 单事务落库(WAL, synchronous=FULL)
src/service/   应用服务:装载状态 → 调状态机 → 状态+事件原子写入;不持有领域内存状态
src/http/      HTTP 适配器(node:http):解析/校验/序列化,不含领域规则
simulator/     可脚本化网关/设备模拟器(client + 场景运行器 + CLI)
e2e/           端到端验收:spawn 编译后的服务进程与模拟器进程
test/          状态机确定性测试 + 基于真实 SQLite 文件的崩溃恢复测试
```

### 核心概念

| 概念 | 说明 |
|---|---|
| Command | 一条业务指令。`idempotency_key` 唯一;`execution_token` 是面向设备的**稳定执行身份**,创建后终身不变 |
| Attempt | 一次投递(租约)。`attempt_no` 单调递增;`lease_id` 是网关续约/回传确认的凭据 |
| Ack | 设备确认。`ack_id` 唯一用于去重;`applied=false` 的确认也会持久化并记录忽略原因 |
| Event | 因果事件日志(`seq` 全局单调)。每次状态变化都有事件,而不是只保存最终值 |

状态机:`PENDING → DISPATCHED →(确认生效)SUCCEEDED`;`DISPATCHED →(租约过期)PENDING(退避重试)`;重试耗尽 `→ FAILED`。`SUCCEEDED`/`FAILED` 为终态,不可逆。

## 交付保证(能做到的)

1. **提交幂等**:同一 `idempotencyKey` 无论提交多少次(包括"服务持久化后、响应前崩溃"导致的
   客户端盲重试),只产生一条指令、至多一条设备动作链;重复提交返回原指令并记录 `SUBMISSION_DEDUPED`。
2. **至少一次投递(at-least-once delivery)**:网关领走后失联,租约到期(`LEASE_TTL_MS`)后任务
   重新进入队列并按线性退避重派,直到成功或重试耗尽(`MAX_ATTEMPTS`)。
3. **稳定执行身份**:重试、跨网关重新领取,设备看到的 `executionToken` 始终相同;设备侧据此去重,
   同一业务请求不会被设备执行两次(需要设备配合,见下节)。
4. **至多一次成功标记**:只有"已持久化且被应用"的设备确认能把指令标为 `SUCCEEDED`;
   确认与状态迁移、因果事件在**同一个 SQLite 事务**中提交。
5. **终态不可逆**:迟到确认、迟到续约不能复活 `SUCCEEDED`/`FAILED` 指令——消息会被持久化留痕
   (`DEVICE_ACK_IGNORED` + 原因),但状态不变。
6. **语义不依赖巡检时序**:过期判定只取决于时钟。领取/续约/确认路径与后台巡检器使用同一判定函数
   做惰性结算,"巡检器先跑还是迟到消息先到"产生完全一致的结果,消除时序竞态。
7. **可解释的因果追溯**:每次提交去重、投递、续约、过期、重试、确认生效/忽略、终态都有事件,
   含 `causedBy`(触发请求/事件)与全局单调 `seq`,运维可回放完整因果链:
   `GET /v1/commands/:id/events`。

## 无法做到的语义(边界)

1. **恰好一次设备执行(exactly-once execution)不可能**:确认在回传途中丢失时,服务无法区分
   "设备没执行"与"执行了但确认丢了"。本服务选择重发(至少一次),**设备必须以 `executionToken`
   做幂等执行**,否则同一指令可能被设备执行两次。这是两将军问题的理论上限,不是实现缺陷。
2. **成功标记的持久性 = SQLite 文件的持久性**:`synchronous=FULL` 保证事务落盘后才响应,
   但整盘损毁无法恢复;如需更高持久性,需自行做数据库文件级备份/复制。
3. **不保证跨网关的墙钟序**:事件顺序以服务侧单调 `seq` 为准;设备侧时钟不参与判定。
4. **网关身份仅按 `gatewayId` 字符串校验**:本服务未内置鉴权/加密(产线内网前提);如需
   mTLS/令牌,放在反向代理或在此之上加一层,不影响领域语义。
5. **"已执行但未确认"期间服务崩溃**:重启后该尝试按租约过期处理并可能重发——正确性依赖第 1 条
   的设备幂等。

## 恢复边界(崩溃窗口枚举)

| 崩溃窗口 | 重启后行为 |
|---|---|
| 提交:落库前 | 指令不存在;客户端以同幂等键重提即可,无副作用 |
| 提交:落库后、响应前 | 指令已存在;客户端重提同幂等键 → 返回原指令(`deduped=true`),不会生成第二条设备动作 |
| 领取:落库后、响应前 | 网关从未拿到任务;租约到期后自动重派(执行身份不变) |
| 续约/确认:落库前 | 视为未发生;租约到期或网关重发确认即可收敛 |
| 确认:落库后、响应前 | 网关重发同 `ackId` → 幂等重放(`duplicate=true`),不二次生效 |
| 任意时刻进程死亡 | 服务无内存领域状态,重启即读库继续;后台巡检器重新结算过期租约 |

以上窗口中 A(提交崩溃)、失联重派、迟到/重复确认均由 `npm run e2e` 真实注入验证。

## HTTP API 摘要

| 方法/路径 | 说明 |
|---|---|
| `POST /v1/commands` | 提交指令 `{idempotencyKey, action, params}`;201 新建 / 200 去重命中 |
| `GET /v1/commands/:idOrKey` | 查询进度(指令 id 或幂等键) |
| `GET /v1/commands/:id/events` | 追溯该指令的完整因果事件 |
| `GET /v1/events?command_id=` | 全局/按指令事件流 |
| `POST /v1/gateway/claims` | 网关领取任务 `{gatewayId, limit}` → `{tasks:[{commandId, executionToken, attemptNo, leaseId, leaseExpiresAt, action, params}]}` |
| `POST /v1/gateway/renewals` | 续约 `{gatewayId, leaseId}`;过期/失效返回 409+原因 |
| `POST /v1/gateway/acks` | 回传设备确认 `{gatewayId, leaseId, ackId, result}` → `{outcome: applied\|ignored, reason, duplicate}` |
| `GET /healthz` | 健康检查 |

## 配置(环境变量)

| 变量 | 默认 | 说明 |
|---|---|---|
| `PORT` | 8080 | 监听端口 |
| `DB_PATH` | ./dispatch.db | SQLite 文件路径 |
| `LEASE_TTL_MS` | 15000 | 租约时长(网关需在此前续约或确认) |
| `MAX_ATTEMPTS` | 5 | 最大投递次数,耗尽后指令进入 FAILED |
| `RETRY_BACKOFF_MS` | 2000 | 线性退避基数(第 n 次重试等待 n×该值) |
| `REAPER_INTERVAL_MS` | 1000 | 巡检器间隔(仅影响重派及时性,不影响语义) |
| `CRASH_AFTER_SUBMIT_COMMIT` | - | 测试钩子:=1 时首个提交落库后、响应前退出进程(e2e 使用) |

## 模拟器场景脚本

场景是 JSON 步骤数组(`steps`),逐步执行并校验期望,任一失败即非零退出。
支持的 `op`:`log / submit / claim / renew / ack / sleep / waitStatus / assertStatus / assertEvents`。

- **断网/失联**:领取后不再 `renew`/`ack`(配合 `sleep` 超过租约)
- **重复提交**:两个 `submit` 使用相同 `key`,`expectDeduped` + `expectSameCommandAs` 校验
- **乱序/重复确认**:任意顺序多个 `ack`,`expect: applied|ignored|duplicate|rejected`
- **执行身份沿用**:`claim` 的 `expectExecutionTokenOf`

完整字段见 [simulator/scenario.ts](simulator/scenario.ts) 顶部注释;示例见
[scenarios/offline-redelivery.json](scenarios/offline-redelivery.json) 与 `npm run simulate` 的内建场景。

## 测试与验收

- `npm test`:14 个用例。状态机测试注入假时钟/ID,确定性复现超时、迟到、乱序、重复;
  恢复测试用真实 SQLite 文件,以"关库→同库重启服务"模拟进程崩溃。
- `npm run e2e`:编译后真实启动服务子进程,分三段验收——
  A 崩溃注入(落库后响应前退出,exit 42)→ 重启幂等重提;
  B spawn 编译后的模拟器跑完整故障场景(17 项断言);
  C 再次重启验证状态恢复、幂等保持、事件序号单调。
