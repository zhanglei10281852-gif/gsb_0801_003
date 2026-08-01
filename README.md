# 边缘网关设备指令可靠下发服务

面向"工厂产线旁、经常断网的边缘网关"场景:上游调度系统提交设备指令(校准、切换工艺等),
网关在线时领取任务、续约租约、回传设备确认;运维可完整追溯每一次投递、超时与重试。

**冗余网关**:同一产线可挂主备两台网关。每条产线有一份**带租约的所有权(ownership)+
单调递增代际(generation,即 fencing token)**:网关心跳获取/续约;主网关心跳中断后,
备用网关在所有权租约到期时接管(代际 +1)。网络分区恢复后,旧代际网关的一切操作
(领取/续约/确认)都会被 fencing 拒绝且不得推进状态——但全部留痕可审计。

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
npm run e2e        # 一条命令看完全部验收(崩溃注入/双网关争抢/接管/fencing/重复确认)
```

手动对跑(两个终端):

```bash
# 终端 1:用较短的租约参数启动,便于观察失联与接管
npm run build
$env:LEASE_TTL_MS=2000; $env:MAX_ATTEMPTS=3; $env:OWNERSHIP_LEASE_TTL_MS=6000; npm start

# 终端 2:运行模拟器(复现争抢、断网、接管、旧代际 fencing、重复确认)
$env:SIM_SLEEP_MS=3000; $env:SIM_TAKEOVER_SLEEP_MS=7000; npm run simulate
```

## 架构与分层

```
src/domain/    纯领域层:types.ts(领域模型) + machine.ts(状态机纯函数)
               输入=持久化状态+消息+注入的时钟/ID/配置,输出=写集合+因果事件+回复
               不 import 任何 IO;崩溃、超时、网络分区、迟到消息靠注入时钟确定性复现
src/storage/   SQLite 适配器:schema + 轻量迁移 + 行映射 + 单事务落库(WAL, synchronous=FULL)
src/service/   应用服务:装载状态 → 调状态机 → 状态+事件原子写入;不持有领域内存状态
src/http/      HTTP 适配器(node:http):解析/校验/序列化,不含领域规则
simulator/     可脚本化网关/设备模拟器(client + 场景运行器 + CLI)
e2e/           端到端验收:spawn 编译后的服务进程与模拟器进程
test/          状态机确定性测试 + 基于真实 SQLite 文件的崩溃恢复测试
```

### 核心概念

| 概念 | 说明 |
|---|---|
| Command | 一条业务指令,挂在某条产线(`lineId`)。`idempotency_key` 唯一;`execution_token` 是面向设备的**稳定执行身份**,创建后终身不变 |
| Ownership | 产线所有权(带租约的 fencing token):`ownerGatewayId` + 单调递增 `generation` + `leaseExpiresAt`。心跳获取/续约,到期后任何网关可接管(代际 +1) |
| Attempt | 一次投递(租约)。`attemptNo` 单调递增并记录颁发时的 `generation`;`leaseId` 是网关续约/回传确认的凭据 |
| Ack | 设备确认。`ackId` 唯一用于去重;`applied=false` 的确认(迟到/乱序/被 fencing)也会持久化并记录原因 |
| Event | 因果事件日志(`seq` 全局单调)。分指令维度与产线维度(所有权/争抢/fencing),可还原接管前后的完整因果 |

状态机:`PENDING → DISPATCHED →(确认生效)SUCCEEDED`;`DISPATCHED →(租约过期/接管中止)PENDING(重试)`;重试耗尽 `→ FAILED`。`SUCCEEDED`/`FAILED` 为终态,不可逆。

### 所有权与 fencing 规则

1. 网关心跳 `POST /v1/gateway/heartbeats`:无所有权则获取(代际 1);本人持有未过期则续约
   (代际不变);**租约过期则接管(代际 +1)**——无论原属主是别人还是自己(失联后重新获取同样升代际,
   因为其在途投递已不可信);他人持有未过期则拒绝(争抢留痕 `OWNERSHIP_HEARTBEAT_REJECTED`)。
2. 领取/续约/确认必须携带调用方所持代际。fencing 判定:
   非属主(`not_owner`)、属主但代际已旧(`stale_generation`)、所有权租约已过期
   (`ownership_lease_expired`)、产线尚无属主(`no_ownership`)——一律拒绝且不改变领域状态。
3. 接管发生时,前属主在该产线上的**在途投递被立即中止**(`ATTEMPT_ABORTED`,不退避),
   指令立刻可供新属主重投——`executionToken` 沿用首轮身份,设备侧幂等去重。
4. 被 fencing 的确认**持久化留痕但不生效**(`ACK_FENCED` + ack 行 `applied=false`):
   审计能看到"设备其实确认了,但经由旧代际送达",状态推进只认当前代际。
5. 所有权与代际持久化在 SQLite,**跨进程重启保持**:重启后旧代际依旧被 fencing。

## 交付保证(能做到的)

1. **提交幂等**:同一 `idempotencyKey` 无论提交多少次(包括"服务持久化后、响应前崩溃"导致的
   客户端盲重试),只产生一条指令、至多一条设备动作链;重复提交返回原指令并记录 `SUBMISSION_DEDUPED`。
2. **至少一次投递(at-least-once delivery)**:网关领走后失联,投递租约到期(`LEASE_TTL_MS`)后
   重派(线性退避);主网关心跳中断,备用在所有权租约到期(`OWNERSHIP_LEASE_TTL_MS`)后接管并立即
   重投在途任务,直到成功或重试耗尽(`MAX_ATTEMPTS`)。
3. **单一写入者(fencing)**:任意时刻每条产线只有当前代际的属主网关能推进状态;
   旧代际的领取返回 403、续约返回 409、确认持久化但不生效。
4. **稳定执行身份**:重试、跨网关、跨代际重投,设备看到的 `executionToken` 始终相同;
   设备侧据此去重,同一业务请求不会被设备执行两次(需要设备配合,见下节)。
5. **至多一次成功标记**:只有"已持久化且被应用"的设备确认能把指令标为 `SUCCEEDED`;
   确认与状态迁移、因果事件在**同一个 SQLite 事务**中提交。
6. **终态不可逆**:迟到确认、迟到续约、旧代际消息不能复活 `SUCCEEDED`/`FAILED` 指令。
7. **语义不依赖巡检时序**:过期判定只取决于时钟。领取/续约/确认路径与后台巡检器使用同一判定函数
   做惰性结算,"巡检器先跑还是迟到消息先到"产生完全一致的结果,消除时序竞态。
8. **可解释的因果追溯**:每次提交去重、所有权获取/续约/接管/争抢、投递、中止、过期、重试、
   fencing 拒绝、确认生效/忽略都有事件,含 `causedBy` 与全局单调 `seq`;
   指令视角 `GET /v1/commands/:id/events`,产线视角 `GET /v1/lines/:line/events` 可还原一次接管前后因果。

## 无法做到的语义(边界)

1. **恰好一次设备执行(exactly-once execution)不可能**:确认在回传途中丢失时,服务无法区分
   "设备没执行"与"执行了但确认丢了";接管/重试期间同一指令可能被投递多次。本服务选择重发
   (至少一次),**设备必须以 `executionToken` 做幂等执行**。这是两将军问题的理论上限,不是实现缺陷。
2. **fencing 不能阻止旧网关物理上接触设备**:如果旧网关被分区但仍有到设备的链路,它可能已经把
   指令发给设备——服务侧能做的是拒绝其状态推进,并靠设备按 `executionToken` 幂等吸收重复执行。
   真正的物理隔离需要设备侧也校验代际(本协议已把 `generation` 随任务下发,设备可选做二次 fencing)。
3. **被 fencing 的确认不代表指令已执行**:它只证明"某网关声称设备确认了"。此类确认留痕但不生效,
   指令仍会由当前属主重投并等待当前代际的确认。
4. **成功标记的持久性 = SQLite 文件的持久性**:`synchronous=FULL` 保证事务落盘后才响应,
   但整盘损毁无法恢复;如需更高持久性,需自行做数据库文件级备份/复制。
5. **不保证跨网关的墙钟序**:事件顺序以服务侧单调 `seq` 为准;设备侧时钟不参与判定。
6. **网关身份仅按 `gatewayId` 字符串校验**:未内置鉴权/加密(产线内网前提);如需 mTLS/令牌,
   放在反向代理或在此之上加一层,不影响领域语义。

## 恢复边界(崩溃窗口枚举)

| 崩溃窗口 | 重启后行为 |
|---|---|
| 提交:落库前 | 指令不存在;客户端以同幂等键重提即可,无副作用 |
| 提交:落库后、响应前 | 指令已存在;客户端重提同幂等键 → 返回原指令(`deduped=true`) |
| 心跳/接管:落库后、响应前 | 所有权与代际已生效;网关重新心跳,已持有者续约、未持有者看到当前属主与代际 |
| 领取:落库后、响应前 | 网关从未拿到任务;投递租约到期后自动重派(执行身份不变) |
| 续约/确认:落库前 | 视为未发生;租约到期或网关重发确认即可收敛 |
| 确认:落库后、响应前 | 网关重发同 `ackId` → 幂等重放(`duplicate=true`),不二次生效 |
| 接管后立刻宕机 | 代际 +1 已持久化;旧代际依旧被 fencing;被中止的在途任务已被新属主领取或留在队列 |
| 任意时刻进程死亡 | 服务无内存领域状态(含所有权),重启即读库继续;巡检器重新结算过期租约 |

以上窗口中 A(提交崩溃)、D(双网关争抢/接管/fencing/重启)均由 `npm run e2e` 真实注入验证。

## HTTP API 摘要

| 方法/路径 | 说明 |
|---|---|
| `POST /v1/commands` | 提交指令 `{idempotencyKey, action, params, lineId?}`;201 新建 / 200 去重命中 |
| `GET /v1/commands/:idOrKey` | 查询进度(指令 id 或幂等键) |
| `GET /v1/commands/:id/events` | 追溯该指令的完整因果事件 |
| `POST /v1/gateway/heartbeats` | 网关心跳 `{gatewayId, lineId}` → `{acquired, generation, owner, leaseExpiresAt}`;过期即接管(代际 +1) |
| `POST /v1/gateway/claims` | 领取任务 `{gatewayId, lineId, generation, limit}`;旧代际 → 403 `fenced` |
| `POST /v1/gateway/renewals` | 续约 `{gatewayId, leaseId, generation}`;过期/被 fencing → 409+原因 |
| `POST /v1/gateway/acks` | 回传设备确认 `{gatewayId, leaseId, generation, ackId, result}` → `{outcome: applied\|ignored, reason, duplicate}`;旧代际确认持久化但不生效 |
| `GET /v1/lines/:line/ownership` | 产线当前所有权(属主/代际/到期时间) |
| `GET /v1/lines/:line/events` | 产线维度事件流(还原接管前后因果) |
| `GET /v1/events?command_id=&line_id=` | 全局/按指令/按产线追溯事件 |
| `GET /healthz` | 健康检查 |

领取返回的任务:`{commandId, lineId, executionToken, attemptNo, generation, leaseId, leaseExpiresAt, action, params}`。

## 配置(环境变量)

| 变量 | 默认 | 说明 |
|---|---|---|
| `PORT` | 8080 | 监听端口 |
| `DB_PATH` | ./dispatch.db | SQLite 文件路径 |
| `LEASE_TTL_MS` | 15000 | 投递租约时长(网关需在此前续约或确认) |
| `OWNERSHIP_LEASE_TTL_MS` | 10000 | 产线所有权租约;主网关心跳中断后备用需等待此时长接管 |
| `MAX_ATTEMPTS` | 5 | 最大投递次数,耗尽后指令进入 FAILED |
| `RETRY_BACKOFF_MS` | 2000 | 线性退避基数(第 n 次重试等待 n×该值;接管中止不退避) |
| `REAPER_INTERVAL_MS` | 1000 | 巡检器间隔(仅影响重派及时性,不影响语义) |
| `CRASH_AFTER_SUBMIT_COMMIT` | - | 测试钩子:=1 时首个提交落库后、响应前退出进程(e2e 使用) |

## 模拟器场景脚本

场景是 JSON 步骤数组(`steps`),逐步执行并校验期望,任一失败即非零退出。
`op` 一览:`log / submit / heartbeat / claim / renew / ack / sleep / waitStatus / assertStatus / assertEvents / assertLineEvents / assertOwnership`。

- **断网/失联**:领取后不再 `heartbeat`/`renew`/`ack`(配合 `sleep` 超过租约)
- **双网关争抢/接管**:两台网关对同一 `line` 心跳;主沉默超过 `OWNERSHIP_LEASE_TTL_MS` 后备用接管
- **网络分区旧代际仍发送**:`claim`/`ack` 显式给旧 `generation`,`expect: 'fenced'`
- **重复提交**:两个 `submit` 使用相同 `key`
- **乱序/重复确认**:任意顺序多个 `ack`,`expect: applied|ignored|duplicate|fenced`
- **执行身份沿用**:`claim` 的 `expectExecutionTokenOf`
- **接管因果审计**:`assertLineEvents`(产线维度)与 `assertOwnership`

完整字段见 [simulator/scenario.ts](simulator/scenario.ts) 顶部注释;示例见
[scenarios/offline-redelivery.json](scenarios/offline-redelivery.json) 与 `npm run simulate` 的内建场景。

## 测试与验收

- `npm test`:22 个用例。状态机测试注入假时钟/ID,确定性复现超时、迟到、乱序、重复、
  心跳争抢、接管、fencing 判定矩阵;恢复测试用真实 SQLite 文件,以"关库→同库重启服务"
  模拟进程崩溃,验证所有权代际跨重启保持。
- `npm run e2e`:编译后真实启动服务子进程,分四段验收——
  A 崩溃注入(落库后响应前退出,exit 42)→ 重启幂等重提;
  B spawn 编译后的模拟器跑完整故障场景(27 项断言:争抢/接管/fencing/重复与乱序确认/重试耗尽);
  C 再次重启验证状态恢复、幂等保持、所有权保持、事件序号单调;
  D 双冗余网关全链路:争抢 → 主领取 → 调度进程重启(fencing 保持)→ 主沉默 → 备用接管 →
  旧代际领取/确认被拒且状态不被推进 → 新代际重投沿用执行身份并确认成功 → 产线事件流还原因果。
