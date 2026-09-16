# msg-bridge

桥接两个消息系统（下称 **A** / **B**）的参考实现。连接器、映射器、投递器是**独立进程/容器**，
仅通过 HTTP 通信，各自持久化自己的状态。核心保证：

- 一条业务消息跨桥**只产生一次有效投递**（幂等发布 + 双向映射）
- 两端互相转发形成的**回环会被识别并抑制**，不会无限复制
- 任一端断线时，对方已确认的消息**不丢**；恢复后从正确序列**补发**
- 同序列内容冲突时**冻结该段**，支持人工 `skip / override / keep_local`
- 映射失败、重复确认、连接器重启、部分批量成功都留有**可查询的投递链**
- 多租户**流量预算与公平调度**：租户双向独立速率/突发/最大在途、业务类共享预算、
  原子预占→结算、有上限等待区与可查询预计资格时间、租户间按权重 DRR 公平推进、
  租户内 FIFO、预算修订号 CAS、接管后过期预占单次回收（见第 8 节）
- 支持 Docker / docker-compose 部署

> 两个系统的编号与确认机制刻意不同，用来证明桥接层确实吸收了差异：
>
> | | 编号 | 确认 | 重投 |
> |---|---|---|---|
> | **系统 A** | 字符串式每消息序号 `"101"` | **逐条** ack（允许空洞） | 可见性超时后重新可见，带 `attempt` |
> | **系统 B** | 整数连续日志序号 `1,2,3` | **累积** ack（ack(n) 确认所有 ≤n） | 重复 pull 返回同一条 |

---

## 1. 架构

```
                 ┌─────────────────────── mapper ───────────────────────┐
                 │ 双向映射 / body 变换 / 冻结段 / 重复确认 / 投递链(权威)  │
                 └───────────────▲───────────────────▲───────────────────┘
                                 │ /map /ack /event   │ /freeze /resolve
  system A                ┌──────┴──────┐      ┌──────┴──────┐            system B
┌────────────┐  pull/pub  │ connector-A │      │ connector-B │  pull/pub ┌────────────┐
│  broker-a  │◄──────────►│  ingress+    │      │  ingress+    │◄────────►│  broker-b  │
│ 逐条 ack    │  /egress   │  egress      │      │  egress      │ /egress  │ 累积 ack    │
└────────────┘            └──────▲───────┘      └──────▲───────┘          └────────────┘
                                 │  /deliver           │
                                 └──────────┬──────────┘
                                            ▼
                                 ┌───────────────────────┐
                                 │      deliverer        │  outbox + 批量
                                 │ 重试 / 部分成功 / 两阶段 │  + 回调补发
                                 └───────────────────────┘
```

**投递是两阶段（outbox 模式）**，这是"不丢且不重"的关键：

1. 连接器 ingress 拉到消息 → 回环检查 → 序列冲突检查 → `mapper /map` 得到映射与变换后 body →
   `deliverer /deliver`（**入箱即持久化**，源消息此刻**还没 ack**）。
2. deliverer 批量推到**对端连接器** `/egress`，对端用**幂等键**发布到 broker；
   发布成功后 deliverer 回调**源连接器** `/ingress/complete`，由源连接器按 A/B 各自的语义确认源消息。
3. 任何一阶段失败都安全：没确认的源消息会重投（幂等键保证对端只有一份），
   已发布但回调失败的由 deliverer 的 pending-callback 在连接器恢复后补发确认。

---

## 2. 快速开始

要求 Node.js ≥ 18（**零运行时依赖**）。

```bash
# 一键在单进程内拉起全部 7 个组件（开发/演示）
npm start

# 另开终端：带旁白的演示，覆盖全部保证
npm run demo

# 端到端测试（10 个场景，使用临时数据目录与高端口）
npm test
```

运维 CLI（组件在默认端口时）：

```bash
node bin/bridgectl.js overview
node bin/bridgectl.js mappings
node bin/bridgectl.js chain <mappingId>
node bin/bridgectl.js freezes
node bin/bridgectl.js freeze <freezeId> resolve skip      # 保留本地映射，丢弃冲突副本，推进序列
node bin/bridgectl.js freeze <freezeId> resolve override  # 接受新内容，生成"取代"映射并重投
node bin/bridgectl.js freeze <freezeId> resolve keep_local
node bin/bridgectl.js retry <mappingId> --body '{"id":"o-1",...}'
node bin/bridgectl.js events --type SOURCE_ACK_DUPLICATE
```

配额调度器 CLI（默认 `http://localhost:8701`，可用 `QUOTA_URL` 覆盖）：

```bash
node bin/quotactl.js overview
node bin/quotactl.js tenants
node bin/quotactl.js tenant <tenantId>            # 当前占用 / 等待顺序 / 预计资格时间
node bin/quotactl.js configure-tenant <id> --revision N --in '{...}' --out '{...}'
node bin/quotactl.js configure-class <id> --in '{...}' --out '{...}'
node bin/quotactl.js reserve <tenantId> in --request r-1 [--class gold]
node bin/quotactl.js settle <reservationId> --complete   # 或 --abort
node bin/quotactl.js request r-1
node bin/quotactl.js records <tenantId> --type EXPIRY_RECLAIMED
```

---

## 3. Docker 部署（每组件独立）

```bash
docker compose up --build
```

compose 启动 7 个独立服务（`broker-a/b`、`connector-a/b`、`mapper`、`deliverer`、
`quota`），映射器/投递器/连接器/配额器各自挂载独立数据卷。任意一个都可单独重建、
替换、扩缩容，彼此只靠 HTTP 解耦：

| 服务 | 端口 | 关键环境变量 |
|---|---|---|
| broker-a / broker-b | 8101 / 8201 | `PORT` |
| mapper | 8301 | `DATA_DIR`, `BRIDGE_INSTANCE` |
| deliverer | 8401 | `MAPPER_URL`, `CONNECTOR_A_URL`, `CONNECTOR_B_URL`, `BATCH_MAX` |
| connector-a | 8501 | `SIDE=A`, `BROKER_URL`, `MAPPER_URL`, `DELIVERER_URL` |
| connector-b | 8601 | 同上（`SIDE=B`） |
| quota | 8701 | `DATA_DIR`, `QUOTA_TICK_MS`, `QUOTA_REAPER_MS`, `QUOTA_LEASE_TTL_MS` |

---

## 4. 投递链（delivery chain）与存储

每个有状态组件的存储是一个**只追加事件日志**（`journal.jsonl` + 周期快照），
当前状态是日志的 fold。日志本身即是审计链，永远不就地改写历史
（override 也是追加一条 `MAPPING_SUPERSEDED` 再加新映射）。

按 mappingId 可查这条消息的完整轨迹，例如：

```
MAPPING_CREATED → EGRESS_ENQUEUED → EGRESS_PUBLISHED → SOURCE_ACKED
```

异常路径同样成链：

- 映射失败：`MAPPING_CREATED → MAP_FAILED`（可 `retry` → `RETRY_REQUESTED → EGRESS_*`）
- 部分批量：失败项 `EGRESS_FAILED`，重试成功后 `EGRESS_PUBLISHED`
- 回环抑制：`LOOP_SUPPRESSED`（挂在原始 mappingId 上）
- 重复确认：`SOURCE_ACKED` 之后再确认 → `SOURCE_ACK_DUPLICATE`
- 连接器重启：每次启动追加 `CONNECTOR_STARTED`（含 runId 与恢复水位线）
- 序列冲突：`FREEZE_OPENED → RESOLVED_SKIP / RESOLVED_OVERRIDE / RESOLVED_KEEP_LOCAL`

查询接口：`GET /chain/:id`、`GET /mappings`、`GET /freezes`、`GET /events`、`GET /overview`。

---

## 5. 各项保证是怎么实现的

### 5.1 一次有效投递（幂等 + 双向映射）

- 映射键：`(originSide, ingressSeq)` → `mappingId`；另有 `(direction, bizKey)` 收敛业务重复。
- 投递幂等键：`idem:<originSide>:<ingressSeq>`（override 重映射用 `...:v2`）。
- 对端 broker 的 `/publish` 按幂等键去重；连接器本地记录 `idempotencyKey → peerSeq`，
  即使"已发布但未收到响应→重启→重发"，也拿到同一个 peerSeq，不会产生第二条。
- 双向都能从任一侧序号反查另一侧（`peerSeq` 与 `ingressSeq` 双向落 mapping 记录）。

### 5.2 回环抑制

每条跨桥消息带 header `x-bridge-marker: v1:<instance>:<originSide>:<mappingId>:<ttl>`。

- 入口连接器看到 marker 的 `instance` 等于本桥实例 → 这是**自己发出的消息被弹回**：
  不再 map/publish，直接确认掉，并记录 `LOOP_SUPPRESSED`。
- 来自**其它桥**实例的 marker（多桥环形拓扑）：允许继续转发但 `ttl` 每跳减 1，
  `ttl<=0` 时丢弃，环路上界为 3 跳，不会无限放大。

### 5.3 断线不丢 + 从正确序列补发

- 源消息**在对端发布确认之后**才 ack；未 ack 前 A 超时会重投、B 重 pull 会再给，
  mapper/deliverer 的幂等响应保证重投安全。
- 对端连接器宕机时，消息停在 deliverer 的 outbox（`QUEUED`，带退避 `nextAttemptAt`）；
  对端恢复后自动补发。
- 已发布但源确认回调失败（源连接器恰好重启）时进入 `callbacksPending`，
  恢复后由 deliverer 的补偿循环完成源确认。
- B 侧连接器持久化本地"已完成连续水位线"，并从 broker pull 响应学习 broker 权威水位线，
  重启后据此续传；乱序完成也能正确求出连续前缀。

### 5.4 同序列冲突 → 冻结该段 → 人工选择

连接器对未完成的在途序号比对内容哈希；同序号但哈希变化即判定冲突，调用 mapper 冻结：

- 冻结期间该序号不投递；在 B 上因为是累积确认，水位线停在缺口前，**后面的消息也无法越过**，
  正是"冻结该段"。
- `skip`：保留本地（先到）映射，丢弃冲突副本并推进确认。
- `override`：接受新内容。连接器拉取 broker 当前副本，mapper 生成**新映射**并把旧映射标记为
  `MAPPING_SUPERSEDED`，新映射正常投递，然后推进序列。
- `keep_local`：仅记录人工裁决，不自动推进（留作进一步处置）。

### 5.5 可查询的异常链

映射失败（毒消息会被确认以免空转，但完整证据在链上、可人工修正后 replay）、
重复确认、连接器重启、部分批量成功/失败重试，全部以独立事件类型追加，可按
mappingId / 事件类型 / 序号查询，见第 4 节。

---

## 6. HTTP API 摘要

- mapper：`POST /map`、`POST /ack`、`POST /freeze`、`POST /freeze/:id/resolve`、
  `POST /remap-conflict`、`POST /mappings/:id/retry`、`POST /event`、`POST /connector/started`、
  `GET /chain/:id`、`GET /mappings`、`GET /freezes`、`GET /events`、`GET /overview`
- deliverer：`POST /deliver`、`POST /loop-suppressed`、`GET /deliveries`、`GET /events`、`GET /overview`
- 连接器：`POST /egress`、`POST /ingress/complete`、`POST /ingress/release`、
  `GET /state`、`GET /events`、`GET /overview`
- quota：见第 8.6 节（`/reserve`、`/reservations/:id/settle`、`/admin/tenants|classes`、
  `/tenants`、`/classes`、`/requests/:id`、`/waiting`、`/events`、`/overview`）
- broker：`POST /publish`、`POST /pull`、`POST /ack/:seq`(A) / `POST /ack`(B)、
  `POST /admin/rewrite/:seq`（演练冲突）、`GET /admin/state`

---

## 8. 多租户流量预算与公平调度（quota，端口 8701）

独立的 **quota** 服务，和其它组件一样用只追加账本持久化（`data/quota/journal.jsonl`），
只通过 HTTP 解耦。CLI 见 `bin/quotactl.js`。

### 8.1 预算模型

- **租户预算**：每个方向（`in` / `out`）独立配置 `ratePerSec`（令牌速率）、
  `burst`（突发桶容量）、`maxInflight`（最大在途）；租户级另有 `weight`（DRR 权重）、
  `waitCapacity`（等待区上限）、`holdTtlMs`（预占有效期）、`waitTimeoutMs`（等待超时）。
  两个方向的令牌桶与在途计数互不影响。
- **业务类共享预算**：`class` 也有双向 `ratePerSec / burst / maxInflight`。请求带
  `class` 时必须**同时**通过租户预算和类预算（桶令牌 + 在途名额都要够）。类预算由所有
  命中该类的租户共享。
- **原子预占 → 结算**：`POST /reserve` 是一次原子判定：
  - 立即通过：返回 `GRANTED` + `reservationId` + 过期时刻；此时令牌**已扣除**、在途名额
    **已占用**。
  - 不能通过：进入该租户该方向的 FIFO 等待区（有上限，满了返回 `429 wait_area_full`），
    返回 `WAITING`、排队位置与**预计资格时间（上界）**。反复查询状态或重复发请求
    （相同 `requestId` 走幂等返回原结果）**不会改变排队位置**，不能靠轮询取得优先权。
  - 业务在预占有效期内完成后调用 `POST /reservations/:id/settle`：
    - `outcome=complete`：释放在途名额，令牌**永久消耗**（不退还，速率由令牌桶自然补充）；
    - `outcome=abort`：释放在途名额并**退还令牌**（同时补记持有期间自然再生的令牌）。

### 8.2 公平调度

调度器每个 tick（默认 50ms；结算/配置变更会立即额外触发一次）按 **DRR（赤字轮询）**
在租户间推进：

- 租户间按 `weight` 分配每轮可服务额度（`quantum = weight`），用不完的赤字有上限地结转；
- **租户内严格 FIFO**（方向内按到达顺序），后面的请求不能越过队头；两个方向各有独立
  FIFO 队头，互不阻塞；
- 每轮结束后游标停在"最后被服务租户的下一个"，于是当某类共享资源（或突发窗口）被一个
  持续打满额度的租户占用时，资源一空出，未被服务的租户排在最前——重租户与轻租户在单一
  共享名额上**严格交替**，低流量租户不会被饿死。

### 8.3 预算修订号（乐观并发）

- 每次配置变更返回单调递增的 `revision`；修改时带 `expectedRevision`，并发修改只有一个
  成功，其余返回 `409 revision_conflict`（不传则覆盖式更新）。
- **降低额度**：已经取得资格（`HELD`）的预占**不撤回**；已在等待的请求不丢弃，按新规则
  重新参与判定（桶的突发上限立即下调，速率立即按新值再生）。
- **提高额度**：配置事件追加后调度器**立即**运行一次，等待区里符合新容量的请求当场晋升，
  无需等旧预占过期。

### 8.4 崩溃恢复与"恰好回收一次"

- 每次启动生成 `ownerEpoch`，并通过账本里的**租约**（`LEASE_ACQUIRED`，默认 TTL 5s，
  约 TTL/2 续约）保证同一时刻只有一个调度器推进/回收；租约过期后新进程可接管
  （JSONL 参考实现为单写者；多副本请换第 7 节的 Postgres 仓库）。
- 调度器意外退出后，**未过期的预占仍然有效**——它们是账本事件，重启 fold 后状态完整，
  接管者继续承认其结算。
- 过期预占只能由持有租约的**唯一一个接管者**回收（`EXPIRY_RECLAIMED`：状态机保证同一预占
  只回收一次，随后退还令牌、释放名额）。
- 原处理方迟到的完成回执：预占已过期则返回 `LATE_AFTER_EXPIRY` 且**不再次释放任何额度**；
  预占已结算则返回 `DUPLICATE`。

### 8.5 查询与审计

- `GET /tenants/:id`：当前占用（双向令牌余量、在途预占明细）、等待顺序（位置/预计资格
  时间/排队时修订号）、预算修订号；`GET /tenants` 为列表。
- `GET /classes/:id` / `GET /classes`：共享预算占用与等待。
- `GET /requests/:id`：按请求 `requestId` 查 `WAITING / HELD / COMPLETED / ABORTED /
  EXPIRED / TIMED_OUT / REJECTED`，等待中含位置与预计资格时间。
- `GET /tenants/:id/records?type=GRANTED|SETTLED|EXPIRY_RECLAIMED`：该租户每一次预占、
  结算、过期回收的事件记录；另有 `GET /events?tenantId=`、`GET /waiting`、`GET /overview`。

```bash
# 配置（首次不传 expectedRevision）
node bin/quotactl.js configure-tenant acme \
  --in  '{"ratePerSec":20,"burst":40,"maxInflight":10}' \
  --out '{"ratePerSec":10,"burst":20,"maxInflight":5}' --weight 3 --hold-ms 30000
node bin/quotactl.js configure-class gold \
  --in '{"ratePerSec":100,"burst":100,"maxInflight":50}' \
  --out '{"ratePerSec":100,"burst":100,"maxInflight":50}'
# 并发修改：带修订号，只有一个成功（另一个 409 revision_conflict）
node bin/quotactl.js configure-tenant acme --revision 1 --in '{"ratePerSec":5,"burst":10,"maxInflight":4}'

# 预占 / 查询 / 结算
node bin/quotactl.js reserve acme in --request r-1 --class gold
node bin/quotactl.js request r-1
node bin/quotactl.js settle rsv_... --complete     # 或 --abort
node bin/quotactl.js tenant acme                   # 占用 + 等待顺序 + 预计资格时间
node bin/quotactl.js records acme                  # GRANTED/SETTLED/EXPIRY_RECLAIMED
```

### 8.6 HTTP API 摘要（quota）

`POST /reserve`、`POST /reservations/:id/settle`、
`POST /admin/tenants`、`POST /admin/classes`、
`GET /tenants`、`GET /tenants/:id`、`GET /tenants/:id/records`、
`GET /classes`、`GET /classes/:id`、
`GET /requests/:id`、`GET /reservations/:id`、`GET /waiting`、`GET /events`、`GET /overview`。

---

## 9. 从参考实现走向生产

当前持久化是**单卷、仅 fsync 到 OS 页缓存的 JSONL 账本**（零依赖、便于看懂与测试）。
上生产建议：

1. **把 `src/store/ledger.js` 换成 PostgreSQL 仓库**（接口仅 `open/append/subscribe/readEvents`）。
   事件表 `(aggregate_id, seq, type, data jsonb, ts)` 加唯一约束与 `INSERT ... RETURNING`，
   状态 fold 用物化表/快照；compose 里已预留注释掉的 `postgres` 服务。这样连接器/投递器可多副本。
2. 入口连接器与 broker 之间换成真实协议（Kafka offset、AMQP ack、MQTT、HTTP webhook…），
   每种系统一个 adapter，内部仍走同一套 map/deliver/ack 契约。
3. 给 deliverer 增加死信队列（`terminal` 失败已有标记）与告警；mapper 增加冻结段告警。
4. marker 用签名头防止外部伪造；按拓扑配置真实 ttl 与多实例白名单。

## 目录

```
src/
  util.js log.js http.js client.js
  store/ledger.js            # 只追加事件账本（可换成 Postgres）
  mapping/transform.js       # A<->B 信封与业务 body 映射
  mapper/index.js            # 映射器：映射/冻结/确认/投递链
  deliverer/index.js         # 投递器：outbox/批量/重试/回调补偿
  connector/index.js         # 连接器（参数化 SIDE=A|B）
  quota/bucket.js            # 令牌桶（惰性补充/扣除/退还）
  quota/scheduler.js         # 租户间 DRR 公平策略 + 预计资格时间计算
  quota/index.js             # 配额调度器：预占/等待/结算/过期回收/修订号/租约
  systems/broker-a.js        # 模拟系统 A（逐条 ack）
  systems/broker-b.js        # 模拟系统 B（累积 ack）
bin/start-all.js bin/demo.js bin/bridgectl.js bin/quotactl.js
tests/e2e.test.js            # 10 个保证场景
tests/quota.test.js          # 9 个配额/公平调度/接管场景
Dockerfile docker-compose.yml
```
