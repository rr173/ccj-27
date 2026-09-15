# msg-bridge

桥接两个消息系统（下称 **A** / **B**）的参考实现。连接器、映射器、投递器是**独立进程/容器**，
仅通过 HTTP 通信，各自持久化自己的状态。核心保证：

- 一条业务消息跨桥**只产生一次有效投递**（幂等发布 + 双向映射）
- 两端互相转发形成的**回环会被识别并抑制**，不会无限复制
- 任一端断线时，对方已确认的消息**不丢**；恢复后从正确序列**补发**
- 同序列内容冲突时**冻结该段**，支持人工 `skip / override / keep_local`
- 映射失败、重复确认、连接器重启、部分批量成功都留有**可查询的投递链**
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
# 一键在单进程内拉起全部 6 个组件（开发/演示）
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

---

## 3. Docker 部署（每组件独立）

```bash
docker compose up --build
```

compose 启动 6 个独立服务（`broker-a/b`、`connector-a/b`、`mapper`、`deliverer`），
映射器/投递器/连接器各自挂载独立数据卷。任意一个都可单独重建、替换、扩缩容，
彼此只靠 HTTP 解耦：

| 服务 | 端口 | 关键环境变量 |
|---|---|---|
| broker-a / broker-b | 8101 / 8201 | `PORT` |
| mapper | 8301 | `DATA_DIR`, `BRIDGE_INSTANCE` |
| deliverer | 8401 | `MAPPER_URL`, `CONNECTOR_A_URL`, `CONNECTOR_B_URL`, `BATCH_MAX` |
| connector-a | 8501 | `SIDE=A`, `BROKER_URL`, `MAPPER_URL`, `DELIVERER_URL` |
| connector-b | 8601 | 同上（`SIDE=B`） |

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
- broker：`POST /publish`、`POST /pull`、`POST /ack/:seq`(A) / `POST /ack`(B)、
  `POST /admin/rewrite/:seq`（演练冲突）、`GET /admin/state`

---

## 7. 从参考实现走向生产

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
  systems/broker-a.js        # 模拟系统 A（逐条 ack）
  systems/broker-b.js        # 模拟系统 B（累积 ack）
bin/start-all.js bin/demo.js bin/bridgectl.js
tests/e2e.test.js            # 10 个保证场景
Dockerfile docker-compose.yml
```
