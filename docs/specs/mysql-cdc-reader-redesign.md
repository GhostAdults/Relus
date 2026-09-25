# MySQL CDC Reader Redesign and Candidate Evaluation

Status: **SPEC READY**

Date: 2026-09-25

## 1. Problem Statement

Relus 当前的 MySQL Binlog Reader 使用 `mysql_cdc 0.2.1`。该 crate 无条件依赖
`openssl`，即使运行时配置为禁用 TLS，构建时仍会引入 `openssl-sys`。这导致：

- Windows CI 依赖 OpenSSL、Perl、NASM 或特定系统安装路径。
- Windows GNU、Linux ARM64、macOS Apple Silicon 和 MUSL 的构建与交叉编译成本升高。
- 当前依赖承担了 OpenSSL 构建成本，但并未为 Relus 提供满足要求的完整 TLS 身份验证能力。
- Binlog Reader 已存在 `gtid`、`checkpoint_dir` 等配置字段，但运行逻辑没有真正使用它们。
- 当前 `JsonStream` 只传递业务 JSON，Reader 不知道 Sink 何时成功提交，因此无法安全推进 Binlog offset。
- 当前全局 `column_mapping` 和单个字符串 `table_filter` 无法正确表达多表 CDC、每表映射和跨表事务。

本设计以替换底层 CDC 依赖为起点，但目标不是只消除一个构建错误。新设计必须建立清晰的
MySQL CDC 配置、事件信封、事务边界、恢复位点、TLS、错误分类、Sink 能力校验和验证矩阵。

## 2. Goals

1. 评估并选出一个不依赖 `openssl`/`openssl-sys` 的 MySQL CDC 候选实现。
2. 通过独立原型证明候选具备 Relus 所需的复制协议、TLS、恢复和取消能力。
3. 为未来生产迁移定义稳定的 MySQL CDC 配置和内部适配边界。
4. 首版生产语义正式承诺 **at-least-once**，不虚假宣称 exactly-once。
5. 支持 MySQL 5.7、8.0 和 8.4 LTS 作为正式目标版本。
6. 支持以下正式构建目标：
   - Windows MSVC
   - Windows GNU
   - Linux x86_64
   - Linux ARM64
   - macOS Apple Silicon
   - MUSL 静态编译目标
7. 支持 file/position 和 MySQL GTID 两种恢复位置。
8. 支持一个 Job 限定一个数据库，并消费该数据库下多张表。
9. 使用完整、可版本化的 CDC 事件信封传递事务和 offset 信息。
10. 为未来的端到端 checkpoint acknowledgment 和 Sink 能力模型保留明确边界。

## 3. Non-goals for the Approved Execution Scope

本规格描述最终目标架构，但当前批准的执行范围只到第 18 节中的阶段三。当前范围明确不包括：

- 不替换生产 `reader/src/binlog_reader.rs`。
- 不修改生产 Engine、Writer 或 Redb checkpoint 流程。
- 不删除当前 `mysql_cdc` 或 OpenSSL 过渡依赖。
- 不实现生产级 exactly-once。
- 不在 CDC Job 中自动创建或修改目标表。
- 不自动修改 MySQL 全局变量、用户、权限、Binlog 文件或服务状态。
- 不提交、推送、创建 PR 或部署。
- 不把未实际运行的版本或平台标记为通过。

生产迁移必须在全部正式硬门槛得到证据、原型报告完成并由用户再次确认后单独实施。

## 4. Decision Summary

### 4.1 Delivery semantics

- 首版正式保证为 **at-least-once**。
- Checkpoint 只能推进到 Sink 已经确认处理完成的完整 MySQL 事务。
- 故障恢复允许重放未确认事务，但不得跳过或丢失已提交源事务。
- `event_id` 在重放后必须稳定，以便 Sink 去重。
- 不支持静默跳过有毒记录；首版遇到无法解析或无法写入的数据时停止 Job。

### 4.2 Candidate strategy

- 候选按静态证据先排序，然后依次验证。
- 第一个通过全部门槛的候选即停止后续候选原型，不实现三套完整原型。
- 初始静态候选包括：
  1. `mysql_async`
  2. `mysql`
  3. `mysql-binlog-connector-rust`
- 静态评分完成后再决定第一原型候选，不在规格中预设赢家。
- 如果第一候选只缺少小型外围能力，可用本地临时 patch 验证；正式 fork 必须再次取得用户确认。
- 如果缺少握手、认证、事件解析等协议核心能力，则淘汰并测试下一候选。

### 4.3 OpenSSL acceptance criterion

新候选及未来生产 Binlog Reader 的依赖树中不得出现：

```text
openssl
openssl-sys
```

验收必须执行：

```bash
cargo tree -i openssl
cargo tree -i openssl-sys
```

两条命令都必须报告没有匹配包。`openssl-sys/vendored` 不视为通过，因为它仍引入 Perl、
C 编译器、汇编器和跨平台维护成本。

当前 Windows MSVC vendored OpenSSL 仅是过渡方案，生产 Reader 成功替换后删除。

## 5. Candidate Hard Gates

候选必须满足以下硬门槛；任何明确不支持的核心项都会淘汰候选：

1. 支持 MySQL 5.7、8.0、8.4 LTS。
2. 支持 file/position 和 MySQL GTID 启动与恢复。
3. TLS 不依赖 OpenSSL，并支持 CA 与服务器身份验证。
4. 支持 Windows MSVC、Windows GNU、Linux x86_64、Linux ARM64、macOS Apple Silicon 和 MUSL。
5. 能解析 INSERT、UPDATE、DELETE、TABLE_MAP、ROTATE、GTID、XID/COMMIT 和 HEARTBEAT。
6. 支持 `mysql_native_password` 和 `caching_sha2_password`。
7. 能主动取消网络读取并正常关闭，不遗留不可控的阻塞线程。
8. 能表达完整事务边界和稳定恢复 offset。
9. 支持背压，不在 Sink 变慢时无限增长内存。
10. 在 Relus 当前声明的 Workspace MSRV 下编译，不提高整个 Workspace MSRV。
11. 仅使用 MIT、Apache-2.0、BSD、ISC 等宽松许可证；未知、自定义、GPL 或 AGPL 依赖需法律确认，不能直接进入生产候选。

`sha256_password` 作为记录能力，不是首批硬门槛。不支持的认证插件必须在握手阶段返回明确、脱敏的错误。

## 6. Candidate Scoring

全部硬门槛通过后，按以下权重评分：

| Category | Weight |
| --- | ---: |
| 协议和事件正确性 | 30% |
| TLS、认证和安全性 | 20% |
| 跨平台及 MUSL | 15% |
| 中断、背压和异步集成 | 15% |
| 维护活跃度与代码质量 | 10% |
| 性能 | 5% |
| Relus 迁移代码量 | 5% |

正常依赖锁定精确版本。若未来使用 fork，依赖必须锁定到不可变 commit，并记录：

- 上游版本和 commit。
- Relus 补丁清单。
- 许可证。
- MSRV。
- 未解决缺陷。
- rebase 与长期维护成本。

## 7. Configuration Architecture

### 7.1 Configuration ownership

- 通用的连接、Secret、TLS、重试类型放入 `common/`。
- MySQL Binlog/CDC 专属配置留在 `reader/`。
- 执行逻辑禁止继续散落使用 `serde_json::Value::get` 读取字段。
- 新配置结构使用严格反序列化，拒绝未知字段并报告完整 JSON 路径。
- Source 类型从 `mysql_binlog` 改为 `mysql_cdc`。
- 检测到旧 `mysql_binlog` 或旧平铺字段时返回迁移错误，不永久支持双格式。
- 配置带独立整数 `config_version`，未知的更高版本必须拒绝。

### 7.2 Proposed top-level shape

```json
{
  "execution": {
    "delivery_guarantee": "at_least_once",
    "checkpoint": {
      "enabled": true,
      "interval": "30s",
      "timeout": "2m",
      "max_consecutive_failures": 3
    }
  },
  "source": {
    "name": "mysql-sales-cdc",
    "type": "mysql_cdc",
    "config_version": 1,
    "config": {
      "source_id": "mysql-prod-sales",
      "connection": {},
      "replication": {},
      "tls": {},
      "startup": {},
      "checkpoint": {},
      "retry": {},
      "heartbeat": {},
      "buffer": {},
      "tables": []
    }
  },
  "sink": {
    "name": "warehouse",
    "type": "database",
    "connection": {},
    "write": {},
    "tables": []
  }
}
```

本规格先定义统一 Source/Sink 配置方向。当前生产实现范围不要求一次性迁移其他 Reader 和 Writer，
但新 MySQL CDC 配置必须遵循该结构，不能再新增一套不一致的动态 JSON 风格。

### 7.3 Delivery guarantee ownership

借鉴 SeaTunnel 的职责分离，但不复制其具体字段：

- `execution.delivery_guarantee` 是用户要求的端到端保证等级。
- Source 声明并实现 offset snapshot、延迟确认和恢复能力。
- Sink 通过代码接口声明实际能力，不能由配置伪造。
- 用户选择较低保证时，即使组件能力更高，也只启用并承诺所选等级。
- 首版只开放 `at_least_once`；未来完成事务 checkpoint/2PC 后才允许 `exactly_once`。

能力校验规则：

```text
requested guarantee <= effective end-to-end capability  => allow start
requested guarantee > effective end-to-end capability   => fail validation
```

不在 Reader 或 Sink 中使用含义混乱的 `exactly_once: bool` 表示端到端语义。

## 8. Connection, Secrets, and TLS

### 8.1 Accounts

- 默认允许快照和复制共用一个账号。
- 生产环境允许分别配置 snapshot account 和 replication account。
- Relus 只检查权限并报告缺失项，不创建用户、不执行 `GRANT`。

### 8.2 Secrets

Secret 支持：

- 直接值。
- 环境变量引用。
- 文件引用。

所有日志、`Debug`、错误输出和公开序列化必须脱敏。生产类型使用专门的 `SecretValue`，不得派生会泄露内容的默认 `Debug`。

### 8.3 TLS modes

仅支持：

- `disabled`
- `required`
- `mutual`

不支持自动降级到明文的 `preferred`，不支持 `skip_verify`。

TLS 规则：

- `required` 必须验证 CA、证书链、主机名或合法 IP SAN。
- `mutual` 额外要求客户端证书与私钥。
- 首版证书格式为 PEM。
- 信任根显式配置为 `system` 或 `custom`。
- `server_name` 默认使用 hostname；连接 IP 时必须配置证书中的合法 DNS 名称或 IP SAN。
- 自定义 CA 加载失败不得回退到系统根或明文。

## 9. Startup Modes

`startup.mode` 必须显式配置，不提供危险默认值。支持：

### 9.1 `initial`

先同步一致性历史快照，再从快照对应的 Binlog 位点同步增量。

- 首版只支持 InnoDB。
- 启动时发现选中表包含非事务存储引擎则失败。
- 默认允许短暂全局读锁以取得一致快照和 Binlog/GTID 起点。
- `snapshot.locking=none` 仅在外部系统同时提供一致快照和对应 offset 时允许。
- 快照使用单连接、单 consistent snapshot 事务。
- 表扫描按主键或非空唯一索引分页；不存在稳定唯一键时失败。
- 快照阶段全部确认后，状态从 `SNAPSHOTTING` 进入 `CATCHING_UP`，再进入 `STREAMING`。
- 快照期间不把 Binlog 事件无限缓存在内存；快照完成后从起点消费。
- 启动和完成快照后都验证起点 Binlog 文件仍存在，已被清理则失败。
- 快照中断后从头重新执行，不从已经失效的一致性事务中间恢复。
- `initial` 只允许 Sink 声明具备幂等快照写入能力。

历史记录使用 `operation="snapshot"`，不伪装成实时 INSERT。

### 9.2 `earliest`

从 MySQL 当前仍保留的最早 Binlog 开始，不表示数据库建立以来的完整历史。

### 9.3 `latest`

从启动时最近的 offset 开始，只消费启动后的新变化。

### 9.4 `specific`

使用互斥的 offset 类型：

```json
{
  "mode": "specific",
  "offset": {
    "kind": "position",
    "file": "mysql-bin.000123",
    "position": 456
  }
}
```

或：

```json
{
  "mode": "specific",
  "offset": {
    "kind": "gtid",
    "set": "server-uuid:1-100"
  }
}
```

禁止同时配置 GTID 和 file/position。

### 9.5 `timestamp`

- 只接受带明确 UTC offset 的 RFC 3339 时间，内部统一转换为 UTC。
- Relus 使用候选库扫描 Binlog，将 timestamp 解析为明确的完整事务边界和 offset。
- 不依赖系统 `mysqlbinlog`，以保持跨平台和静态二进制能力。
- 从覆盖指定时间点的完整事务开始，即使事务开始时间略早于 timestamp。
- timestamp 早于当前最早可用 Binlog 时直接失败，不静默回退。
- 成功定位后持久化明确的 GTID 或 file/position，恢复时不重复扫描。

### 9.6 Checkpoint priority

```text
checkpoint exists  => resume from checkpoint
checkpoint missing => execute configured startup mode/fallback
restart from new position => explicit reset or new checkpoint_id
```

`initial` 只在首次启动或显式重置后运行。

## 10. Table Selection and Mapping

- 一个 MySQL CDC Job 限定一个数据库。
- 支持精确表名和 glob。
- 每张源表只能匹配一条规则；多条规则同时匹配时启动失败。
- 不使用全局 `JobConfig.column_mapping` 作为 MySQL CDC 映射来源。
- 每条表规则拥有自己的目标路由和列映射。
- 对 `mysql_cdc` 同时配置全局 `column_mapping` 时返回迁移错误。

示例：

```json
{
  "database": "sales",
  "tables": [
    {
      "include": "orders",
      "target": "ods_orders",
      "columns": {
        "id": "order_id",
        "status": "status"
      },
      "initial_target_policy": "require_empty"
    },
    {
      "include": "customer_*",
      "target": {
        "prefix": "ods_"
      },
      "columns": "*",
      "initial_target_policy": "upsert"
    }
  ]
}
```

`initial_target_policy` 支持：

- `require_empty`，默认值。
- `upsert`。
- `truncate_then_load`，必须显式启用且 Sink 声明安全支持。

运行期间不自动创建或修改目标表。启动前验证目标 Schema；不兼容则失败。可以单独生成 schema plan 或 SQL，但 CDC Job 不执行 DDL。

## 11. CDC Event Envelope

事件使用独立的整数 `schema_version`，与 Relus 和 MySQL 版本解耦：

```json
{
  "schema_version": 1,
  "event_id": "stable-event-id",
  "meta": {
    "database": "sales",
    "table": "orders",
    "operation": "update",
    "event_timestamp": "2026-09-25T12:00:00Z",
    "binlog_file": "mysql-bin.000123",
    "binlog_position": 456,
    "gtid": "server-uuid:101",
    "transaction_id": "server-uuid:101",
    "transaction_event_index": 2,
    "row_index": 0
  },
  "key": {
    "id": 1001
  },
  "before": {
    "status": "pending"
  },
  "after": {
    "status": "paid"
  }
}
```

语义固定为：

| Operation | before | after |
| --- | --- | --- |
| INSERT | `null` | 完整新行 |
| UPDATE | 完整旧行 | 完整新行 |
| DELETE | 完整旧行 | `null` |
| SNAPSHOT | `null` | 当前完整行 |

首版强制 `binlog_row_image=FULL`。不通过查询源表补齐 `MINIMAL` 事件，因为该查询可能读到后续事务状态。

`event_id`：

- 有 GTID：`GTID + transaction event index + row index`。
- 无 GTID：`binlog file + event position + row index`。
- 重放后必须保持一致，禁止使用随机 UUID。

常用类型硬门槛：

- 有符号和无符号整数，且不得损失精度。
- `DECIMAL`，不得转成有精度损失的浮点数。
- `FLOAT/DOUBLE`。
- `CHAR/VARCHAR/TEXT`。
- `BINARY/VARBINARY/BLOB`。
- `DATE/TIME/DATETIME/TIMESTAMP/YEAR`。
- `JSON`、`ENUM/SET`、`BIT`、`NULL`。
- 多字节 UTF-8、零日期和时区边界。

空间类型首版明确不支持，启动 Schema 校验时发现后失败。

## 12. Replication Adapter Boundary

第三方库类型只允许存在于适配器模块内。生产方向使用 Relus 自有接口和类型，例如：

```rust
trait MySqlReplicationClient {
    async fn connect(&mut self) -> Result<()>;
    async fn next_event(&mut self) -> Result<CdcEvent>;
    async fn close(&mut self) -> Result<()>;
}
```

Relus 自有类型包括：

- `CdcEvent`
- `CdcOffset`
- `TransactionBoundary`
- `ReplicationError`
- `ReplicationErrorKind`

错误分类至少包括：

- 临时网络错误。
- MySQL 暂时不可用。
- 认证失败。
- TLS 验证失败。
- Binlog 已清理。
- Offset/GTID 不合法。
- Schema 漂移。
- 不支持事件。
- 数据解码失败。

适配器负责把第三方错误映射成稳定枚举，并携带 `retryable` 分类和脱敏上下文。Engine 不解析错误字符串。

## 13. Transaction, Sink Capability, and Acknowledgment

### 13.1 Sink capabilities

Writer 通过代码接口声明事实能力，用户配置不能把不支持的能力声明成支持。能力模型至少包含：

```rust
pub struct SinkCapabilities {
    pub transactional_writes: bool,
    pub idempotent_snapshot: bool,
    pub idempotent_cdc: bool,
    pub supports_multi_table: bool,
}
```

Job 启动前校验实际能力与 `execution.delivery_guarantee`、startup mode 和表规则。

### 13.2 Transaction ordering

- 首版严格按 MySQL commit 顺序串行处理事务。
- 同一个源事务中的所有表变更保留共同事务边界。
- RDBMS Sink 支持时，在一个目标事务内原子提交源事务。
- 非事务 Sink 可以出现部分可见，但所有事件成功前不得确认 checkpoint。
- 大事务流式传输，不把全部事件装入内存；只在 COMMIT 被完整确认后推进位点。
- 提供事务行数和字节数警告阈值。

### 13.3 Acknowledgment

生产迁移阶段需要修改 Reader/Engine/Writer 边界，使 Sink 成功提交后能够确认对应源事务。Reader 不能在发出事件时自行推进 checkpoint。

首版 exactly-once 不在批准范围。未来如果 RDBMS Sink 把业务数据和 offset 表放在同一目标事务中，可单独设计更高保证；本地 Redb 与远端 Sink 之间不能通过简单布尔开关获得 exactly-once。

## 14. Checkpoint Model

### 14.1 Meaning

CDC checkpoint 是 **下游已安全确认的最后一个完整源事务位置**，不是普通 Scheduler 状态。

模型包含：

```json
{
  "format_version": 1,
  "checkpoint_id": "mysql-prod-sales",
  "source_id": "mysql-prod-sales",
  "server_uuid": "redacted-runtime-identity",
  "confirmed_offset": {
    "kind": "gtid",
    "set": "server-uuid:1-100"
  },
  "last_commit_timestamp": "2026-09-25T12:00:00Z",
  "source_fingerprint": "...",
  "schema_fingerprint": "...",
  "tables": {
    "orders": {
      "state": "streaming"
    }
  },
  "sequence": 42,
  "updated_at": "2026-09-25T12:00:30Z"
}
```

- 一个 Job 只有一个全局 confirmed Binlog offset，以保持跨表事务顺序。
- 表级状态只记录 `pending_snapshot`、`snapshotting`、`caught_up`、`streaming`、`removed` 等 bootstrap 生命周期。
- Checkpoint 复用现有 Redb 实例和生命周期，但使用独立表、数据结构和版本号。
- `format_version` 支持显式向前迁移；未知更高版本拒绝读取。
- 迁移前备份原记录。

### 14.2 Source identity

- 用户声明稳定 `source_id`。
- 运行时同时记录 MySQL `server_uuid`。
- Checkpoint 指纹包含源实例、数据库、表规则和事件语义。
- 密码、TLS 路径和重试参数不进入指纹。
- `server_uuid` 变化时默认暂停。
- `checkpoint migrate-source` 只有在新节点 GTID 集覆盖 checkpoint GTID 时允许迁移；file/position 不在不同服务器之间自动迁移。

### 14.3 Schedule and failure behavior

默认值：

```json
{
  "interval": "30s",
  "timeout": "2m",
  "max_consecutive_failures": 3
}
```

- 定时保存最后一个已经完整确认的源事务。
- Checkpoint 写入时允许后续处理继续，但每次写入携带单调序号。
- 持久层使用 compare-and-set 或单写入队列，禁止旧 checkpoint 覆盖新位点。
- Checkpoint 失败后进入 `CHECKPOINT_BLOCKED`，暂停继续扩大未确认窗口并重试。
- 连续失败或总耗时超过阈值后 Job 失败。
- Checkpoint 失败时不得确认新的 offset。

### 14.4 Management commands

未来生产迁移提供：

```text
rls checkpoint show <checkpoint-id>
rls checkpoint reset <checkpoint-id>
rls checkpoint migrate-source <checkpoint-id>
rls checkpoint migrate-config <checkpoint-id>
```

- Job 运行时拒绝 reset 或 migrate，必须先停止 Job。
- 表规则变化首版只支持停机后的显式 `migrate-config`。
- 新增表需要明确选择 `initial`、`latest`、`specific` 或 `timestamp`。
- 删除表显式标记 `stop-tracking`。
- 不自动接受配置变化，不在配置文件中保留永久 `reset_checkpoint=true`。

## 15. Runtime Behavior

### 15.1 Job state machine

正式状态：

```text
VALIDATING
SNAPSHOTTING
CATCHING_UP
STREAMING
RECONNECTING
CHECKPOINT_BLOCKED
STOPPING
STOPPED
FAILED
```

关键转换：

```text
VALIDATING -> SNAPSHOTTING -> CATCHING_UP -> STREAMING
VALIDATING -> STREAMING
STREAMING -> RECONNECTING -> STREAMING
running state -> CHECKPOINT_BLOCKED -> previous running state
running state -> STOPPING -> STOPPED
permanent error -> FAILED
```

状态必须驱动控制逻辑并持久化关键阶段，不能只是 UI 文本。

### 15.2 Retry

- 临时网络和暂时服务器错误使用指数退避和随机抖动。
- 默认无限重试，但允许配置最大次数或总时长。
- 认证、TLS、Schema、位点和不支持事件属于永久错误，立即失败。
- 从最后确认 checkpoint 重连。
- 状态显示累计次数、最近错误和下次重试时间。

### 15.3 Heartbeat

默认建议：

```json
{
  "interval": "10s",
  "timeout": "45s"
}
```

使用 MySQL replication heartbeat。有效事件或 heartbeat 都刷新连接活跃时间；超时进入 `RECONNECTING`。

### 15.4 Backpressure

- 同时限制事件数量和估算字节数。
- 到达任一上限时暂停继续读取，让 TCP 层施加背压。
- 暴露 `buffer.events`、`buffer.bytes` 和相应指标。
- 首版不实现磁盘溢写。

### 15.5 Graceful shutdown

- 进入 `STOPPING` 后不再开始新事务。
- 尝试完成当前事务、等待 Sink 确认并写入 checkpoint。
- 超过 `shutdown_timeout` 后强制退出，但不得推进 checkpoint。
- 未确认事务在下次启动时完整重放。

### 15.6 Binlog retention monitoring

暴露：

- `binlog_lag_bytes`
- `binlog_lag_seconds`
- `checkpoint_age_seconds`
- `checkpoint_file_available`

定期确认 checkpoint 所指 Binlog 仍存在。文件已被清理时立即失败；Relus 不自动修改 MySQL 保留参数。

## 16. Validation

未来提供：

```text
rls binlog validate -c job.json
```

验证内容：

- 严格配置结构与 Secret 引用。
- MySQL 版本和权限。
- `log_bin=ON`。
- `binlog_format=ROW`。
- `binlog_row_image=FULL`。
- MySQL `server_id != 0`。
- Relus replication `server_id` 必须显式配置，且当前进程内不得重复。
- TLS 握手、证书链和服务器身份。
- 表规则、存储引擎和主键/非空唯一索引。
- 源/目标 Schema 和每表映射。
- Sink 能力。
- Checkpoint 存在性、格式版本和指纹。
- Startup offset 是否仍可用。

正式启动时仍重复关键检查，不能把先前 validate 结果当成永久凭证。

影响数据语义或事务边界的未知 Binlog 事件立即失败；只有明确白名单中的无关事件可以忽略。

## 17. Prototype Scope

原型放置于：

```text
prototypes/mysql-cdc-candidate/
```

它拥有独立 `Cargo.toml`，不加入默认 workspace。建议结构：

```text
prototypes/mysql-cdc-candidate/
├── Cargo.toml
├── src/
├── tests/
├── README.md
└── test-state/
```

原型实现最小但完整的 CDC 客户端能力：

- 连接与认证。
- TLS required/mutual 验证。
- file/position 和 GTID。
- 事务边界和必要事件解析。
- 主动取消和正常关闭。
- 临时错误重连。
- 简化的持久化 offset，用于恢复实验。
- `initial` 的一张小表快照到增量切换证明。
- `timestamp` 定位完整事务的最小证明。
- failpoint 崩溃恢复实验。

原型不接入生产 Engine、Writer 或 Redb，不替换生产 Reader。

## 18. Execution Phases and Approval Boundary

### Phase 1: Specification and static evaluation

- 完成本规格。
- 调查候选 API、features、依赖树、TLS、认证、事件、取消、MSRV、许可证和维护状态。
- 生成静态评分和第一原型候选。

### Phase 2: Independent candidate prototype

- 在 `prototypes/mysql-cdc-candidate/` 实现第一候选。
- 若候选失败核心门槛，记录证据并测试下一候选。
- 若只需小型 patch，记录补丁设计并等待用户确认是否正式 fork。

### Phase 3: Available local verification

- 使用本机 MySQL 8.0 进行正式目标版本实测。
- 使用本机 MySQL 5.6 做非承诺兼容性参考，不代替 MySQL 5.7。
- Windows MSVC 运行完整集成测试。
- 当前不可用的 MySQL 5.7、8.4、Linux 运行和其他平台保持 `not_run`。
- 生成脱敏报告。

**当前执行授权到此结束。**

### Phase 4: Complete formal evidence matrix

- 补齐 MySQL 5.7、8.4。
- Linux x86_64 运行完整集成测试。
- 验证 Windows GNU、Linux ARM64、macOS Apple Silicon 和 MUSL 编译。
- 只有全部正式硬门槛有证据后，才允许进入生产迁移。

### Phase 5: Production Reader migration

需要用户再次明确批准。包括新配置、适配层、生产 Reader、Engine acknowledgment、Sink 能力和 checkpoint 管理。

### Phase 6: Remove transitional OpenSSL dependencies

生产 Reader 替换后：

- 删除 `mysql_cdc`。
- 删除 `openssl`、`openssl-src` 和 CI 中的 Perl/NASM 过渡安装。
- 更新锁文件。
- 用 `cargo tree -i openssl` 和 `cargo tree -i openssl-sys` 证明依赖消失。

### Phase 7: Production-grade checkpoint acknowledgment

实现完整的 Engine/Sink 确认、Redb checkpoint、CLI 管理和状态机。该阶段不因 Phase 1-3 完成而自动获批。

## 19. Local Integration Test Environment

不使用 Docker。测试连接由环境变量提供，每个 MySQL 版本使用独立变量：

```text
RELUS_TEST_MYSQL56_HOST
RELUS_TEST_MYSQL56_PORT
RELUS_TEST_MYSQL56_USERNAME
RELUS_TEST_MYSQL56_PASSWORD
RELUS_TEST_MYSQL56_DATABASE

RELUS_TEST_MYSQL80_HOST
RELUS_TEST_MYSQL80_PORT
RELUS_TEST_MYSQL80_USERNAME
RELUS_TEST_MYSQL80_PASSWORD
RELUS_TEST_MYSQL80_DATABASE
```

未来 5.7、8.4 使用同样命名规则。

证书只通过路径注入：

```text
RELUS_TEST_MYSQL_CA_PATH
RELUS_TEST_MYSQL_CLIENT_CERT_PATH
RELUS_TEST_MYSQL_CLIENT_KEY_PATH
RELUS_TEST_MYSQL_SERVER_NAME
```

执行代理只被允许配置和传递证书路径，不读取、复制、修改、输出或解析证书内容。

测试账号由用户提供，其数据库权限是允许操作的上限。测试工具：

- 可以创建测试所需数据库、表和数据。
- 只能修改或删除当前 run 自己创建的对象。
- 不要求数据库名必须带固定前缀。
- 使用当前 run 的对象清单进行定向清理。
- 不修改 MySQL 全局变量、用户、授权、Binlog 文件或服务状态。
- 异常退出后提供 `cleanup --run-id`，校验 server UUID 和对象清单后清理。

普通 `cargo test` 可以跳过缺少外部环境的集成测试。正式原型验收命令必须把缺失环境报告为失败或 `not_run`，不得把跳过计为通过。

## 20. Integration Test Scenarios

真实测试至少覆盖：

- INSERT、UPDATE、DELETE。
- 单表事务和跨表事务。
- 多行事件。
- TABLE_MAP、ROTATE、GTID、XID/COMMIT、HEARTBEAT。
- file/position 启动和恢复。
- GTID 启动和恢复。
- TLS 正确 CA、错误 CA、正确 server name、错误 server name、mutual TLS。
- `mysql_native_password`、`caching_sha2_password`。
- 主动取消、正常关闭、网络断线与重连。
- 下游背压和有界内存。
- 大事务。
- Schema 漂移导致安全停止。
- `initial` 小表快照与增量切换。
- `timestamp` 定位完整事务。
- 常用 MySQL 类型和边界值。

Failpoint 必须覆盖：

1. Sink 写入前退出。
2. Sink 写入后、checkpoint 保存前退出。
3. Checkpoint 写入过程中退出。
4. 大事务传输中退出。
5. 快照完成、切换增量前退出。

恢复断言：

- 不丢失源端已提交事务。
- 允许重放未确认事务。
- 重放 `event_id` 稳定。
- Checkpoint 不跨越未完整写入事务。

## 21. Performance Gates

在相同环境记录当前 `mysql_cdc` 基线，然后验证候选：

- 候选吞吐不低于当前实现的 80%。
- 持续消费内存不随总事件数线性增长。
- 实际缓冲不得超过配置字节上限的 110%。
- 大事务测试不得 OOM。
- 下游背压时必须停止无限拉取。
- 重连后不得丢失 checkpoint 之后的事务。

测量项：

- 行事件吞吐量。
- 端到端延迟。
- 100 万行持续消费内存峰值。
- 大事务内存增长。
- 背压行为。
- 重连恢复耗时。

性能不是替换的主要目标，但明显退化会淘汰候选。

## 22. Platform Verification Matrix

| Target | Runtime integration | Compile verification |
| --- | --- | --- |
| Windows MSVC | Required | Required |
| Windows GNU | Not required initially | Required |
| Linux x86_64 | Required before production migration | Required |
| Linux ARM64 | Not required initially | Required |
| macOS Apple Silicon | Not required initially | Required |
| MUSL | Build/static target required | Required |

本轮在 Windows 实测。Linux 使用相同验收脚本由未来 Linux 环境执行；在没有实际结果前保持 `not_run`。

## 23. MySQL Version Matrix

| Version | Support status | Current local evidence |
| --- | --- | --- |
| MySQL 5.6 | Compatibility only, not promised | Available locally |
| MySQL 5.7 | Formal target | `not_run` until environment exists |
| MySQL 8.0 | Formal target | Available locally |
| MySQL 8.4 LTS | Formal target | `not_run` until environment exists |

5.6 通过不能替代 5.7 或 8.4。缺少 GTID、TLS 或权限时，对应能力标记 `not_run`，不修改服务器来制造测试条件。

## 24. Reporting

正式评估报告保存为：

```text
docs/research/mysql-cdc-library-evaluation.md
```

报告包含：

- 候选静态评分和淘汰理由。
- 暂定候选和精确版本/commit/features。
- API、依赖树、MSRV 和许可证。
- OpenSSL/Rustls/MUSL 结果。
- MySQL 版本测试矩阵。
- TLS、GTID、事务、恢复、类型和性能证据。
- 平台构建矩阵。
- 未验证项目。
- 是否需要 fork 及补丁设计。
- 生产迁移范围和待删除的 OpenSSL 过渡配置。

每项能力只能使用：

- `passed`
- `failed`
- `not_run`
- `unsupported`
- `blocked`

候选总体状态只能使用：

- `recommended_for_production_migration`
- `conditionally_recommended`
- `rejected`

当前缺少 MySQL 5.7、8.4 和 Linux 实测时，最高只能是 `conditionally_recommended`。

报告只记录版本、平台、命令、结果和脱敏标识。不得包含主机/IP、数据库账号、证书绝对路径、用户目录、完整 URL、server UUID 或凭证。

## 25. Acceptance Criteria for the Approved Scope

当前批准范围完成时必须满足：

1. 本规格已提交并保持 `SPEC READY`。
2. 静态评估覆盖三个初始候选及其传递依赖。
3. 选出第一原型候选，或用明确硬门槛证据淘汰后继续下一候选。
4. 独立原型不进入默认 workspace，不修改生产 Reader。
5. 原型依赖树没有 `openssl` 或 `openssl-sys`。
6. Windows MSVC 上完成可用环境允许的真实测试。
7. MySQL 8.0 正式测试结果和 MySQL 5.6 兼容性结果如实记录。
8. 缺少的 MySQL 5.7、8.4、Linux 和其他平台证据标记为 `not_run`。
9. 证书只通过路径配置，执行代理不读取其内容。
10. 测试只修改当前 run 创建的数据库对象，不修改服务器级状态。
11. 性能、内存、背压和恢复结果进入脱敏报告。
12. 生产迁移、正式 fork 和删除 OpenSSL 依赖均未在没有新批准的情况下执行。

## 26. Production Migration Gate

只有以下条件全部满足，才能请求开始生产迁移：

1. MySQL 5.7、8.0、8.4 正式硬门槛全部实际运行通过。
2. Windows MSVC 与 Linux x86_64 完整集成测试通过。
3. Windows GNU、Linux ARM64、macOS Apple Silicon、MUSL 编译验证通过。
4. TLS、GTID、file/position、事务、恢复、认证和常用类型通过。
5. 候选依赖树完全没有 OpenSSL。
6. 性能和资源门槛通过。
7. 许可证和 MSRV 满足要求。
8. 原型报告列出并解决所有 `blocked` 项。
9. 用户审阅报告并明确批准生产迁移。

在这些条件完成前，候选只能被描述为暂定或有条件推荐。
