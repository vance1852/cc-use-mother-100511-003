# 智能体隔离运行账本

本项目提供人工智能治理协作的服务端基础能力，用于登记主体、任务、权限、风险事件和结构化证据，并通过角色权限、请求幂等、SQLite 事务与审计链保持业务状态一致。

在基础登记能力之上，项目提供**自主任务运行账本（Run Ledger）**：离线可追溯地记录任务提交、
资源租约、每个动作的输入输出摘要、隔离边界变化与人工暂停/恢复，并用每任务一条的哈希链加
交叉校验发现缺失、乱序或被篡改的事件。

## 运行账本能力

- **任务提交**：`idempotency_key` 标识同一任务；相同任务的重试返回同一任务，绝不产生两份
  有效结果。提交时声明初始隔离边界与初始资源租约。
- **资源租约**：`read / write / exclusive` 三种模式，read 可共享、其余互斥；动作只能访问
  被本任务有效租约覆盖的资源。完成时释放、失败时撤销。
- **动作双阶段**：`start-step` 记录输入摘要，`confirm-step` 记录输出摘要并推进检查点；
  只有已确认步骤才会被恢复承认。重复确认相同输出幂等返回，不同输出被拒绝。
- **中断恢复**：执行器崩溃可登记 `interrupt`；恢复开启新的尝试（attempt），执行方从
  “最后一个已确认步骤”继续，未确认步骤通过 `step.redriven` 留痕重放，不分配新步骤号。
- **人工暂停**：`pause` 记录暂停人；暂停期间禁止新步骤。解除人工暂停必须由 reviewer/admin
  执行，任务执行人不能自行放行；崩溃中断则 operator 也可恢复。
- **隔离边界**：`boundary.entered / boundary.changed` 形成有序轨迹，乱序迁移在校验时暴露。
- **执行链还原**：按任务、资源、操作者三种视角返回事件链，每条链都带独立校验结论；
  账本事件同时镜像进全局 `audit_events`，两链在同一事务内成立。
- **篡改发现**：`/ledger/verify` 重放哈希链、序号连续性、生命周期、租约/步骤状态表交叉核对，
  输出具体异常编码（`hash_broken`、`seq_gap_or_reorder`、`checkpoint_mismatch` 等）。

### HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/ledger/tasks` | 提交任务（幂等） |
| POST | `/ledger/grant-lease` `/ledger/release-lease` | 授予/释放资源租约 |
| GET | `/ledger/leases` | 按任务/资源查询租约 |
| POST | `/ledger/start-step` `/ledger/confirm-step` | 动作开始/确认 |
| POST | `/ledger/change-boundary` | 切换隔离边界 |
| POST | `/ledger/pause` `/ledger/interrupt` `/ledger/resume` | 人工暂停/中断/恢复 |
| POST | `/ledger/complete` `/ledger/fail` | 完成（固化唯一结果）/失败 |
| GET | `/ledger/checkpoint` | 恢复检查点与待重放步骤 |
| GET | `/ledger/chain/task` `/ledger/chain/resource` `/ledger/chain/actor` | 三维执行链还原 |
| GET | `/ledger/verify` | 全量校验 |

写操作均通过 `X-Actor-Id` 头标识操作者、通过 `request_id` 保证请求幂等。

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## 构建检查

```bash
python3 -m compileall -q src tests
```

## 离线验收

基础登记链：

```bash
PYTHONPATH=src python3 -m ai_governance_foundation.acceptance
```

运行账本链（提交 → 步骤确认 → 崩溃中断 → 检查点恢复重放 → 边界切换 → 人工暂停/恢复 →
唯一结果固化 → 三维执行链还原 → 删除中段事件后必须检出缺失与哈希断裂）：

```bash
PYTHONPATH=src python3 -m ai_governance_foundation.ledger_acceptance
```

## HTTP 服务

```bash
PYTHONPATH=src python3 -m ai_governance_foundation.api --database ai_governance.sqlite3 --host 127.0.0.1 --port 8080
```

服务提供健康检查和带操作者身份的业务请求，重启后 SQLite 中的状态与审计历史继续保留。
`/health` 同时报告全局审计链与运行账本链的校验结果。
