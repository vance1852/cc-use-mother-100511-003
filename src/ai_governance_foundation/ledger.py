"""实现自主任务的离线可追溯运行账本。

账本中的每个任务拥有一条独立的哈希链（ledger_events），同时镜像进全局
审计链（audit_events）。事件按任务内单调递增的 seq 排列，任何缺失、乱序或
篡改都会在校验时暴露。步骤采用“开始/确认”两阶段：只有确认过的步骤才推进
恢复检查点，任务中断后恢复时从上一个已确认步骤继续重放。
"""

from __future__ import annotations

import json
import re
import uuid
from typing import Any, Callable

from .audit import GENESIS_HASH, append_event, canonical_json, digest
from .clock import Clock, SystemClock
from .errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from .models import Lease, LedgerTask, StepRecord
from .storage import Database

REQUEST_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}$")

LEDGER_EVENT_TYPES = frozenset({
    "task.submitted", "task.paused", "task.interrupted", "task.resumed",
    "task.completed", "task.failed",
    "attempt.started", "attempt.succeeded", "attempt.abandoned",
    "lease.granted", "lease.released",
    "boundary.entered", "boundary.changed",
    "step.started", "step.redriven", "step.confirmed",
})

LEASE_MODES = frozenset({"read", "write", "exclusive"})
WRITER_ROLES = frozenset({"admin", "operator"})
HUMAN_CONTROL_ROLES = frozenset({"admin", "operator", "reviewer"})
TERMINAL_STATUSES = frozenset({"completed", "failed"})

# 租约模式之间的冲突矩阵：read 可与 read 共存，其余互斥。
def _modes_conflict(existing: str, requested: str) -> bool:
    return existing != "read" or requested != "read"


class LedgerService:
    """协调任务提交、租约、动作步骤、隔离边界与人工干预的记账规则。"""

    def __init__(self, database: Database, clock: Clock | None = None) -> None:
        self.database = database
        self.clock = clock or SystemClock()

    # ------------------------------------------------------------------ 基础工具

    def _now(self) -> str:
        return self.clock.now().isoformat().replace("+00:00", "Z")

    def _actor(self, connection, actor_id: str):
        row = connection.execute("SELECT * FROM actors WHERE actor_id=?", (actor_id,)).fetchone()
        if row is None:
            raise NotFoundError("操作者不存在")
        if not row["active"]:
            raise PermissionDenied("操作者已停用")
        return row

    def _require(self, actor, *roles: str) -> None:
        if actor["role"] not in roles:
            raise PermissionDenied("当前角色不能执行该动作")

    def _task_row(self, connection, task_id: str):
        row = connection.execute("SELECT * FROM ledger_tasks WHERE task_id=?", (task_id,)).fetchone()
        if row is None:
            raise NotFoundError("任务不存在")
        return row

    def _idempotent(self, connection, *, request_id: str, action: str, payload: dict[str, Any],
                    create: Callable[[], tuple[str, str, dict[str, Any]]]) -> tuple[bool, dict[str, Any]]:
        """复用全局 request_receipts 实现请求级幂等，返回 (是否重放, 存档响应)。"""

        request_id = str(request_id).strip()
        if not REQUEST_IDENTIFIER.fullmatch(request_id):
            raise ValidationError("request_id 格式无效")
        payload_hash = digest(payload)
        row = connection.execute(
            "SELECT * FROM request_receipts WHERE request_id=?", (request_id,)
        ).fetchone()
        if row:
            if row["action"] != action or row["payload_hash"] != payload_hash:
                raise ConflictError("request_id 已被不同内容使用")
            return True, json.loads(row["response_json"])
        resource_type, resource_id, response = create()
        connection.execute(
            "INSERT INTO request_receipts(request_id,action,payload_hash,resource_type,resource_id,"
            "response_json,created_at) VALUES(?,?,?,?,?,?,?)",
            (request_id, action, payload_hash, resource_type, resource_id,
             canonical_json(response), self._now()),
        )
        return False, response

    def _next_seq(self, connection, task_id: str) -> int:
        row = connection.execute(
            "SELECT COALESCE(MAX(seq),0) AS m FROM ledger_events WHERE task_id=?", (task_id,)
        ).fetchone()
        return row["m"] + 1

    def _append(self, connection, *, task_id: str, event_type: str, actor_id: str,
                detail: dict[str, Any], resource_id: str | None = None,
                step_no: int | None = None, input_hash: str | None = None,
                output_hash: str | None = None) -> dict[str, Any]:
        """追加任务链事件，并镜像一条事件进全局审计链。"""

        if event_type not in LEDGER_EVENT_TYPES:
            raise ValidationError("未知账本事件类型")
        seq = self._next_seq(connection, task_id)
        occurred_at = self._now()
        event_id = uuid.uuid4().hex
        row = connection.execute(
            "SELECT event_hash FROM ledger_events WHERE task_id=? ORDER BY seq DESC LIMIT 1",
            (task_id,),
        ).fetchone()
        previous_hash = row["event_hash"] if row else GENESIS_HASH
        material = {
            "event_id": event_id, "task_id": task_id, "seq": seq,
            "event_type": event_type, "actor_id": actor_id, "resource_id": resource_id,
            "step_no": step_no, "detail": detail, "input_hash": input_hash,
            "output_hash": output_hash, "previous_hash": previous_hash, "occurred_at": occurred_at,
        }
        event_hash = digest(material)
        connection.execute(
            "INSERT INTO ledger_events(event_id,task_id,seq,event_type,actor_id,resource_id,step_no,"
            "detail_json,input_hash,output_hash,previous_hash,event_hash,occurred_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (event_id, task_id, seq, event_type, actor_id, resource_id, step_no,
             canonical_json(detail), input_hash, output_hash, previous_hash, event_hash, occurred_at),
        )
        # 镜像进全局审计链，使两类链在同一事务内同时成立。
        append_event(connection, actor_id=actor_id, action=f"ledger.{event_type}",
                     resource_type="ledger_task", resource_id=task_id,
                     detail={"ledger_event_id": event_id, "seq": seq, "resource_id": resource_id,
                             "step_no": step_no, "input_hash": input_hash, "output_hash": output_hash},
                     occurred_at=occurred_at)
        return {**material, "event_hash": event_hash}

    def _touch(self, connection, task_id: str) -> None:
        connection.execute(
            "UPDATE ledger_tasks SET updated_at=? WHERE task_id=?", (self._now(), task_id)
        )

    def _abandon_current_attempt(self, connection, task_id: str, attempt_no: int, reason: str) -> None:
        connection.execute(
            "UPDATE ledger_attempts SET status='abandoned', ended_at=?, reason=? "
            "WHERE task_id=? AND attempt_no=? AND status='running'",
            (self._now(), reason, task_id, attempt_no),
        )

    def _release_active_leases(self, connection, task_id: str, status: str, reason: str,
                               actor_id: str) -> None:
        rows = connection.execute(
            "SELECT * FROM ledger_leases WHERE task_id=? AND status='granted'", (task_id,)
        ).fetchall()
        for row in rows:
            released_at = self._now()
            connection.execute(
                "UPDATE ledger_leases SET status=?, released_at=?, released_by=?, release_reason=? "
                "WHERE lease_id=?",
                (status, released_at, actor_id, reason, row["lease_id"]),
            )
            self._append(connection, task_id=task_id, event_type="lease.released",
                         actor_id=actor_id, resource_id=row["resource_id"],
                         detail={"lease_id": row["lease_id"], "mode": row["mode"],
                                 "release_status": status, "reason": reason})

    @staticmethod
    def _task_model(row: dict[str, Any], replayed: bool = False) -> LedgerTask:
        return LedgerTask(
            task_id=row["task_id"], idempotency_key=row["idempotency_key"], title=row["title"],
            status=row["status"], submitted_by=row["submitted_by"],
            current_boundary=row["current_boundary"], last_step_no=row["last_step_no"],
            last_step_key=row["last_step_key"], last_output_hash=row["last_output_hash"],
            attempt_no=row["attempt_no"], result_hash=row["result_hash"],
            created_at=row["created_at"], updated_at=row["updated_at"], replayed=replayed,
        )

    # ------------------------------------------------------------------ 任务提交

    def submit_task(self, *, request_id: str, actor_id: str, idempotency_key: str, title: str,
                    payload: dict[str, Any], boundary: str,
                    resources: list[dict[str, str]] | None = None,
                    task_id: str | None = None) -> LedgerTask:
        """提交自主任务并登记初始边界与资源租约；相同幂等键只返回同一任务。"""

        resources = resources or []
        payload_body = {"actor_id": actor_id, "idempotency_key": idempotency_key, "title": title,
                        "payload": payload, "boundary": boundary, "resources": resources}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, *WRITER_ROLES)
            idempotency_key = str(idempotency_key).strip()
            if not REQUEST_IDENTIFIER.fullmatch(idempotency_key):
                raise ValidationError("idempotency_key 格式无效")
            title = str(title).strip()
            if not title or len(title) > 200:
                raise ValidationError("title 不能为空且不能超过 200 个字符")
            boundary = str(boundary).strip()
            if not boundary or len(boundary) > 120:
                raise ValidationError("boundary 不能为空")
            if not isinstance(payload, dict):
                raise ValidationError("payload 必须是对象")
            payload_hash = digest(payload)
            if not isinstance(resources, list) or not resources:
                raise ValidationError("resources 至少包含一个资源租约")
            normalized: list[tuple[str, str]] = []
            seen: set[str] = set()
            for item in resources:
                resource_id = str(item.get("resource_id", "")).strip()
                mode = str(item.get("mode", "")).strip()
                if not resource_id or mode not in LEASE_MODES:
                    raise ValidationError("资源项必须包含 resource_id 与合法 mode")
                if resource_id in seen:
                    raise ValidationError(f"资源 {resource_id} 重复登记")
                seen.add(resource_id)
                normalized.append((resource_id, mode))
            normalized.sort()
            submission_hash = digest({"payload_hash": payload_hash, "boundary": boundary,
                                      "resources": normalized})

            def create() -> tuple[str, str, dict[str, Any]]:
                existing = connection.execute(
                    "SELECT * FROM ledger_tasks WHERE idempotency_key=?", (idempotency_key,)
                ).fetchone()
                if existing is not None:
                    if (existing["payload_hash"] != payload_hash or existing["title"] != title
                            or existing["submission_hash"] != submission_hash):
                        raise ConflictError("幂等键已绑定不同的任务内容、边界或资源集合")
                    response = self._task_model(existing, replayed=True).__dict__
                    return "ledger_task", existing["task_id"], response
                new_task_id = task_id or uuid.uuid4().hex
                now = self._now()
                # 先完成所有冲突检查并预生成租约编号，避免追加事件后再回写详情。
                for resource_id, mode in normalized:
                    self._ensure_resource_free(connection, resource_id, mode, new_task_id)
                prepared = [(uuid.uuid4().hex, resource_id, mode) for resource_id, mode in normalized]
                try:
                    connection.execute(
                        "INSERT INTO ledger_tasks(task_id,idempotency_key,title,payload_hash,"
                        "submission_hash,status,submitted_by,current_boundary,last_step_no,"
                        "attempt_no,created_at,updated_at) "
                        "VALUES(?,?,?,?,?,'running',?,?,0,1,?,?)",
                        (new_task_id, idempotency_key, title, payload_hash, submission_hash,
                         actor_id, boundary, now, now),
                    )
                except Exception as exc:
                    raise ConflictError("任务编号或幂等键已经存在") from exc
                connection.execute(
                    "INSERT INTO ledger_attempts(attempt_id,task_id,attempt_no,triggered_by,reason,"
                    "status,started_at) VALUES(?,?,1,?,?,'running',?)",
                    (uuid.uuid4().hex, new_task_id, actor_id, "submit", now),
                )
                resource_refs = [{"resource_id": resource_id, "mode": mode, "lease_id": lease_id}
                                 for lease_id, resource_id, mode in prepared]
                self._append(connection, task_id=new_task_id, event_type="task.submitted",
                             actor_id=actor_id,
                             detail={"title": title, "payload_hash": payload_hash,
                                     "boundary": boundary, "resources": resource_refs})
                self._append(connection, task_id=new_task_id, event_type="attempt.started",
                             actor_id=actor_id, detail={"attempt_no": 1, "reason": "submit"})
                self._append(connection, task_id=new_task_id, event_type="boundary.entered",
                             actor_id=actor_id, detail={"boundary": boundary})
                for lease_id, resource_id, mode in prepared:
                    connection.execute(
                        "INSERT INTO ledger_leases(lease_id,task_id,resource_id,mode,status,"
                        "granted_by,granted_at) VALUES(?,?,?,?,'granted',?,?)",
                        (lease_id, new_task_id, resource_id, mode, actor_id, now),
                    )
                    self._append(connection, task_id=new_task_id, event_type="lease.granted",
                                 actor_id=actor_id, resource_id=resource_id,
                                 detail={"lease_id": lease_id, "mode": mode})
                row = self._task_row(connection, new_task_id)
                return "ledger_task", new_task_id, self._task_model(row).__dict__

            replayed, response = self._idempotent(
                connection, request_id=request_id, action="ledger.submit_task",
                payload=payload_body, create=create,
            )
            return self._task_model(response, replayed=replayed or response.get("replayed", False))

    def _ensure_resource_free(self, connection, resource_id: str, mode: str, task_id: str) -> None:
        rows = connection.execute(
            "SELECT task_id, mode FROM ledger_leases WHERE resource_id=? AND status='granted'",
            (resource_id,),
        ).fetchall()
        for row in rows:
            if row["task_id"] == task_id:
                raise ConflictError(f"任务已持有资源 {resource_id} 的租约")
            if _modes_conflict(row["mode"], mode):
                raise ConflictError(f"资源 {resource_id} 已被任务 {row['task_id']} 以 {row['mode']} 模式占用")

    # ------------------------------------------------------------------ 租约管理

    def grant_lease(self, *, request_id: str, actor_id: str, task_id: str,
                    resource_id: str, mode: str) -> Lease:
        payload = {"actor_id": actor_id, "task_id": task_id, "resource_id": resource_id, "mode": mode}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, *WRITER_ROLES)
            task = self._task_row(connection, task_id)
            if task["status"] in TERMINAL_STATUSES:
                raise ConflictError("任务已结束，不能再获取租约")
            if mode not in LEASE_MODES:
                raise ValidationError("mode 必须是 read/write/exclusive")

            def create() -> tuple[str, str, dict[str, Any]]:
                self._ensure_resource_free(connection, resource_id, mode, task_id)
                lease_id = uuid.uuid4().hex
                now = self._now()
                connection.execute(
                    "INSERT INTO ledger_leases(lease_id,task_id,resource_id,mode,status,granted_by,granted_at) "
                    "VALUES(?,?,?,?,'granted',?,?)",
                    (lease_id, task_id, resource_id, mode, actor_id, now),
                )
                self._append(connection, task_id=task_id, event_type="lease.granted",
                             actor_id=actor_id, resource_id=resource_id,
                             detail={"lease_id": lease_id, "mode": mode})
                self._touch(connection, task_id)
                response = {"lease_id": lease_id, "task_id": task_id, "resource_id": resource_id,
                            "mode": mode, "status": "granted", "granted_by": actor_id, "granted_at": now,
                            "released_at": None, "released_by": None, "release_reason": None}
                return "ledger_lease", lease_id, response

            replayed, response = self._idempotent(
                connection, request_id=request_id, action="ledger.grant_lease",
                payload=payload, create=create,
            )
            return Lease(**response, replayed=replayed)

    def release_lease(self, *, request_id: str, actor_id: str, task_id: str,
                      resource_id: str, reason: str) -> Lease:
        payload = {"actor_id": actor_id, "task_id": task_id, "resource_id": resource_id, "reason": reason}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, *WRITER_ROLES)
            self._task_row(connection, task_id)
            reason = str(reason).strip()
            if not reason:
                raise ValidationError("reason 不能为空")

            def create() -> tuple[str, str, dict[str, Any]]:
                row = connection.execute(
                    "SELECT * FROM ledger_leases WHERE task_id=? AND resource_id=? AND status='granted'",
                    (task_id, resource_id),
                ).fetchone()
                if row is None:
                    raise ConflictError("该任务没有持有此资源的有效租约")
                now = self._now()
                connection.execute(
                    "UPDATE ledger_leases SET status='released', released_at=?, released_by=?, release_reason=? "
                    "WHERE lease_id=?",
                    (now, actor_id, reason, row["lease_id"]),
                )
                self._append(connection, task_id=task_id, event_type="lease.released",
                             actor_id=actor_id, resource_id=resource_id,
                             detail={"lease_id": row["lease_id"], "mode": row["mode"],
                                     "release_status": "released", "reason": reason})
                self._touch(connection, task_id)
                return "ledger_lease", row["lease_id"], self._lease_dict(
                    row, status="released", released_at=now, released_by=actor_id, release_reason=reason)

            replayed, response = self._idempotent(
                connection, request_id=request_id, action="ledger.release_lease",
                payload=payload, create=create,
            )
            return Lease(**response, replayed=replayed)

    @staticmethod
    def _lease_dict(row, *, status: str | None = None, released_at: str | None = None,
                    released_by: str | None = None, release_reason: str | None = None) -> dict[str, Any]:
        return {
            "lease_id": row["lease_id"], "task_id": row["task_id"],
            "resource_id": row["resource_id"], "mode": row["mode"],
            "status": status or row["status"], "granted_by": row["granted_by"],
            "granted_at": row["granted_at"],
            "released_at": released_at if released_at is not None else row["released_at"],
            "released_by": released_by if released_by is not None else row["released_by"],
            "release_reason": release_reason if release_reason is not None else row["release_reason"],
        }

    # ------------------------------------------------------------------ 动作步骤

    @staticmethod
    def _summary(value: Any) -> tuple[str, str]:
        text = canonical_json(value)
        return digest(value), f"{len(text.encode('utf-8'))}B"

    def start_step(self, *, request_id: str, actor_id: str, task_id: str, step_key: str,
                   action_type: str, inputs: Any, resource_id: str | None = None) -> StepRecord:
        """记录一个动作的开始；崩溃恢复时重复调用表示重放未确认步骤。"""

        payload = {"actor_id": actor_id, "task_id": task_id, "step_key": step_key,
                   "action_type": action_type, "inputs": inputs, "resource_id": resource_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, *WRITER_ROLES)
            task = self._task_row(connection, task_id)
            if task["status"] != "running":
                raise ConflictError(f"任务当前状态为 {task['status']}，不能开始步骤")
            step_key = str(step_key).strip()
            action_type = str(action_type).strip()
            if not step_key or not action_type:
                raise ValidationError("step_key 与 action_type 不能为空")
            input_hash, input_size = self._summary(inputs)
            if resource_id is not None:
                held = connection.execute(
                    "SELECT 1 FROM ledger_leases WHERE task_id=? AND resource_id=? AND status='granted'",
                    (task_id, resource_id),
                ).fetchone()
                if held is None:
                    raise PermissionDenied("动作引用的资源未被任务有效租约覆盖")

            existing = connection.execute(
                "SELECT * FROM ledger_steps WHERE task_id=? AND step_key=?", (task_id, step_key)
            ).fetchone()
            if existing is not None and existing["input_hash"] != input_hash:
                raise ConflictError("步骤键已存在但输入摘要不同")
            if existing is not None and existing["status"] == "confirmed":
                # 已确认步骤的重试：幂等返回原结果，绝不产生第二份输出。
                return StepRecord(task_id, existing["step_no"], step_key, existing["action_type"],
                                  input_hash, existing["output_hash"], "confirmed",
                                  existing["started_at"], existing["confirmed_at"],
                                  replayed=True)

            def create() -> tuple[str, str, dict[str, Any]]:
                now = self._now()
                if existing is not None:
                    # 未确认步骤的重放：不分配新步骤号，追加 step.redriven 留痕。
                    step_no = existing["step_no"]
                    self._append(connection, task_id=task_id, event_type="step.redriven",
                                 actor_id=actor_id, step_no=step_no, resource_id=resource_id,
                                 detail={"step_key": step_key, "action_type": action_type,
                                         "attempt_no": task["attempt_no"]},
                                 input_hash=input_hash)
                    self._touch(connection, task_id)
                    return "ledger_step", f"{task_id}:{step_no}", {
                        "task_id": task_id, "step_no": step_no, "step_key": step_key,
                        "action_type": action_type, "input_hash": input_hash, "output_hash": None,
                        "status": "started", "started_at": existing["started_at"],
                        "confirmed_at": None, "replayed": True, "redriven": True,
                    }
                max_row = connection.execute(
                    "SELECT step_no, status FROM ledger_steps WHERE task_id=? ORDER BY step_no DESC LIMIT 1",
                    (task_id,),
                ).fetchone()
                if max_row is not None and max_row["status"] == "started":
                    raise ConflictError("上一个步骤尚未确认，不能跳过它开始新步骤")
                step_no = (max_row["step_no"] + 1) if max_row else 1
                connection.execute(
                    "INSERT INTO ledger_steps(task_id,step_no,step_key,action_type,input_hash,status,"
                    "started_at) VALUES(?,?,?,?,?,'started',?)",
                    (task_id, step_no, step_key, action_type, input_hash, now),
                )
                self._append(connection, task_id=task_id, event_type="step.started",
                             actor_id=actor_id, step_no=step_no, resource_id=resource_id,
                             detail={"step_key": step_key, "action_type": action_type,
                                     "input_bytes": input_size}, input_hash=input_hash)
                self._touch(connection, task_id)
                return "ledger_step", f"{task_id}:{step_no}", {
                    "task_id": task_id, "step_no": step_no, "step_key": step_key,
                    "action_type": action_type, "input_hash": input_hash, "output_hash": None,
                    "status": "started", "started_at": now, "confirmed_at": None,
                }

            replayed, response = self._idempotent(
                connection, request_id=request_id, action="ledger.start_step",
                payload=payload, create=create,
            )
            response.pop("replayed", None)
            response.pop("redriven", None)
            is_redrive = existing is not None
            return StepRecord(**response, replayed=replayed or is_redrive, redriven=is_redrive)

    def confirm_step(self, *, request_id: str, actor_id: str, task_id: str,
                     step_key: str, outputs: Any) -> StepRecord:
        """确认动作输出并推进检查点；同一步骤的重复确认必须提交相同输出。"""

        payload = {"actor_id": actor_id, "task_id": task_id, "step_key": step_key, "outputs": outputs}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, *WRITER_ROLES)
            task = self._task_row(connection, task_id)
            if task["status"] != "running":
                raise ConflictError(f"任务当前状态为 {task['status']}，不能确认步骤；请先恢复任务")
            output_hash, output_size = self._summary(outputs)
            step = connection.execute(
                "SELECT * FROM ledger_steps WHERE task_id=? AND step_key=?", (task_id, step_key)
            ).fetchone()
            if step is None:
                raise NotFoundError("步骤尚未开始")
            if step["status"] == "confirmed":
                if step["output_hash"] != output_hash:
                    raise ConflictError("步骤已确认，但本次输出摘要与已确认结果不同")
                return StepRecord(task_id, step["step_no"], step_key, step["action_type"],
                                  step["input_hash"], step["output_hash"], "confirmed",
                                  step["started_at"], step["confirmed_at"], replayed=True)

            def create() -> tuple[str, str, dict[str, Any]]:
                now = self._now()
                connection.execute(
                    "UPDATE ledger_steps SET status='confirmed', output_hash=?, confirmed_at=? "
                    "WHERE task_id=? AND step_key=?",
                    (output_hash, now, task_id, step_key),
                )
                connection.execute(
                    "UPDATE ledger_tasks SET last_step_no=?, last_step_key=?, last_output_hash=?, updated_at=? "
                    "WHERE task_id=?",
                    (step["step_no"], step_key, output_hash, now, task_id),
                )
                self._append(connection, task_id=task_id, event_type="step.confirmed",
                             actor_id=actor_id, step_no=step["step_no"],
                             detail={"step_key": step_key, "action_type": step["action_type"],
                                     "output_bytes": output_size},
                             input_hash=step["input_hash"], output_hash=output_hash)
                return "ledger_step", f"{task_id}:{step['step_no']}", {
                    "task_id": task_id, "step_no": step["step_no"], "step_key": step_key,
                    "action_type": step["action_type"], "input_hash": step["input_hash"],
                    "output_hash": output_hash, "status": "confirmed",
                    "started_at": step["started_at"], "confirmed_at": now,
                }

            _, response = self._idempotent(
                connection, request_id=request_id, action="ledger.confirm_step",
                payload=payload, create=create,
            )
            return StepRecord(**response, replayed=response.get("replayed", False))

    # ------------------------------------------------------------------ 隔离边界

    def change_boundary(self, *, request_id: str, actor_id: str, task_id: str,
                        new_boundary: str, reason: str) -> LedgerTask:
        payload = {"actor_id": actor_id, "task_id": task_id, "new_boundary": new_boundary,
                   "reason": reason}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, *WRITER_ROLES)
            task = self._task_row(connection, task_id)
            if task["status"] != "running":
                raise ConflictError("只有运行中的任务可以切换隔离边界")
            new_boundary = str(new_boundary).strip()
            reason = str(reason).strip()
            if not new_boundary or not reason:
                raise ValidationError("new_boundary 与 reason 不能为空")

            def create() -> tuple[str, str, dict[str, Any]]:
                self._append(connection, task_id=task_id, event_type="boundary.changed",
                             actor_id=actor_id,
                             detail={"from": task["current_boundary"], "to": new_boundary,
                                     "reason": reason})
                connection.execute(
                    "UPDATE ledger_tasks SET current_boundary=?, updated_at=? WHERE task_id=?",
                    (new_boundary, self._now(), task_id),
                )
                row = self._task_row(connection, task_id)
                return "ledger_task", task_id, self._task_model(row).__dict__

            replayed, response = self._idempotent(
                connection, request_id=request_id, action="ledger.change_boundary",
                payload=payload, create=create,
            )
            return self._task_model(response, replayed=replayed)

    # ------------------------------------------------------------------ 人工干预

    def pause_task(self, *, request_id: str, actor_id: str, task_id: str, reason: str) -> LedgerTask:
        """人工暂停运行中的任务。"""

        payload = {"actor_id": actor_id, "task_id": task_id, "reason": reason}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, *HUMAN_CONTROL_ROLES)
            task = self._task_row(connection, task_id)
            reason = str(reason).strip()
            if not reason:
                raise ValidationError("reason 不能为空")
            if task["status"] != "running":
                raise ConflictError(f"只有运行中的任务可以暂停，当前状态 {task['status']}")

            def create() -> tuple[str, str, dict[str, Any]]:
                now = self._now()
                self._append(connection, task_id=task_id, event_type="task.paused",
                             actor_id=actor_id, detail={"reason": reason})
                self._abandon_current_attempt(connection, task_id, task["attempt_no"], "task.paused")
                self._append(connection, task_id=task_id, event_type="attempt.abandoned",
                             actor_id=actor_id,
                             detail={"attempt_no": task["attempt_no"], "reason": "task.paused"})
                connection.execute(
                    "UPDATE ledger_tasks SET status='paused', updated_at=? WHERE task_id=?",
                    (now, task_id),
                )
                row = self._task_row(connection, task_id)
                return "ledger_task", task_id, self._task_model(row).__dict__

            replayed, response = self._idempotent(
                connection, request_id=request_id, action="ledger.pause_task",
                payload=payload, create=create,
            )
            return self._task_model(response, replayed=replayed)

    def mark_interrupted(self, *, request_id: str, actor_id: str, task_id: str, reason: str) -> LedgerTask:
        """登记执行器崩溃等非人工中断；租约保留，等待恢复。"""

        payload = {"actor_id": actor_id, "task_id": task_id, "reason": reason}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, *WRITER_ROLES)
            task = self._task_row(connection, task_id)
            reason = str(reason).strip()
            if not reason:
                raise ValidationError("reason 不能为空")
            if task["status"] != "running":
                raise ConflictError(f"只有运行中的任务可以标记中断，当前状态 {task['status']}")

            def create() -> tuple[str, str, dict[str, Any]]:
                now = self._now()
                self._append(connection, task_id=task_id, event_type="task.interrupted",
                             actor_id=actor_id, detail={"reason": reason})
                self._abandon_current_attempt(connection, task_id, task["attempt_no"], "task.interrupted")
                self._append(connection, task_id=task_id, event_type="attempt.abandoned",
                             actor_id=actor_id,
                             detail={"attempt_no": task["attempt_no"], "reason": "task.interrupted"})
                connection.execute(
                    "UPDATE ledger_tasks SET status='interrupted', updated_at=? WHERE task_id=?",
                    (now, task_id),
                )
                row = self._task_row(connection, task_id)
                return "ledger_task", task_id, self._task_model(row).__dict__

            replayed, response = self._idempotent(
                connection, request_id=request_id, action="ledger.mark_interrupted",
                payload=payload, create=create,
            )
            return self._task_model(response, replayed=replayed)

    def resume_task(self, *, request_id: str, actor_id: str, task_id: str, reason: str) -> LedgerTask:
        """从暂停或中断恢复：开启新尝试，执行方从最后一个已确认步骤继续。

        崩溃中断可由执行角色（operator/admin）恢复；人工暂停的放行必须由
        复核员或管理员执行，执行人不能自行解除人工 hold。
        """

        payload = {"actor_id": actor_id, "task_id": task_id, "reason": reason}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            task = self._task_row(connection, task_id)
            reason = str(reason).strip()
            if not reason:
                raise ValidationError("reason 不能为空")
            if task["status"] == "paused":
                self._require(actor, "admin", "reviewer")
            elif task["status"] == "interrupted":
                self._require(actor, *HUMAN_CONTROL_ROLES)
            else:
                raise ConflictError(f"只有暂停或中断的任务可以恢复，当前状态 {task['status']}")

            def create() -> tuple[str, str, dict[str, Any]]:
                now = self._now()
                new_attempt_no = task["attempt_no"] + 1
                self._append(connection, task_id=task_id, event_type="task.resumed",
                             actor_id=actor_id,
                             detail={"reason": reason, "from_status": task["status"],
                                     "checkpoint_step_no": task["last_step_no"],
                                     "checkpoint_step_key": task["last_step_key"],
                                     "attempt_no": new_attempt_no})
                connection.execute(
                    "INSERT INTO ledger_attempts(attempt_id,task_id,attempt_no,triggered_by,reason,"
                    "status,started_at) VALUES(?,?,?,?,?,'running',?)",
                    (uuid.uuid4().hex, task_id, new_attempt_no, actor_id, reason, now),
                )
                self._append(connection, task_id=task_id, event_type="attempt.started",
                             actor_id=actor_id,
                             detail={"attempt_no": new_attempt_no, "reason": reason})
                connection.execute(
                    "UPDATE ledger_tasks SET status='running', attempt_no=?, updated_at=? WHERE task_id=?",
                    (new_attempt_no, now, task_id),
                )
                row = self._task_row(connection, task_id)
                return "ledger_task", task_id, self._task_model(row).__dict__

            replayed, response = self._idempotent(
                connection, request_id=request_id, action="ledger.resume_task",
                payload=payload, create=create,
            )
            return self._task_model(response, replayed=replayed)

    def complete_task(self, *, request_id: str, actor_id: str, task_id: str, result: Any) -> LedgerTask:
        """完成任务并固化唯一有效结果；重放完成请求必须提交相同结果。"""

        payload = {"actor_id": actor_id, "task_id": task_id, "result": result}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, *WRITER_ROLES)
            task = self._task_row(connection, task_id)
            result_hash, result_size = self._summary(result)
            if task["status"] == "completed":
                if task["result_hash"] != result_hash:
                    raise ConflictError("任务已完成并固化了不同的结果，不能制造第二份有效结果")
                return self._task_model(task, replayed=True)
            if task["status"] != "running":
                raise ConflictError(f"任务当前状态为 {task['status']}，不能完成")

            def create() -> tuple[str, str, dict[str, Any]]:
                unconfirmed = connection.execute(
                    "SELECT step_key FROM ledger_steps WHERE task_id=? AND status='started'", (task_id,)
                ).fetchall()
                if unconfirmed:
                    raise ConflictError("仍存在未确认步骤，不能完成任务")
                now = self._now()
                self._release_active_leases(connection, task_id, "released", "task.completed", actor_id)
                connection.execute(
                    "UPDATE ledger_attempts SET status='succeeded', ended_at=? "
                    "WHERE task_id=? AND attempt_no=? AND status='running'",
                    (now, task_id, task["attempt_no"]),
                )
                self._append(connection, task_id=task_id, event_type="attempt.succeeded",
                             actor_id=actor_id, detail={"attempt_no": task["attempt_no"]})
                self._append(connection, task_id=task_id, event_type="task.completed",
                             actor_id=actor_id,
                             detail={"result_hash": result_hash, "result_bytes": result_size},
                             output_hash=result_hash)
                connection.execute(
                    "UPDATE ledger_tasks SET status='completed', result_hash=?, updated_at=? WHERE task_id=?",
                    (result_hash, now, task_id),
                )
                row = self._task_row(connection, task_id)
                return "ledger_task", task_id, self._task_model(row).__dict__

            replayed, response = self._idempotent(
                connection, request_id=request_id, action="ledger.complete_task",
                payload=payload, create=create,
            )
            return self._task_model(response, replayed=replayed)

    def fail_task(self, *, request_id: str, actor_id: str, task_id: str, reason: str) -> LedgerTask:
        payload = {"actor_id": actor_id, "task_id": task_id, "reason": reason}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, *WRITER_ROLES)
            task = self._task_row(connection, task_id)
            reason = str(reason).strip()
            if not reason:
                raise ValidationError("reason 不能为空")
            if task["status"] in TERMINAL_STATUSES:
                raise ConflictError(f"任务已结束（{task['status']}），不能再次标记失败")

            def create() -> tuple[str, str, dict[str, Any]]:
                now = self._now()
                self._release_active_leases(connection, task_id, "revoked", "task.failed", actor_id)
                self._abandon_current_attempt(connection, task_id, task["attempt_no"], "task.failed")
                self._append(connection, task_id=task_id, event_type="attempt.abandoned",
                             actor_id=actor_id,
                             detail={"attempt_no": task["attempt_no"], "reason": "task.failed"})
                self._append(connection, task_id=task_id, event_type="task.failed",
                             actor_id=actor_id, detail={"reason": reason})
                connection.execute(
                    "UPDATE ledger_tasks SET status='failed', updated_at=? WHERE task_id=?",
                    (now, task_id),
                )
                row = self._task_row(connection, task_id)
                return "ledger_task", task_id, self._task_model(row).__dict__

            replayed, response = self._idempotent(
                connection, request_id=request_id, action="ledger.fail_task",
                payload=payload, create=create,
            )
            return self._task_model(response, replayed=replayed)

    # ------------------------------------------------------------------ 查询与校验

    def get_task(self, task_id: str) -> LedgerTask:
        row = self.database.connection.execute(
            "SELECT * FROM ledger_tasks WHERE task_id=?", (task_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError("任务不存在")
        return self._task_model(row)

    def get_checkpoint(self, task_id: str) -> dict[str, Any]:
        """返回恢复执行所需的最小检查点。"""

        task = self.get_task(task_id)
        pending = self.database.connection.execute(
            "SELECT step_no, step_key, action_type, input_hash FROM ledger_steps "
            "WHERE task_id=? AND status='started' ORDER BY step_no", (task_id,)
        ).fetchall()
        return {
            "task_id": task.task_id, "status": task.status, "current_boundary": task.current_boundary,
            "attempt_no": task.attempt_no,
            "last_confirmed_step_no": task.last_step_no,
            "last_confirmed_step_key": task.last_step_key,
            "last_confirmed_output_hash": task.last_output_hash,
            "pending_steps": [dict(row) for row in pending],
        }

    def list_leases(self, *, task_id: str | None = None, resource_id: str | None = None,
                    active_only: bool = False) -> list[dict[str, Any]]:
        sql = "SELECT * FROM ledger_leases WHERE 1=1"
        parameters: list[Any] = []
        if task_id:
            sql += " AND task_id=?"
            parameters.append(task_id)
        if resource_id:
            sql += " AND resource_id=?"
            parameters.append(resource_id)
        if active_only:
            sql += " AND status='granted'"
        sql += " ORDER BY granted_at, lease_id"
        return [dict(row) for row in self.database.connection.execute(sql, parameters)]

    def _event_dicts(self, where: str, parameters: list[Any], order: str) -> list[dict[str, Any]]:
        sql = ("SELECT event_id,task_id,seq,event_type,actor_id,resource_id,step_no,detail_json,"
               "input_hash,output_hash,previous_hash,event_hash,occurred_at FROM ledger_events ")
        rows = self.database.connection.execute(sql + where + " " + order, parameters).fetchall()
        items = []
        for row in rows:
            item = dict(row)
            item["detail"] = json.loads(item.pop("detail_json"))
            items.append(item)
        return items

    def chain_by_task(self, task_id: str) -> dict[str, Any]:
        self.get_task(task_id)  # 不存在时抛出 404
        events = self._event_dicts("WHERE task_id=?", [task_id], "ORDER BY seq")
        verification = self.verify_task(task_id)
        return {"task_id": task_id, "events": events, "verification": verification}

    def chain_by_resource(self, resource_id: str) -> dict[str, Any]:
        events = self._event_dicts("WHERE resource_id=?", [resource_id],
                                   "ORDER BY occurred_at, id")
        task_ids = sorted({event["task_id"] for event in events})
        tasks = [self.verify_task(task_id) for task_id in task_ids]
        return {"resource_id": resource_id, "events": events,
                "involved_tasks": task_ids,
                "all_chains_valid": all(item["valid"] for item in tasks),
                "task_verification": tasks}

    def chain_by_actor(self, actor_id: str) -> dict[str, Any]:
        events = self._event_dicts("WHERE actor_id=?", [actor_id], "ORDER BY occurred_at, id")
        task_ids = sorted({event["task_id"] for event in events})
        tasks = [self.verify_task(task_id) for task_id in task_ids]
        return {"actor_id": actor_id, "events": events,
                "involved_tasks": task_ids,
                "all_chains_valid": all(item["valid"] for item in tasks),
                "task_verification": tasks}

    def verify_task(self, task_id: str) -> dict[str, Any]:
        """重放哈希链、序号连续性、生命周期与状态表交叉核对。"""

        task_row = self.database.connection.execute(
            "SELECT * FROM ledger_tasks WHERE task_id=?", (task_id,)
        ).fetchone()
        anomalies: list[dict[str, Any]] = []
        if task_row is None:
            return {"task_id": task_id, "valid": False, "event_count": 0,
                    "anomalies": [{"code": "task_missing", "message": "任务不存在"}]}
        rows = self.database.connection.execute(
            "SELECT * FROM ledger_events WHERE task_id=? ORDER BY seq", (task_id,)
        ).fetchall()

        previous_hash = GENESIS_HASH
        phase: str | None = None
        boundary: str | None = None
        active_leases: dict[str, dict[str, Any]] = {}
        started_steps: dict[int, dict[str, Any]] = {}
        confirmed_steps: dict[int, dict[str, Any]] = {}
        expected_step_no = 1
        attempt_count = 0

        for position, row in enumerate(rows, start=1):
            ref = {"seq": row["seq"], "event_type": row["event_type"]}
            if row["seq"] != position:
                anomalies.append({**ref, "code": "seq_gap_or_reorder",
                                  "message": f"事件序号应为 {position}，实际为 {row['seq']}（存在缺失或乱序）"})
            detail = json.loads(row["detail_json"])
            material = {
                "event_id": row["event_id"], "task_id": row["task_id"], "seq": row["seq"],
                "event_type": row["event_type"], "actor_id": row["actor_id"],
                "resource_id": row["resource_id"], "step_no": row["step_no"], "detail": detail,
                "input_hash": row["input_hash"], "output_hash": row["output_hash"],
                "previous_hash": row["previous_hash"], "occurred_at": row["occurred_at"],
            }
            if row["previous_hash"] != previous_hash or digest(material) != row["event_hash"]:
                anomalies.append({**ref, "code": "hash_broken", "message": "哈希链校验失败，事件被篡改或缺失"})
            previous_hash = row["event_hash"]
            etype = row["event_type"]
            try:
                if etype == "task.submitted":
                    if phase is not None:
                        anomalies.append({**ref, "code": "lifecycle", "message": "重复的任务提交事件"})
                    phase = "running"
                elif etype == "attempt.started":
                    attempt_count += 1
                    if detail.get("attempt_no") != attempt_count:
                        anomalies.append({**ref, "code": "attempt_out_of_order",
                                          "message": f"尝试序号应为 {attempt_count}"})
                    if phase not in ("running",):
                        anomalies.append({**ref, "code": "lifecycle",
                                          "message": "新尝试只能在运行态开始"})
                elif etype == "attempt.abandoned":
                    if phase not in ("running", "paused", "interrupted"):
                        anomalies.append({**ref, "code": "lifecycle", "message": "尝试弃用发生在非法状态"})
                elif etype == "attempt.succeeded":
                    if phase != "running":
                        anomalies.append({**ref, "code": "lifecycle", "message": "尝试只能在运行态成功"})
                elif etype == "lease.granted":
                    resource_id = row["resource_id"]
                    if resource_id in active_leases:
                        anomalies.append({**ref, "code": "lease_conflict",
                                          "message": f"资源 {resource_id} 存在未释放的重复授予"})
                    active_leases[resource_id] = {"mode": detail.get("mode"), "seq": row["seq"]}
                elif etype == "lease.released":
                    resource_id = row["resource_id"]
                    if resource_id not in active_leases:
                        anomalies.append({**ref, "code": "lease_without_grant",
                                          "message": f"资源 {resource_id} 的释放事件没有对应授予"})
                    else:
                        active_leases.pop(resource_id)
                elif etype == "boundary.entered":
                    if boundary is not None:
                        anomalies.append({**ref, "code": "boundary", "message": "初始边界重复进入"})
                    boundary = detail.get("boundary")
                elif etype == "boundary.changed":
                    if detail.get("from") != boundary:
                        anomalies.append({**ref, "code": "boundary_out_of_order",
                                          "message": "边界变化的起点与当前边界不一致"})
                    boundary = detail.get("to")
                elif etype == "step.started":
                    step_no = row["step_no"]
                    if step_no != expected_step_no:
                        anomalies.append({**ref, "code": "step_out_of_order",
                                          "message": f"步骤号应为 {expected_step_no}，实际为 {step_no}"})
                    if step_no in started_steps:
                        anomalies.append({**ref, "code": "step_duplicate", "message": "步骤重复开始"})
                    started_steps[step_no] = {"input_hash": row["input_hash"], "detail": detail}
                elif etype == "step.redriven":
                    step_no = row["step_no"]
                    started = started_steps.get(step_no)
                    if started is None:
                        anomalies.append({**ref, "code": "redrive_without_pending_step",
                                          "message": "重放事件对应的步骤不存在或已确认"})
                    elif started["input_hash"] != row["input_hash"]:
                        anomalies.append({**ref, "code": "redrive_input_changed",
                                          "message": "重放时的输入摘要与开始时不一致"})
                elif etype == "step.confirmed":
                    step_no = row["step_no"]
                    if step_no not in started_steps:
                        anomalies.append({**ref, "code": "step_without_start",
                                          "message": "确认事件没有对应的开始事件（事件缺失）"})
                    elif not row["output_hash"]:
                        anomalies.append({**ref, "code": "step_missing_output",
                                          "message": "确认事件缺少输出摘要"})
                    else:
                        confirmed_steps[step_no] = {"output_hash": row["output_hash"]}
                        started_steps.pop(step_no, None)
                        expected_step_no = step_no + 1
                elif etype == "task.paused":
                    if phase != "running":
                        anomalies.append({**ref, "code": "lifecycle", "message": "暂停只能发生在运行态"})
                    phase = "paused"
                elif etype == "task.interrupted":
                    if phase != "running":
                        anomalies.append({**ref, "code": "lifecycle", "message": "中断只能发生在运行态"})
                    phase = "interrupted"
                elif etype == "task.resumed":
                    if phase not in ("paused", "interrupted"):
                        anomalies.append({**ref, "code": "lifecycle", "message": "恢复只能发生在暂停或中断态"})
                    phase = "running"
                elif etype == "task.completed":
                    if phase != "running":
                        anomalies.append({**ref, "code": "lifecycle", "message": "完成只能发生在运行态"})
                    if started_steps:
                        anomalies.append({**ref, "code": "unconfirmed_at_completion",
                                          "message": "任务完成时仍有未确认步骤"})
                    phase = "completed"
                elif etype == "task.failed":
                    if phase not in ("running", "paused", "interrupted"):
                        anomalies.append({**ref, "code": "lifecycle", "message": "失败发生在非法状态"})
                    phase = "failed"
            except KeyError as exc:
                anomalies.append({**ref, "code": "malformed_detail", "message": f"详情缺少字段 {exc}"})

        if phase != task_row["status"]:
            anomalies.append({"code": "status_mismatch",
                              "message": f"事件重放状态为 {phase}，任务表状态为 {task_row['status']}"})
        if boundary != task_row["current_boundary"]:
            anomalies.append({"code": "boundary_mismatch", "message": "边界重放结果与任务表不一致"})
        if attempt_count != task_row["attempt_no"]:
            anomalies.append({"code": "attempt_mismatch", "message": "尝试次数与任务表不一致"})

        # 与步骤表交叉核对，发现只改一边的篡改。
        started_events = {row["step_no"]: row for row in rows if row["event_type"] == "step.started"}
        step_rows = self.database.connection.execute(
            "SELECT * FROM ledger_steps WHERE task_id=? ORDER BY step_no", (task_id,)
        ).fetchall()
        if step_rows and step_rows[-1]["step_no"] != len(step_rows):
            anomalies.append({"code": "step_gap", "message": "步骤表存在序号缺口"})
        for srow in step_rows:
            event = started_events.get(srow["step_no"])
            if event is None or event["input_hash"] != srow["input_hash"]:
                anomalies.append({"code": "step_table_mismatch", "seq": None,
                                  "message": f"步骤 {srow['step_no']} 与开始事件不一致"})
            if srow["status"] == "confirmed":
                confirmed = confirmed_steps.get(srow["step_no"])
                if confirmed is None or confirmed["output_hash"] != srow["output_hash"]:
                    anomalies.append({"code": "step_table_mismatch",
                                      "message": f"步骤 {srow['step_no']} 的确认结果与事件不一致"})
        last_confirmed = max(confirmed_steps, default=0)
        if task_row["last_step_no"] != last_confirmed:
            anomalies.append({"code": "checkpoint_mismatch",
                              "message": "检查点步骤与最后确认步骤不一致"})

        # 与租约表交叉核对。
        lease_rows = self.database.connection.execute(
            "SELECT * FROM ledger_leases WHERE task_id=?", (task_id,)
        ).fetchall()
        for lrow in lease_rows:
            grant = next((row for row in rows if row["event_type"] == "lease.granted"
                          and row["resource_id"] == lrow["resource_id"]), None)
            if grant is None:
                anomalies.append({"code": "lease_table_mismatch",
                                  "message": f"租约表资源 {lrow['resource_id']} 缺少授予事件"})
            table_active = lrow["status"] == "granted"
            event_active = lrow["resource_id"] in active_leases
            if table_active != event_active:
                anomalies.append({"code": "lease_table_mismatch",
                                  "message": f"资源 {lrow['resource_id']} 的租约活跃状态与事件链不一致"})

        return {"task_id": task_id, "valid": not anomalies, "event_count": len(rows),
                "status": task_row["status"], "anomalies": anomalies}

    def verify_all(self) -> dict[str, Any]:
        rows = self.database.connection.execute("SELECT task_id FROM ledger_tasks ORDER BY task_id").fetchall()
        tasks = [self.verify_task(row["task_id"]) for row in rows]
        return {"task_count": len(tasks), "valid": all(item["valid"] for item in tasks),
                "tasks": tasks}
