"""运行自主任务运行账本的离线端到端验收。"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .clock import FixedClock
from .errors import ConflictError
from .ledger import LedgerService
from .service import DomainService
from .storage import Database


def run() -> dict[str, object]:
    """执行包含租约、步骤确认、中断恢复、人工暂停与篡改校验的完整账本链。"""

    with tempfile.TemporaryDirectory() as directory:
        database = Database(Path(directory) / "ledger_acceptance.sqlite3")
        base = datetime(2026, 10, 5, 8, 0, tzinfo=timezone.utc)
        clock = FixedClock(base)
        governance = DomainService(database, clock)
        ledger = LedgerService(database, clock)

        governance.register_organization(request_id="org", actor_id="bootstrap",
                                         organization_id="org-001", name="示范科研机构")
        governance.register_actor(request_id="admin", actor_id="bootstrap", new_actor_id="admin-001",
                                  display_name="系统管理员", role="admin", organization_id="org-001")
        governance.register_actor(request_id="operator", actor_id="admin-001", new_actor_id="op-001",
                                  display_name="任务执行人", role="operator", organization_id="org-001")
        governance.register_actor(request_id="reviewer", actor_id="admin-001", new_actor_id="rv-001",
                                  display_name="人工复核员", role="reviewer", organization_id="org-001")

        def tick(minutes: int) -> None:
            clock.advance(timedelta(minutes=minutes))

        # 1) 提交任务：声明初始隔离边界与资源租约；重放得到同一任务。
        task = ledger.submit_task(
            request_id="req-submit", actor_id="op-001", idempotency_key="job-2026-1005",
            title="自主文献综述任务", payload={"goal": "汇总数据集并生成报告"},
            boundary="sandbox/read-only",
            resources=[{"resource_id": "dataset://papers", "mode": "read"},
                       {"resource_id": "model://reasoner", "mode": "exclusive"}],
            task_id="task-001",
        )
        replay_submit = ledger.submit_task(
            request_id="req-submit-2", actor_id="op-001", idempotency_key="job-2026-1005",
            title="自主文献综述任务", payload={"goal": "汇总数据集并生成报告"},
            boundary="sandbox/read-only",
            resources=[{"resource_id": "dataset://papers", "mode": "read"},
                       {"resource_id": "model://reasoner", "mode": "exclusive"}],
            task_id="task-001",
        )
        tick(1)

        # 2) 步骤 1 开始并确认。
        ledger.start_step(request_id="req-s1-start", actor_id="op-001", task_id="task-001",
                          step_key="step-fetch", action_type="dataset.fetch",
                          inputs={"dataset": "papers", "limit": 100}, resource_id="dataset://papers")
        tick(1)
        ledger.confirm_step(request_id="req-s1-confirm", actor_id="op-001", task_id="task-001",
                            step_key="step-fetch", outputs={"rows": 100, "checksum": "abc"})
        tick(1)

        # 3) 步骤 2 开始后执行器崩溃（未确认），随后登记中断。
        ledger.start_step(request_id="req-s2-start", actor_id="op-001", task_id="task-001",
                          step_key="step-reason", action_type="model.reason",
                          inputs={"prompt": "summarize"}, resource_id="model://reasoner")
        tick(1)
        ledger.mark_interrupted(request_id="req-interrupt", actor_id="op-001", task_id="task-001",
                                reason="执行器进程崩溃")
        checkpoint = ledger.get_checkpoint("task-001")
        tick(1)

        # 4) 恢复：开启第 2 次尝试，从未确认步骤重放，检查点停在 step-fetch。
        ledger.resume_task(request_id="req-resume-1", actor_id="rv-001", task_id="task-001",
                           reason="崩溃后恢复")
        redriven = ledger.start_step(request_id="req-s2-redrive", actor_id="op-001", task_id="task-001",
                                     step_key="step-reason", action_type="model.reason",
                                     inputs={"prompt": "summarize"}, resource_id="model://reasoner")
        tick(1)
        ledger.confirm_step(request_id="req-s2-confirm", actor_id="op-001", task_id="task-001",
                            step_key="step-reason", outputs={"summary": "完成", "tokens": 4200})
        tick(1)

        # 5) 隔离边界变化。
        ledger.change_boundary(request_id="req-boundary", actor_id="op-001", task_id="task-001",
                               new_boundary="sandbox/write-output", reason="需要写入报告存储")
        tick(1)

        # 6) 人工暂停：复核员介入；暂停期间不能执行步骤；之后恢复为第 3 次尝试。
        ledger.pause_task(request_id="req-pause", actor_id="rv-001", task_id="task-001",
                          reason="等待人工确认外发范围")
        blocked = False
        try:
            ledger.start_step(request_id="req-blocked", actor_id="op-001", task_id="task-001",
                              step_key="step-write", action_type="storage.write",
                              inputs={"path": "/out/report.md"})
        except ConflictError:
            blocked = True
        tick(1)
        resumed = ledger.resume_task(request_id="req-resume-2", actor_id="rv-001", task_id="task-001",
                                     reason="人工确认通过")
        ledger.start_step(request_id="req-s3-start", actor_id="op-001", task_id="task-001",
                          step_key="step-write", action_type="storage.write",
                          inputs={"path": "/out/report.md"})
        tick(1)
        ledger.confirm_step(request_id="req-s3-confirm", actor_id="op-001", task_id="task-001",
                            step_key="step-write", outputs={"bytes": 8800, "uri": "store://report"})
        tick(1)

        # 7) 完成任务：唯一有效结果；重试相同结果被幂等吸收，不同结果被拒绝。
        completed = ledger.complete_task(request_id="req-complete", actor_id="op-001",
                                         task_id="task-001", result={"report": "store://report"})
        replay_same = ledger.complete_task(request_id="req-complete-replay", actor_id="op-001",
                                           task_id="task-001", result={"report": "store://report"})
        duplicate_rejected = False
        try:
            ledger.complete_task(request_id="req-complete-other", actor_id="op-001",
                                 task_id="task-001", result={"report": "store://TAMPERED"})
        except ConflictError:
            duplicate_rejected = True

        # 8) 三类执行链还原与全量校验。
        verification = ledger.verify_all()
        by_task = ledger.chain_by_task("task-001")
        by_resource = ledger.chain_by_resource("model://reasoner")
        by_actor = ledger.chain_by_actor("rv-001")
        audit_valid, audit_count = governance.verify_audit()

        # 9) 篡改：删除链条中段事件，校验必须发现缺失/乱序与哈希断裂。
        database.connection.execute("DELETE FROM ledger_events WHERE task_id='task-001' AND seq=5")
        tampered = ledger.verify_task("task-001")
        tamper_codes = {item["code"] for item in tampered["anomalies"]}

        database.close()
        return {
            "status": "ok",
            "submit_replayed": replay_submit.replayed,
            "same_task_id": replay_submit.task_id == "task-001",
            "checkpoint_after_crash": checkpoint["last_confirmed_step_key"],
            "pending_after_crash": [s["step_key"] for s in checkpoint["pending_steps"]],
            "redriven_same_step": redriven.redriven and redriven.step_no == 2,
            "step_blocked_while_paused": blocked,
            "attempts_after_resume": resumed.attempt_no,
            "final_status": completed.status,
            "completion_replayed": replay_same.replayed,
            "duplicate_result_rejected": duplicate_rejected,
            "task_event_count": len(by_task["events"]),
            "resource_chain_events": len(by_resource["events"]),
            "actor_chain_events": len(by_actor["events"]),
            "audit_valid": audit_valid,
            "audit_events": audit_count,
            "ledger_valid_before_tamper": verification["valid"],
            "tamper_detected": (not tampered["valid"])
            and {"seq_gap_or_reorder", "hash_broken"}.issubset(tamper_codes),
        }


def main() -> int:
    """打印验收结果并设置退出码。"""

    result = run()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    expected_true = [
        "submit_replayed", "same_task_id", "redriven_same_step", "step_blocked_while_paused",
        "completion_replayed", "duplicate_result_rejected", "audit_valid",
        "ledger_valid_before_tamper", "tamper_detected",
    ]
    ok = result["status"] == "ok" and all(result[key] for key in expected_true)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
