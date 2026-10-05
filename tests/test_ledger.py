import unittest
from datetime import datetime, timezone

from ai_governance_foundation.clock import FixedClock
from ai_governance_foundation.errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from ai_governance_foundation.ledger import LedgerService
from ai_governance_foundation.service import DomainService
from ai_governance_foundation.storage import Database


class LedgerTest(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        clock = FixedClock(datetime(2026, 10, 5, tzinfo=timezone.utc))
        self.clock = clock
        self.governance = DomainService(self.database, clock)
        self.ledger = LedgerService(self.database, clock)
        self.governance.register_organization(request_id="org", actor_id="bootstrap",
                                              organization_id="o1", name="机构一")
        self.governance.register_actor(request_id="admin", actor_id="bootstrap", new_actor_id="a1",
                                       display_name="管理员", role="admin", organization_id="o1")
        self.governance.register_actor(request_id="operator", actor_id="a1", new_actor_id="op1",
                                       display_name="操作员", role="operator", organization_id="o1")
        self.governance.register_actor(request_id="reviewer", actor_id="a1", new_actor_id="rv1",
                                       display_name="复核员", role="reviewer", organization_id="o1")
        self.governance.register_actor(request_id="auditor", actor_id="a1", new_actor_id="au1",
                                       display_name="审计员", role="auditor", organization_id="o1")

    def tearDown(self):
        self.database.close()

    def _submit(self, request_id="r1", task_id="t1", key="job-1", resources=None):
        return self.ledger.submit_task(
            request_id=request_id, actor_id="op1", idempotency_key=key, title="任务一",
            payload={"goal": "x"}, boundary="sandbox/read-only", task_id=task_id,
            resources=resources or [{"resource_id": "res-a", "mode": "read"},
                                    {"resource_id": "res-b", "mode": "exclusive"}])

    def _two_steps(self, task_id="t1"):
        self.ledger.start_step(request_id="s1", actor_id="op1", task_id=task_id, step_key="k1",
                               action_type="a1", inputs={"i": 1}, resource_id="res-a")
        self.ledger.confirm_step(request_id="c1", actor_id="op1", task_id=task_id,
                                 step_key="k1", outputs={"o": 1})
        self.ledger.start_step(request_id="s2", actor_id="op1", task_id=task_id, step_key="k2",
                               action_type="a2", inputs={"i": 2}, resource_id="res-b")
        self.ledger.confirm_step(request_id="c2", actor_id="op1", task_id=task_id,
                                 step_key="k2", outputs={"o": 2})

    # ---------------------------------------------------------------- 提交与租约

    def test_submit_records_boundary_and_leases(self):
        task = self._submit()
        self.assertEqual("running", task.status)
        self.assertEqual("sandbox/read-only", task.current_boundary)
        leases = self.ledger.list_leases(task_id="t1")
        self.assertEqual({"read", "exclusive"}, {item["mode"] for item in leases})
        report = self.ledger.verify_task("t1")
        self.assertTrue(report["valid"], report["anomalies"])

    def test_submit_is_idempotent_by_key(self):
        first = self._submit()
        again = self._submit(request_id="r2")
        self.assertFalse(first.replayed)
        self.assertTrue(again.replayed)
        self.assertEqual(first.task_id, again.task_id)
        # 重放不能重复授予租约。
        self.assertEqual(2, len(self.ledger.list_leases(task_id="t1")))
        self.assertEqual(2, len(self.ledger.list_leases(task_id="t1", active_only=True)))

    def test_same_key_with_changed_resources_rejected(self):
        self._submit()
        with self.assertRaises(ConflictError):
            self.ledger.submit_task(request_id="r9", actor_id="op1", idempotency_key="job-1",
                                    title="任务一", payload={"goal": "x"},
                                    boundary="sandbox/read-only", task_id="t1",
                                    resources=[{"resource_id": "res-c", "mode": "read"}])

    def test_exclusive_lease_blocks_other_task(self):
        self._submit(task_id="t1")
        self._submit(request_id="r2", task_id="t2", key="job-2",
                     resources=[{"resource_id": "res-c", "mode": "read"}])
        with self.assertRaises(ConflictError):
            self.ledger.grant_lease(request_id="g1", actor_id="op1", task_id="t2",
                                    resource_id="res-b", mode="read")
        # read 与 read 可以共享。
        granted = self.ledger.grant_lease(request_id="g2", actor_id="op1", task_id="t2",
                                          resource_id="res-a", mode="read")
        self.assertFalse(granted.replayed)

    def test_action_without_lease_is_denied(self):
        self._submit()
        with self.assertRaises(PermissionDenied):
            self.ledger.start_step(request_id="s1", actor_id="op1", task_id="t1", step_key="k1",
                                   action_type="a1", inputs={"i": 1}, resource_id="res-unknown")

    def test_auditor_cannot_submit(self):
        with self.assertRaises(PermissionDenied):
            self.ledger.submit_task(request_id="rx", actor_id="au1", idempotency_key="job-x",
                                    title="任务", payload={}, boundary="b",
                                    resources=[{"resource_id": "r", "mode": "read"}])

    # ------------------------------------------------------------- 步骤与唯一结果

    def test_steps_must_be_confirmed_in_order(self):
        self._submit()
        self.ledger.start_step(request_id="s1", actor_id="op1", task_id="t1", step_key="k1",
                               action_type="a1", inputs={"i": 1}, resource_id="res-a")
        with self.assertRaises(ConflictError):
            self.ledger.start_step(request_id="s2", actor_id="op1", task_id="t1", step_key="k2",
                                   action_type="a2", inputs={"i": 2}, resource_id="res-b")

    def test_confirm_without_start_is_not_found(self):
        self._submit()
        with self.assertRaises(NotFoundError):
            self.ledger.confirm_step(request_id="c1", actor_id="op1", task_id="t1",
                                     step_key="ghost", outputs={"o": 1})

    def test_retrying_confirmed_step_returns_same_result(self):
        self._submit()
        self._two_steps()
        replay = self.ledger.start_step(request_id="s1b", actor_id="op1", task_id="t1",
                                        step_key="k1", action_type="a1",
                                        inputs={"i": 1}, resource_id="res-a")
        self.assertTrue(replay.replayed)
        self.assertFalse(replay.redriven)
        self.assertEqual(1, replay.step_no)
        # 重复确认不同输出必须被拒绝，防止制造第二份有效结果。
        with self.assertRaises(ConflictError):
            self.ledger.confirm_step(request_id="c1b", actor_id="op1", task_id="t1",
                                     step_key="k1", outputs={"o": 999})
        same = self.ledger.confirm_step(request_id="c1c", actor_id="op1", task_id="t1",
                                        step_key="k1", outputs={"o": 1})
        self.assertTrue(same.replayed)

    def test_request_id_replay_does_not_duplicate_step(self):
        self._submit()
        kwargs = dict(actor_id="op1", task_id="t1", step_key="k1", action_type="a1",
                      inputs={"i": 1}, resource_id="res-a")
        first = self.ledger.start_step(request_id="dup", **kwargs)
        second = self.ledger.start_step(request_id="dup", **kwargs)
        self.assertFalse(first.replayed)
        self.assertTrue(second.replayed)
        self.assertEqual(1, second.step_no)

    # ------------------------------------------------------------- 中断与恢复

    def test_interrupted_task_resumes_from_last_confirmed_step(self):
        self._submit()
        self.ledger.start_step(request_id="s1", actor_id="op1", task_id="t1", step_key="k1",
                               action_type="a1", inputs={"i": 1}, resource_id="res-a")
        self.ledger.confirm_step(request_id="c1", actor_id="op1", task_id="t1",
                                 step_key="k1", outputs={"o": 1})
        self.ledger.start_step(request_id="s2", actor_id="op1", task_id="t1", step_key="k2",
                               action_type="a2", inputs={"i": 2}, resource_id="res-b")
        self.ledger.mark_interrupted(request_id="i1", actor_id="op1", task_id="t1",
                                     reason="崩溃")
        checkpoint = self.ledger.get_checkpoint("t1")
        self.assertEqual("interrupted", checkpoint["status"])
        self.assertEqual(1, checkpoint["last_confirmed_step_no"])
        self.assertEqual("k1", checkpoint["last_confirmed_step_key"])
        self.assertEqual(["k2"], [s["step_key"] for s in checkpoint["pending_steps"]])

        resumed = self.ledger.resume_task(request_id="rs1", actor_id="rv1", task_id="t1",
                                          reason="恢复")
        self.assertEqual(2, resumed.attempt_no)
        redrive = self.ledger.start_step(request_id="s2r", actor_id="op1", task_id="t1",
                                         step_key="k2", action_type="a2",
                                         inputs={"i": 2}, resource_id="res-b")
        self.assertTrue(redrive.redriven)
        self.assertEqual(2, redrive.step_no)
        self.ledger.confirm_step(request_id="c2", actor_id="op1", task_id="t1",
                                 step_key="k2", outputs={"o": 2})
        self.assertEqual(2, self.ledger.get_task("t1").last_step_no)
        self.assertTrue(self.ledger.verify_task("t1")["valid"])

    def test_redrive_with_changed_input_rejected(self):
        self._submit()
        self.ledger.start_step(request_id="s1", actor_id="op1", task_id="t1", step_key="k1",
                               action_type="a1", inputs={"i": 1}, resource_id="res-a")
        self.ledger.mark_interrupted(request_id="i1", actor_id="op1", task_id="t1", reason="崩溃")
        self.ledger.resume_task(request_id="rs1", actor_id="rv1", task_id="t1", reason="恢复")
        with self.assertRaises(ConflictError):
            self.ledger.start_step(request_id="s1b", actor_id="op1", task_id="t1", step_key="k1",
                                   action_type="a1", inputs={"i": 2}, resource_id="res-a")

    def test_pause_blocks_steps_and_resume_requires_privilege(self):
        self._submit()
        self.ledger.pause_task(request_id="p1", actor_id="rv1", task_id="t1", reason="人工暂停")
        with self.assertRaises(ConflictError):
            self.ledger.start_step(request_id="s1", actor_id="op1", task_id="t1", step_key="k1",
                                   action_type="a1", inputs={"i": 1})
        # 操作员无权恢复人工暂停。
        with self.assertRaises(PermissionDenied):
            self.ledger.resume_task(request_id="rs-bad", actor_id="op1", task_id="t1", reason="x")
        resumed = self.ledger.resume_task(request_id="rs1", actor_id="rv1", task_id="t1",
                                          reason="人工放行")
        self.assertEqual("running", resumed.status)

    def test_cannot_complete_with_unconfirmed_step(self):
        self._submit()
        self.ledger.start_step(request_id="s1", actor_id="op1", task_id="t1", step_key="k1",
                               action_type="a1", inputs={"i": 1}, resource_id="res-a")
        with self.assertRaises(ConflictError):
            self.ledger.complete_task(request_id="done", actor_id="op1", task_id="t1",
                                      result={"r": 1})

    def test_completed_task_rejects_second_result(self):
        self._submit()
        self._two_steps()
        done = self.ledger.complete_task(request_id="done", actor_id="op1", task_id="t1",
                                         result={"r": 1})
        self.assertEqual("completed", done.status)
        self.assertEqual("completed", self.ledger.get_task("t1").status)
        with self.assertRaises(ConflictError):
            self.ledger.complete_task(request_id="done2", actor_id="op1", task_id="t1",
                                      result={"r": 2})
        # 租约在完成时释放。
        self.assertEqual([], self.ledger.list_leases(task_id="t1", active_only=True))

    # ------------------------------------------------------------- 边界与审计链

    def test_boundary_change_is_traced(self):
        self._submit()
        changed = self.ledger.change_boundary(request_id="b1", actor_id="op1", task_id="t1",
                                              new_boundary="sandbox/write", reason="需要写文件")
        self.assertEqual("sandbox/write", changed.current_boundary)
        chain = self.ledger.chain_by_task("t1")
        types = [e["event_type"] for e in chain["events"]]
        self.assertIn("boundary.entered", types)
        self.assertIn("boundary.changed", types)
        self.assertTrue(chain["verification"]["valid"])

    def test_chains_by_resource_and_actor_are_verifiable(self):
        self._submit()
        self._two_steps()
        by_resource = self.ledger.chain_by_resource("res-b")
        self.assertIn("t1", by_resource["involved_tasks"])
        self.assertTrue(by_resource["all_chains_valid"])
        by_actor = self.ledger.chain_by_actor("op1")
        self.assertTrue(all(e["actor_id"] == "op1" for e in by_actor["events"]))
        self.assertTrue(by_actor["all_chains_valid"])

    def test_unknown_chain_target_raises(self):
        with self.assertRaises(NotFoundError):
            self.ledger.chain_by_task("ghost")

    def test_verify_detects_deleted_event_as_gap_and_broken_hash(self):
        self._submit()
        self._two_steps()
        self.assertTrue(self.ledger.verify_task("t1")["valid"])
        # 删除链条中段事件：序号缺口 + 哈希断裂必须同时被发现。
        row = self.database.connection.execute(
            "SELECT seq FROM ledger_events WHERE task_id='t1' AND event_type='step.started' "
            "ORDER BY seq LIMIT 1").fetchone()
        self.database.connection.execute("DELETE FROM ledger_events WHERE task_id='t1' AND seq=?",
                                         (row["seq"],))
        report = self.ledger.verify_task("t1")
        codes = {item["code"] for item in report["anomalies"]}
        self.assertFalse(report["valid"])
        self.assertIn("seq_gap_or_reorder", codes)
        self.assertIn("hash_broken", codes)

    def test_verify_detects_tampered_step_table(self):
        self._submit()
        self._two_steps()
        self.database.connection.execute(
            "UPDATE ledger_steps SET output_hash=? WHERE task_id='t1' AND step_key='k1'",
            ("0" * 64,))
        codes = {item["code"] for item in self.ledger.verify_task("t1")["anomalies"]}
        self.assertIn("step_table_mismatch", codes)

    def test_verify_detects_rewritten_checkpoint(self):
        self._submit()
        self._two_steps()
        # 把任务表的检查点回退到第一步，模拟有人试图让恢复从更早步骤重跑。
        self.database.connection.execute(
            "UPDATE ledger_tasks SET last_step_no=1, last_step_key='k1' WHERE task_id='t1'")
        codes = {item["code"] for item in self.ledger.verify_task("t1")["anomalies"]}
        self.assertIn("checkpoint_mismatch", codes)

    def test_verify_detects_out_of_order_boundary(self):
        self._submit()
        # 直接篡改边界变化事件的起点，模拟乱序/伪造边界迁移。
        self.ledger.change_boundary(request_id="b1", actor_id="op1", task_id="t1",
                                    new_boundary="b2", reason="r")
        row = self.database.connection.execute(
            "SELECT event_id FROM ledger_events WHERE task_id='t1' AND event_type='boundary.changed'"
        ).fetchone()
        import json
        from ai_governance_foundation.audit import canonical_json
        detail = json.loads(self.database.connection.execute(
            "SELECT detail_json FROM ledger_events WHERE event_id=?", (row["event_id"],)).fetchone()[0])
        detail["from"] = "sandbox/forged"
        self.database.connection.execute(
            "UPDATE ledger_events SET detail_json=? WHERE event_id=?",
            (canonical_json(detail), row["event_id"]))
        codes = {item["code"] for item in self.ledger.verify_task("t1")["anomalies"]}
        self.assertIn("hash_broken", codes)
        self.assertIn("boundary_out_of_order", codes)

    def test_validation_error_on_bad_identifier(self):
        with self.assertRaises(ValidationError):
            self.ledger.submit_task(request_id="bad id!", actor_id="op1", idempotency_key="job-x",
                                    title="t", payload={}, boundary="b",
                                    resources=[{"resource_id": "r", "mode": "read"}])


if __name__ == "__main__":
    unittest.main()
