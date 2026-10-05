import json
import unittest

from ai_governance_foundation.api import route
from ai_governance_foundation.ledger import LedgerService
from ai_governance_foundation.service import DomainService
from ai_governance_foundation.storage import Database


def bootstrap(database: Database):
    governance = DomainService(database)
    ledger = LedgerService(database)
    route(governance, "POST", "/organizations",
          {"request_id": "org", "organization_id": "o1", "name": "机构一"},
          {"X-Actor-Id": "bootstrap"})
    route(governance, "POST", "/actors",
          {"request_id": "a1", "new_actor_id": "op1", "display_name": "操作员",
           "role": "operator", "organization_id": "o1"},
          {"X-Actor-Id": "bootstrap"})
    return governance, ledger


class LedgerApiTest(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        self.governance, self.ledger = bootstrap(self.database)

    def tearDown(self):
        self.database.close()

    def call(self, method, path, body=None, actor="op1"):
        return route(self.governance, method, path, body or {}, {"X-Actor-Id": actor},
                     ledger=self.ledger)

    def test_full_lifecycle_over_http(self):
        status, submit = self.call("POST", "/ledger/tasks", {
            "request_id": "q1", "idempotency_key": "job-1", "title": "任务",
            "payload": {"goal": "x"}, "boundary": "sandbox",
            "resources": [{"resource_id": "res-a", "mode": "exclusive"}]})
        self.assertEqual(201, status)
        task_id = submit["task_id"]

        status, again = self.call("POST", "/ledger/tasks", {
            "request_id": "q1", "idempotency_key": "job-1", "title": "任务",
            "payload": {"goal": "x"}, "boundary": "sandbox",
            "resources": [{"resource_id": "res-a", "mode": "exclusive"}]})
        self.assertEqual(200, status)
        self.assertTrue(again["replayed"])

        status, started = self.call("POST", "/ledger/start-step", {
            "request_id": "q2", "task_id": task_id, "step_key": "k1", "action_type": "a1",
            "inputs": {"i": 1}, "resource_id": "res-a"})
        self.assertEqual(201, status)
        self.assertEqual(1, started["step_no"])

        status, confirmed = self.call("POST", "/ledger/confirm-step", {
            "request_id": "q3", "task_id": task_id, "step_key": "k1", "outputs": {"o": 1}})
        self.assertEqual(201, status)
        self.assertIsNotNone(confirmed["output_hash"])

        status, chain = self.call("GET", f"/ledger/chain/task?task_id={task_id}")
        self.assertEqual(200, status)
        self.assertTrue(chain["verification"]["valid"], chain["verification"]["anomalies"])

        status, resource_chain = self.call("GET", "/ledger/chain/resource?resource_id=res-a")
        self.assertEqual(200, status)
        self.assertTrue(resource_chain["all_chains_valid"])
        self.assertEqual([task_id], resource_chain["involved_tasks"])

        status, verify = self.call("GET", "/ledger/verify")
        self.assertEqual(200, status)
        self.assertTrue(verify["valid"])

        status, health = route(self.governance, "GET", "/health", None, ledger=self.ledger)
        self.assertEqual(200, status)
        self.assertTrue(health["ledger_valid"])

    def test_pause_and_checkpoint_over_http(self):
        _, submit = self.call("POST", "/ledger/tasks", {
            "request_id": "q1", "idempotency_key": "job-1", "title": "任务",
            "payload": {}, "boundary": "sandbox",
            "resources": [{"resource_id": "res-a", "mode": "read"}]})
        task_id = submit["task_id"]
        status, _ = self.call("POST", "/ledger/interrupt",
                              {"request_id": "q2", "task_id": task_id, "reason": "崩溃"})
        self.assertEqual(201, status)
        status, checkpoint = self.call("GET", f"/ledger/checkpoint?task_id={task_id}")
        self.assertEqual(200, status)
        self.assertEqual("interrupted", checkpoint["status"])
        # 未知路由仍然 404。
        status, payload = self.call("GET", "/ledger/nope")
        self.assertEqual(404, status)


if __name__ == "__main__":
    unittest.main()
