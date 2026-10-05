"""提供不依赖第三方框架的 HTTP/JSON 边界。"""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from .errors import DomainError, ValidationError
from .ledger import LedgerService
from .service import DomainService
from .storage import Database


def _query(query: dict[str, list[str]], name: str, default: str | None = None) -> str | None:
    return query.get(name, [default])[0]


def route(service: DomainService, method: str, path: str, body: dict[str, Any] | None,
          headers: dict[str, str] | None = None,
          ledger: LedgerService | None = None) -> tuple[int, dict[str, Any]]:
    """把一个 HTTP 语义请求分派到领域服务。"""

    headers = headers or {}
    body = body or {}
    parsed = urlparse(path)
    actor_id = headers.get("X-Actor-Id", "")
    try:
        if method == "GET" and parsed.path == "/health":
            valid, count = service.verify_audit()
            ledger_valid = ledger.verify_all()["valid"] if ledger is not None else True
            return 200, {"status": "ok", "audit_valid": valid, "audit_events": count,
                         "ledger_valid": ledger_valid}
        if method == "POST" and parsed.path == "/organizations":
            receipt = service.register_organization(actor_id=actor_id, **body)
            return 200 if receipt.replayed else 201, receipt.__dict__
        if method == "POST" and parsed.path == "/actors":
            receipt = service.register_actor(actor_id=actor_id, **body)
            return 200 if receipt.replayed else 201, receipt.__dict__
        if method == "POST" and parsed.path == "/sites":
            receipt = service.register_site(actor_id=actor_id, **body)
            return 200 if receipt.replayed else 201, receipt.__dict__
        if method == "POST" and parsed.path == "/domain-records":
            receipt = service.record_domain_data(actor_id=actor_id, **body)
            return 200 if receipt.replayed else 201, receipt.__dict__
        if method == "GET" and parsed.path == "/domain-records":
            query = parse_qs(parsed.query)
            site_id = query.get("site_id", [""])[0]
            if not site_id:
                raise ValidationError("site_id 不能为空")
            category = query.get("category", [None])[0]
            return 200, {"items": [item.__dict__ for item in service.list_domain_data(site_id, category)]}
        if method == "GET" and parsed.path == "/audit-events":
            query = parse_qs(parsed.query)
            after = int(query.get("after_sequence", ["0"])[0])
            return 200, {"items": service.audit_events(after)}

        if ledger is not None:
            status, payload = _ledger_route(ledger, method, parsed, body, actor_id)
            if status is not None:
                return status, payload
        return 404, {"error": "route_not_found", "message": "接口不存在"}
    except DomainError as exc:
        return exc.status, {"error": exc.code, "message": str(exc)}
    except (TypeError, ValueError) as exc:
        return 400, {"error": "invalid_request", "message": str(exc)}


def _ledger_route(ledger: LedgerService, method: str, parsed, body: dict[str, Any],
                  actor_id: str) -> tuple[int | None, dict[str, Any]]:
    """处理运行账本子接口；未命中时返回 (None, {})。"""

    path = parsed.path
    query = parse_qs(parsed.query)

    if method == "POST" and path == "/ledger/tasks":
        task = ledger.submit_task(actor_id=actor_id, **body)
        return 200 if task.replayed else 201, task.__dict__
    if method == "GET" and path == "/ledger/task":
        return 200, ledger.get_task(_query(query, "task_id", "")).__dict__
    if method == "GET" and path == "/ledger/checkpoint":
        return 200, ledger.get_checkpoint(_query(query, "task_id", ""))
    if method == "POST" and path == "/ledger/grant-lease":
        lease = ledger.grant_lease(actor_id=actor_id, **body)
        return 200 if lease.replayed else 201, lease.__dict__
    if method == "POST" and path == "/ledger/release-lease":
        lease = ledger.release_lease(actor_id=actor_id, **body)
        return 200 if lease.replayed else 201, lease.__dict__
    if method == "GET" and path == "/ledger/leases":
        return 200, {"items": ledger.list_leases(
            task_id=_query(query, "task_id"), resource_id=_query(query, "resource_id"),
            active_only=_query(query, "active_only") in ("1", "true", "yes"))}
    if method == "POST" and path == "/ledger/start-step":
        step = ledger.start_step(actor_id=actor_id, **body)
        return 200 if step.replayed else 201, step.__dict__
    if method == "POST" and path == "/ledger/confirm-step":
        step = ledger.confirm_step(actor_id=actor_id, **body)
        return 200 if step.replayed else 201, step.__dict__
    if method == "POST" and path == "/ledger/change-boundary":
        task = ledger.change_boundary(actor_id=actor_id, **body)
        return 200 if task.replayed else 201, task.__dict__
    if method == "POST" and path == "/ledger/pause":
        task = ledger.pause_task(actor_id=actor_id, **body)
        return 200 if task.replayed else 201, task.__dict__
    if method == "POST" and path == "/ledger/interrupt":
        task = ledger.mark_interrupted(actor_id=actor_id, **body)
        return 200 if task.replayed else 201, task.__dict__
    if method == "POST" and path == "/ledger/resume":
        task = ledger.resume_task(actor_id=actor_id, **body)
        return 200 if task.replayed else 201, task.__dict__
    if method == "POST" and path == "/ledger/complete":
        task = ledger.complete_task(actor_id=actor_id, **body)
        return 200 if task.replayed else 201, task.__dict__
    if method == "POST" and path == "/ledger/fail":
        task = ledger.fail_task(actor_id=actor_id, **body)
        return 200 if task.replayed else 201, task.__dict__
    if method == "GET" and path == "/ledger/chain/task":
        return 200, ledger.chain_by_task(_query(query, "task_id", ""))
    if method == "GET" and path == "/ledger/chain/resource":
        return 200, ledger.chain_by_resource(_query(query, "resource_id", ""))
    if method == "GET" and path == "/ledger/chain/actor":
        return 200, ledger.chain_by_actor(_query(query, "actor_id", ""))
    if method == "GET" and path == "/ledger/verify":
        return 200, ledger.verify_all()
    return None, {}


class Handler(BaseHTTPRequestHandler):
    """把标准库 HTTP 请求转换为路由调用。"""

    service: DomainService
    ledger: LedgerService

    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._write(400, {"error": "invalid_json", "message": "请求体必须是 UTF-8 JSON"})
            return
        status, payload = route(self.service, self.command, self.path, body,
                                {"X-Actor-Id": self.headers.get("X-Actor-Id", "")},
                                ledger=self.ledger)
        self._write(status, payload)

    def _write(self, status: int, payload: dict[str, Any]) -> None:
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        self._handle()

    def do_POST(self) -> None:
        self._handle()

    def log_message(self, format: str, *args: object) -> None:
        return


def main() -> int:
    """启动本地 HTTP 服务。"""

    parser = argparse.ArgumentParser(description="启动科技战略协作基础服务")
    parser.add_argument("--database", default="service.sqlite3")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    database = Database(args.database)
    Handler.service = DomainService(database)
    Handler.ledger = LedgerService(database)
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        database.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
