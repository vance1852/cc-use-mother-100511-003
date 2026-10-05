"""定义基础服务在模块边界使用的数据对象。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Actor:
    """表示具有明确角色的后台操作者。"""

    actor_id: str
    display_name: str
    role: str
    organization_id: str
    active: bool


@dataclass(frozen=True)
class Site:
    """表示科研创新机构下的业务场所。"""

    site_id: str
    organization_id: str
    name: str
    timezone_name: str
    version: int


@dataclass(frozen=True)
class DomainRecord:
    """表示已经持久化的领域资料记录。"""

    record_id: str
    site_id: str
    category: str
    external_key: str
    payload: dict[str, Any]
    created_by: str
    created_at: str


@dataclass(frozen=True)
class WriteReceipt:
    """描述一次幂等写入的稳定结果。"""

    request_id: str
    resource_type: str
    resource_id: str
    replayed: bool


@dataclass(frozen=True)
class LedgerTask:
    """表示自主任务运行账本中的一个任务及其恢复检查点。"""

    task_id: str
    idempotency_key: str
    title: str
    status: str
    submitted_by: str
    current_boundary: str
    last_step_no: int
    last_step_key: str | None
    last_output_hash: str | None
    attempt_no: int
    result_hash: str | None
    created_at: str
    updated_at: str
    replayed: bool = False


@dataclass(frozen=True)
class Lease:
    """表示任务对某个资源持有的租约。"""

    lease_id: str
    task_id: str
    resource_id: str
    mode: str
    status: str
    granted_by: str
    granted_at: str
    released_at: str | None
    released_by: str | None
    release_reason: str | None
    replayed: bool = False


@dataclass(frozen=True)
class StepRecord:
    """表示任务中一个可恢复的动作步骤。"""

    task_id: str
    step_no: int
    step_key: str
    action_type: str
    input_hash: str
    output_hash: str | None
    status: str
    started_at: str
    confirmed_at: str | None
    replayed: bool = False
    redriven: bool = False
