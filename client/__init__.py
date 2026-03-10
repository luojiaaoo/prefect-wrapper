from __future__ import annotations

"""Compatibility wrappers for reusable Prefect client service."""
from typing import TYPE_CHECKING

from .models import DeploymentInfo, FlowRunInfo, RunStatusInfo

if TYPE_CHECKING:
    from .service import PrefectClientConfig, PrefectTaskService


__all__ = [
    "PrefectClientConfig",
    "PrefectTaskService",
    "DeploymentInfo",
    "FlowRunInfo",
    "RunStatusInfo",
    "register_task",
    "list_deployments",
    "register_cron_task",
    "delete_deployment",
    "get_run_status",
]


def _service() -> PrefectTaskService:
    from .service import PrefectTaskService

    return PrefectTaskService()


def register_task(
    task_name: str,
    deployment_name: str,
    entrypoint: str,
    timeout: int = 60,
):
    _ = timeout
    run = _service().register_one_time_run(task_name=task_name, deployment_ref=deployment_name, entrypoint=entrypoint)
    print("=" * 50)
    print("📝 触发任务 (Deployment)")
    print("=" * 50)
    print(f"📋 任务名称: {task_name}")
    print(f"🚀 Deployment: {deployment_name}")
    print("=" * 50)
    print("\n✅ 任务触发成功!")
    print(f"   Flow Run ID: {run.id}")
    print(f"   状态: {run.state_type}")
    return run


def list_deployments() -> list[DeploymentInfo]:
    items = _service().list_deployments()
    if not items:
        print("📋 暂无部署")
        return []

    print(f"\n📋 部署列表 (共 {len(items)} 个):")
    print("-" * 50)
    for item in items:
        print(f"  • {item.full_name} | pool={item.work_pool_name} | queue={item.work_queue_name}")
    print("-" * 50)
    return items


def register_cron_task(
    cron: str,
    entrypoint: str,
    deployment_name: str,
):
    deployment = _service().register_cron_task(
        cron=cron,
        entrypoint=entrypoint,
        deployment_name=deployment_name,
    )
    print("✅ Cron 任务已注册")
    print(f"   Deployment: {deployment.full_name}")
    return deployment


def delete_deployment(deployment_ref: str, missing_ok: bool = True) -> bool:
    deleted = _service().delete_deployment(deployment_ref=deployment_ref, missing_ok=missing_ok)
    if deleted:
        print(f"✅ 已删除 deployment: {deployment_ref}")
    else:
        print(f"ℹ️ deployment 不存在: {deployment_ref}")
    return deleted


def get_run_status(flow_run_id: str) -> RunStatusInfo:
    status = _service().get_run_status(flow_run_id)
    print("📊 任务状态")
    print(f"   Run ID: {status.flow_run_id}")
    print(f"   State: {status.state_type} ({status.state_name})")
    print(f"   Terminal: {status.is_terminal}")
    print(f"   Completed: {status.is_completed}")
    print(f"   Failed: {status.is_failed}")
    return status
