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
    "create_deployment",
    "trigger_run",
    "update_schedule",
    "cancel_schedule",
    "register_task",
    "list_deployments",
    "register_cron_task",
    "delete_deployment",
    "get_run_status",
]


def _service() -> PrefectTaskService:
    from .service import PrefectTaskService

    return PrefectTaskService()


def create_deployment(
    deployment_name: str,
    entrypoint: str,
):
    deployment = _service().create_deployment(entrypoint=entrypoint, deployment_name=deployment_name)
    print("✅ Deployment 已创建/更新")
    print(f"   Deployment: {deployment.name}")
    return deployment


def trigger_run(
    deployment_name: str,
    parameters: dict,
):
    run = _service().trigger_run(deployment_ref=deployment_name, parameters=parameters)
    print("=" * 50)
    print("📝 触发任务 (Deployment)")
    print("=" * 50)
    print(f"🚀 Deployment: {deployment_name}")
    print(f"📦 参数: {parameters}")
    print("=" * 50)
    print("\n✅ 任务触发成功!")
    print(f"   Flow Run ID: {run.id}")
    print(f"   状态: {run.state_type}")
    return run


def update_schedule(
    deployment_ref: str,
    cron: str,
    timezone: str | None = None,
):
    deployment = _service().update_schedule(deployment_ref=deployment_ref, cron=cron, timezone=timezone)
    print("✅ Cron 已更新")
    print(f"   Deployment: {deployment.name}")
    return deployment


def cancel_schedule(
    deployment_ref: str,
):
    deployment = _service().cancel_schedule(deployment_ref=deployment_ref)
    print("✅ Cron 已取消")
    print(f"   Deployment: {deployment.name}")
    return deployment


def register_task(
    deployment_name: str,
    parameters: dict,
):
    return trigger_run(deployment_name=deployment_name, parameters=parameters)


def list_deployments() -> list[DeploymentInfo]:
    items = _service().list_deployments()
    if not items:
        print("📋 暂无部署")
        return []

    print(f"\n📋 部署列表 (共 {len(items)} 个):")
    print("-" * 50)
    for item in items:
        print(f"  • {item.name} | pool={item.work_pool_name} | queue={item.work_queue_name}")
    print("-" * 50)
    return items


def register_cron_task(
    cron: str,
    entrypoint: str,
    deployment_name: str,
):
    _service().create_deployment(entrypoint=entrypoint, deployment_name=deployment_name)
    deployment = _service().update_schedule(deployment_ref=deployment_name, cron=cron)
    print("✅ Cron 任务已注册")
    display_name = deployment_name or deployment.name
    print(f"   Deployment: {display_name}")
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
