import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from prefect.client.orchestration import get_client
from prefect.deployments.runner import RunnerDeployment

from .exceptions import DeploymentNotFoundError, FlowRunNotFoundError, PrefectServiceError
from .models import DeploymentInfo, FlowRunInfo, RunStatusInfo


@dataclass
class PrefectClientConfig:
    api_url: str = "http://127.0.0.1:4200/api"
    prefect_home: Optional[str] = None
    default_work_pool: str = "task-pool"
    default_work_queue: str = "task-queue"
    default_entrypoint: str = "flows.task_flow:my_task_flow"
    default_flow_name: str = "my-task-flow"
    default_deployment_name: str = "task-run-deployment"


class PrefectTaskService:
    def __init__(self, config: Optional[PrefectClientConfig] = None):
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        default_home = os.path.join(project_root, ".prefect_data")
        self.config = config or PrefectClientConfig(prefect_home=default_home)
        if self.config.prefect_home:
            os.environ["PREFECT_HOME"] = self.config.prefect_home
        os.environ["PREFECT_API_URL"] = self.config.api_url

    def _client(self):
        return get_client(sync_client=True)

    def _to_deployment_info(self, d) -> DeploymentInfo:
        flow_name = getattr(d, "flow_name", "") or self.config.default_flow_name
        return DeploymentInfo(
            id=str(d.id),
            name=d.name,
            flow_name=flow_name,
            full_name=f"{flow_name}/{d.name}",
            work_pool_name=getattr(d, "work_pool_name", None),
            work_queue_name=getattr(d, "work_queue_name", None),
        )

    def _to_flow_run_info(self, r) -> FlowRunInfo:
        return FlowRunInfo(
            id=str(r.id),
            deployment_id=str(r.deployment_id) if getattr(r, "deployment_id", None) else None,
            name=getattr(r, "name", None),
            state_type=str(getattr(r, "state_type", "")) if getattr(r, "state_type", None) else None,
            state_name=getattr(r, "state_name", None),
            created=getattr(r, "created", None),
            expected_start_time=getattr(r, "expected_start_time", None),
        )

    def list_deployments(self) -> list[DeploymentInfo]:
        with self._client() as client:
            deployments = client.read_deployments()
        return [self._to_deployment_info(d) for d in deployments]

    def _resolve_deployment_id(self, deployment_ref: str) -> str:
        deployment_ref = deployment_ref.strip()
        deployment_name = deployment_ref.split("/")[-1]
        for d in self.list_deployments():
            if d.name == deployment_name or d.full_name == deployment_ref:
                return d.id
        raise DeploymentNotFoundError(f"Deployment not found: {deployment_ref}")

    def ensure_deployment(
        self,
        deployment_name: Optional[str] = None,
        entrypoint: Optional[str] = None,
        work_pool_name: Optional[str] = None,
        work_queue_name: Optional[str] = None,
        cron: Optional[str] = None,
        timezone: str = "UTC",
    ) -> DeploymentInfo:
        deployment_name = deployment_name or self.config.default_deployment_name
        entrypoint = entrypoint or self.config.default_entrypoint
        work_pool_name = work_pool_name or self.config.default_work_pool
        work_queue_name = work_queue_name or self.config.default_work_queue

        existing = None
        for d in self.list_deployments():
            if d.name == deployment_name:
                existing = d
                break

        deployment_kwargs = {
            "entrypoint": entrypoint,
            "name": deployment_name,
            "work_pool_name": work_pool_name,
            "work_queue_name": work_queue_name,
        }
        if cron:
            deployment_kwargs["cron"] = cron
            deployment_kwargs["timezone"] = timezone

        deployment = RunnerDeployment.from_entrypoint(**deployment_kwargs)
        deployment_id = deployment.apply(work_pool_name=work_pool_name)

        with self._client() as client:
            saved = client.read_deployment(deployment_id)

        if not saved:
            raise PrefectServiceError("Deployment apply succeeded but reading deployment failed")

        return self._to_deployment_info(saved)

    def register_one_time_run(
        self,
        task_name: str,
        deployment_ref: Optional[str] = None,
        work_queue_name: Optional[str] = None,
    ) -> FlowRunInfo:
        deployment_ref = deployment_ref or f"{self.config.default_flow_name}/{self.config.default_deployment_name}"
        work_queue_name = work_queue_name or self.config.default_work_queue

        try:
            deployment_id = self._resolve_deployment_id(deployment_ref)
        except DeploymentNotFoundError:
            self.ensure_deployment()
            deployment_id = self._resolve_deployment_id(deployment_ref)

        with self._client() as client:
            flow_run = client.create_flow_run_from_deployment(
                deployment_id=deployment_id,
                parameters={"task_name": task_name},
                work_queue_name=work_queue_name,
            )

        return self._to_flow_run_info(flow_run)

    def register_cron_task(
        self,
        cron: str,
        deployment_name: Optional[str] = None,
        flow_name: Optional[str] = None,
        timezone: str = "UTC",
        work_pool_name: Optional[str] = None,
        work_queue_name: Optional[str] = None,
        entrypoint: Optional[str] = None,
    ) -> DeploymentInfo:
        _ = flow_name  # reserved for external naming; flow comes from entrypoint
        return self.ensure_deployment(
            deployment_name=deployment_name or self.config.default_deployment_name,
            entrypoint=entrypoint or self.config.default_entrypoint,
            work_pool_name=work_pool_name or self.config.default_work_pool,
            work_queue_name=work_queue_name or self.config.default_work_queue,
            cron=cron,
            timezone=timezone,
        )

    def delete_deployment(self, deployment_ref: str, missing_ok: bool = True) -> bool:
        try:
            deployment_id = self._resolve_deployment_id(deployment_ref)
        except DeploymentNotFoundError:
            if missing_ok:
                return False
            raise

        with self._client() as client:
            client.delete_deployment(deployment_id)
        return True

    def get_run_status(self, flow_run_id: str) -> RunStatusInfo:
        with self._client() as client:
            flow_run = client.read_flow_run(flow_run_id)

        if not flow_run:
            raise FlowRunNotFoundError(f"Flow run not found: {flow_run_id}")

        state_type = str(getattr(flow_run, "state_type", "UNKNOWN"))
        state_name = getattr(flow_run, "state_name", state_type)

        return RunStatusInfo(
            flow_run_id=str(flow_run.id),
            state_type=state_type,
            state_name=state_name,
            is_terminal=state_type in {"COMPLETED", "FAILED", "CANCELLED", "CRASHED", "TIMEDOUT"},
            is_completed=state_type == "COMPLETED",
            is_failed=state_type in {"FAILED", "CRASHED", "TIMEDOUT"},
            is_running=state_type == "RUNNING",
        )
