import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from prefect.client.orchestration import get_client
from prefect.deployments.runner import RunnerDeployment
from prefect.deployments.schedules import create_deployment_schedule_create
from prefect.client.schemas.schedules import CronSchedule

from .exceptions import DeploymentNotFoundError, FlowRunNotFoundError, PrefectServiceError
from .models import DeploymentInfo, FlowRunInfo, RunStatusInfo


@dataclass
class PrefectClientConfig:
    api_url: str = "http://127.0.0.1:4200/api"
    prefect_home: Optional[str] = None
    default_work_pool: str = "task-pool"
    default_work_queue: str = "task-queue"
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
        for d in self.list_deployments():
            if d.name == deployment_ref:
                return d.id
        raise DeploymentNotFoundError(f"Deployment not found: {deployment_ref}")

    def create_deployment(
        self,
        entrypoint: str,
        deployment_name: Optional[str] = None,
        work_pool_name: Optional[str] = None,
        work_queue_name: Optional[str] = None,
    ) -> DeploymentInfo:
        deployment_name = deployment_name or self.config.default_deployment_name
        work_pool_name = work_pool_name or self.config.default_work_pool
        work_queue_name = work_queue_name or self.config.default_work_queue

        deployment_kwargs = {
            "entrypoint": entrypoint,
            "name": deployment_name,
            "work_pool_name": work_pool_name,
            "work_queue_name": work_queue_name,
        }

        deployment = RunnerDeployment.from_entrypoint(**deployment_kwargs)
        deployment_id = deployment.apply(work_pool_name=work_pool_name)

        with self._client() as client:
            saved = client.read_deployment(deployment_id)

        if not saved:
            raise PrefectServiceError("Deployment apply succeeded but reading deployment failed")

        return self._to_deployment_info(saved)

    def ensure_deployment(
        self,
        entrypoint: str,
        deployment_name: Optional[str] = None,
        work_pool_name: Optional[str] = None,
        work_queue_name: Optional[str] = None,
    ) -> DeploymentInfo:
        return self.create_deployment(
            entrypoint=entrypoint,
            deployment_name=deployment_name,
            work_pool_name=work_pool_name,
            work_queue_name=work_queue_name,
        )

    def trigger_run(
        self,
        deployment_ref: str,
        parameters: dict,
        work_queue_name: Optional[str] = None,
    ) -> FlowRunInfo:
        work_queue_name = work_queue_name or self.config.default_work_queue

        try:
            deployment_id = self._resolve_deployment_id(deployment_ref)
        except DeploymentNotFoundError:
            raise DeploymentNotFoundError(
                f"Deployment not found: {deployment_ref}. Please create it first via create_deployment(entrypoint=...)."
            )

        with self._client() as client:
            flow_run = client.create_flow_run_from_deployment(
                deployment_id=deployment_id,
                parameters=parameters,
                work_queue_name=work_queue_name,
            )

        return self._to_flow_run_info(flow_run)

    def update_schedule(
        self,
        deployment_ref: str,
        cron: str,
        timezone: Optional[str] = None,
        active: bool = True,
        parameters: Optional[dict] = None,
    ) -> DeploymentInfo:
        deployment_id = self._resolve_deployment_id(deployment_ref)

        with self._client() as client:
            schedules = client.read_deployment_schedules(deployment_id=deployment_id)

            schedule = CronSchedule(cron=cron, timezone=timezone)
            schedule_create = create_deployment_schedule_create(schedule=schedule, active=active)
            if parameters:
                if hasattr(schedule_create, "model_copy"):
                    schedule_create = schedule_create.model_copy(update={"parameters": parameters})
                else:
                    schedule_create.parameters = parameters

            if schedules:
                for schedule_item in schedules:
                    client.delete_deployment_schedule(deployment_id=deployment_id, schedule_id=schedule_item.id)
            client.create_deployment_schedules(
                deployment_id=deployment_id,
                schedules=[schedule_create],
            )

            saved = client.read_deployment(deployment_id)

        if not saved:
            raise PrefectServiceError("Updating schedule succeeded but reading deployment failed")

        return self._to_deployment_info(saved)

    def cancel_schedule(self, deployment_ref: str) -> DeploymentInfo:
        deployment_id = self._resolve_deployment_id(deployment_ref)

        with self._client() as client:
            schedules = client.read_deployment_schedules(deployment_id=deployment_id)
            for schedule in schedules:
                client.delete_deployment_schedule(deployment_id=deployment_id, schedule_id=schedule.id)
            saved = client.read_deployment(deployment_id)

        if not saved:
            raise PrefectServiceError("Cancelling schedule succeeded but reading deployment failed")

        return self._to_deployment_info(saved)

    def register_one_time_run(
        self,
        parameters: dict,
        deployment_ref: str,
        work_queue_name: Optional[str] = None,
    ) -> FlowRunInfo:
        return self.trigger_run(
            deployment_ref=deployment_ref,
            parameters=parameters,
            work_queue_name=work_queue_name,
        )

    def register_cron_task(
        self,
        cron: str,
        entrypoint: str,
        deployment_name: str,
    ) -> DeploymentInfo:
        self.ensure_deployment(entrypoint=entrypoint, deployment_name=deployment_name)
        return self.update_schedule(deployment_ref=deployment_name, cron=cron)

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
