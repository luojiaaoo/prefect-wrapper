import logging
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

logger = logging.getLogger(__name__)


@dataclass
class PrefectClientConfig:
    api_url: str = "http://127.0.0.1:4200/api"
    prefect_home: Optional[str] = None



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
        flow_name = getattr(d, "flow_name", None)
        flow_id = getattr(d, "flow_id", None)
        entrypoint = getattr(d, "entrypoint", None)
        schedules = getattr(d, "schedules", None)
        schedule_descriptions = None
        if schedules:
            schedule_descriptions = []
            for schedule in schedules:
                schedule_item = getattr(schedule, "schedule", None)
                if schedule_item:
                    schedule_type = type(schedule_item).__name__
                else:
                    schedule_type = None
                cron = getattr(schedule_item, "cron", None) if schedule_item else None
                timezone = getattr(schedule_item, "timezone", None) if schedule_item else None
                if cron:
                    description = f"{schedule_type}:{cron}"
                else:
                    description = schedule_type or "schedule"
                if timezone:
                    description = f"{description} ({timezone})"
                schedule_descriptions.append(description)
        return DeploymentInfo(
            id=str(d.id),
            name=d.name,
            flow_name=flow_name,
            flow_id=str(flow_id) if flow_id else None,
            entrypoint=entrypoint,
            work_pool_name=d.work_pool_name,
            work_queue_name=d.work_queue_name,
            schedules=schedule_descriptions,
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
            items = [self._to_deployment_info(d) for d in deployments]
            for item in items:
                if item.flow_name or not item.flow_id:
                    if not item.flow_name and not item.flow_id:
                        logger.warning("Missing flow_id for deployment %s", item.id)
                    continue
                try:
                    flow = client.read_flow(item.flow_id)
                except Exception as exc:
                    logger.warning("Failed to read flow %s: %s", item.flow_id, exc)
                    flow = None
                flow_name = getattr(flow, "name", None) if flow else None
                if flow_name:
                    item.flow_name = flow_name
                else:
                    logger.warning("Missing flow_name for flow %s", item.flow_id)
        return items

    def _resolve_deployment_id(self, deployment_ref: str) -> str:
        deployment_ref = deployment_ref.strip()
        for d in self.list_deployments():
            if d.name == deployment_ref:
                return d.id
        raise DeploymentNotFoundError(f"Deployment not found: {deployment_ref}")

    def create_deployment(
        self,
        entrypoint: str,
        deployment_name: str,
        work_queue_name: str,
        work_pool_name: str,
    ) -> DeploymentInfo:
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
        deployment_name: str,
        work_queue_name: str,
        work_pool_name: str,
    ) -> DeploymentInfo:
        return self.create_deployment(
            entrypoint=entrypoint,
            deployment_name=deployment_name,
            work_queue_name=work_queue_name,
            work_pool_name=work_pool_name,
        )

    def trigger_run(
        self,
        deployment_ref: str,
        parameters: dict,
        work_queue_name: str,
    ) -> FlowRunInfo:
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

    def update_schedule_parameters(
        self,
        deployment_ref: str,
        parameters: dict,
    ) -> DeploymentInfo:
        deployment_id = self._resolve_deployment_id(deployment_ref)

        with self._client() as client:
            schedules = client.read_deployment_schedules(deployment_id=deployment_id)
            if not schedules:
                raise PrefectServiceError("No schedule found for deployment")
            if len(schedules) > 1:
                raise PrefectServiceError("Multiple schedules found; please keep only one schedule")

            schedule_item = schedules[0]
            schedule_create = create_deployment_schedule_create(
                schedule=schedule_item.schedule,
                active=schedule_item.active,
            )
            if hasattr(schedule_create, "model_copy"):
                schedule_create = schedule_create.model_copy(update={"parameters": parameters})
            else:
                schedule_create.parameters = parameters

            client.delete_deployment_schedule(deployment_id=deployment_id, schedule_id=schedule_item.id)
            client.create_deployment_schedules(
                deployment_id=deployment_id,
                schedules=[schedule_create],
            )

            saved = client.read_deployment(deployment_id)

        if not saved:
            raise PrefectServiceError("Updating schedule parameters succeeded but reading deployment failed")

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
        work_queue_name: str,
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
