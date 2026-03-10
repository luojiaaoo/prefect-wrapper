from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class DeploymentInfo:
    id: str
    name: str
    flow_name: str
    work_pool_name: Optional[str] = None
    work_queue_name: Optional[str] = None


@dataclass
class FlowRunInfo:
    id: str
    deployment_id: Optional[str]
    name: Optional[str]
    state_type: Optional[str]
    state_name: Optional[str]
    created: Optional[datetime]
    expected_start_time: Optional[datetime]


@dataclass
class RunStatusInfo:
    flow_run_id: str
    state_type: str
    state_name: str
    is_terminal: bool
    is_completed: bool
    is_failed: bool
    is_running: bool
