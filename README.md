# Prefect 示例项目（Server / Worker / Client）

这个项目演示了一个最小可用的 Prefect 工作流系统，包含：

- **server**：启动 Prefect Server + UI
- **executor**：启动 Prefect Worker
- **client**：任务管理（查看 / 触发一次性任务 / 注册 cron / 删除 / 查询状态）

并且 `client` 已解耦为可复用的 Python 服务层，可直接搬到 FastAPI 项目里使用。

---

## 1. 项目结构

```text
example-perfert/
├── main.py
├── requirements.txt
├── flows/
│   └── task_flow.py
├── server/
│   └── __init__.py
├── executor/
│   └── __init__.py
└── client/
    ├── __init__.py          # 兼容层 + CLI 封装
    ├── service.py           # 核心服务层（推荐直接复用）
    ├── models.py            # 返回模型
    └── exceptions.py        # 异常定义
```

---

## 2. 安装依赖

```bash
pip install -r requirements.txt
```

---

## 3. 启动顺序

> 建议按顺序在不同终端启动。

### 终端 1：启动 Server

```bash
python main.py server
```

- UI: `http://127.0.0.1:4200`
- API: `http://127.0.0.1:4200/api`

### 终端 2：启动 Worker

```bash
python main.py executor
```

默认使用：
- work pool: `task-pool`
- work queue: `task-queue`

### 终端 3：调用 Client

```bash
python main.py client list
```

---

## 4. CLI 用法

### 4.1 Action 模式

```bash
# 查看 deployment 列表
python main.py client list

# 触发一次性任务
python main.py client run "my-task"

# 注册/更新 cron 定时任务
python main.py client schedule --cron "*/5 * * * *" --timezone "Asia/Shanghai"

# 删除 deployment
python main.py clientdelete --deployment my-task-flow/task-run-deployment

# 查询任务状态
python main.py clientstatus --run-id <FLOW_RUN_ID>
```

常用参数：

- `--deployment`：默认 `my-task-flow/task-run-deployment`
- `--pool`：默认 `task-pool`（executor 使用）
- `--queue`：默认 `task-queue`（executor 使用）
- `--host` / `--port`：默认 `127.0.0.1:4200`

---

## 5. Python 代码示例（可直接搬到 FastAPI）

### 5.1 一次性任务

```python
from client.service import PrefectTaskService

svc = PrefectTaskService()
run = svc.register_one_time_run(task_name="demo-once")
print(run.id, run.state_type)
```

### 5.2 注册 cron 定时任务

```python
from client.service import PrefectTaskService

svc = PrefectTaskService()
deployment = svc.register_cron_task(
    cron="*/10 * * * *",
    timezone="Asia/Shanghai",
)
print(deployment.full_name)
```

### 5.3 查看已有任务（deployments）

```python
from client.service import PrefectTaskService

svc = PrefectTaskService()
for d in svc.list_deployments():
    print(d.full_name, d.work_pool_name, d.work_queue_name)
```

### 5.4 查询任务完成状态

```python
from client.service import PrefectTaskService

svc = PrefectTaskService()
status = svc.get_run_status("<FLOW_RUN_ID>")
print(status.state_type, status.is_terminal, status.is_completed, status.is_failed)
```

### 5.5 删除任务模板（deployment）

```python
from client.service import PrefectTaskService

svc = PrefectTaskService()
svc.delete_deployment("my-task-flow/task-run-deployment")
```

---

## 6. 默认命名约定

为避免 `default` 混淆，项目使用：

- Flow: `my-task-flow`
- Deployment: `task-run-deployment`
- Work Pool: `task-pool`
- Work Queue: `task-queue`

---

## 7. 常见问题

### Q1：Worker 启动后 Server 报 sqlite locked
建议：
- 确保 Server 与 Worker 使用不同的 `PREFECT_HOME` 目录（本项目已分离）
- 先启动 Server，再启动 Worker

### Q2：任务触发失败，提示找不到 deployment
先执行一次：

```bash
python main.py client list
```

若无 deployment，再执行：

```bash
python main.py clientschedule --cron "*/5 * * * *"
```

或直接触发一次 `run`，会自动确保 deployment 存在。

---

## 8. FastAPI 路由示例（完整 app.py）

> 这个示例直接调用 `client.service.PrefectTaskService`，可作为你项目的起点。

```python
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from client.service import PrefectTaskService
from client.exceptions import PrefectServiceError, DeploymentNotFoundError, FlowRunNotFoundError

app = FastAPI(title="Prefect Task API")
svc = PrefectTaskService()


class RunOnceRequest(BaseModel):
    task_name: str
    deployment: str = "my-task-flow/task-run-deployment"


class CronRequest(BaseModel):
    cron: str
    timezone: str = "Asia/Shanghai"
    deployment_name: str = "task-run-deployment"


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/deployments")
def list_deployments():
    try:
        items = svc.list_deployments()
        return [
            {
                "id": d.id,
                "name": d.name,
                "flow_name": d.flow_name,
                "full_name": d.full_name,
                "work_pool_name": d.work_pool_name,
                "work_queue_name": d.work_queue_name,
            }
            for d in items
        ]
    except PrefectServiceError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/runs/once")
def run_once(req: RunOnceRequest):
    try:
        run = svc.register_one_time_run(task_name=req.task_name, deployment_ref=req.deployment)
        return {
            "run_id": run.id,
            "state_type": run.state_type,
            "state_name": run.state_name,
        }
    except DeploymentNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PrefectServiceError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/runs/schedule")
def schedule(req: CronRequest):
    try:
        d = svc.register_cron_task(
            cron=req.cron,
            timezone=req.timezone,
            deployment_name=req.deployment_name,
        )
        return {
            "deployment_id": d.id,
            "full_name": d.full_name,
        }
    except PrefectServiceError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/deployments/{deployment_ref:path}")
def delete_deployment(deployment_ref: str):
    try:
        deleted = svc.delete_deployment(deployment_ref, missing_ok=False)
        return {"deleted": deleted}
    except DeploymentNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PrefectServiceError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/runs/{run_id}/status")
def get_status(run_id: str):
    try:
        s = svc.get_run_status(run_id)
        return {
            "run_id": s.flow_run_id,
            "state_type": s.state_type,
            "state_name": s.state_name,
            "is_terminal": s.is_terminal,
            "is_completed": s.is_completed,
            "is_failed": s.is_failed,
            "is_running": s.is_running,
        }
    except FlowRunNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PrefectServiceError as e:
        raise HTTPException(status_code=500, detail=str(e))
```

运行方式：

```bash
pip install fastapi uvicorn
uvicorn app:app --reload --port 8000
```

常用接口：

- `GET /deployments`
- `POST /runs/once`
- `POST /runs/schedule`
- `DELETE /deployments/{flow}/{deployment}`
- `GET /runs/{run_id}/status`
