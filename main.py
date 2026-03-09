#!/usr/bin/env python
"""主入口 - 统一启动 Prefect 3个子模块"""
import argparse
import os

from client import delete_deployment, get_run_status, list_deployments, register_cron_task, register_task
from executor import run_executor
from server import run_server

# 设置默认 API URL
os.environ.setdefault("PREFECT_API_URL", "http://127.0.0.1:4200/api")


def _parse_schedule_deployment(value: str) -> str:
    deployment_ref = value.strip()
    if not deployment_ref:
        raise argparse.ArgumentTypeError("--deployment 不能为空")

    deployment_name = deployment_ref.rsplit("/", 1)[-1]
    if not deployment_name:
        raise argparse.ArgumentTypeError("--deployment 格式非法")

    return deployment_name


def _handle_client_mode(args):
    os.environ["PREFECT_API_URL"] = f"http://{args.host}:{args.port}/api"

    if args.action == "run":
        register_task(" ".join(args.task_name), deployment_name=args.deployment, timeout=args.timeout)
    elif args.action == "list":
        list_deployments()
    elif args.action == "schedule":
        register_cron_task(cron=args.cron, deployment_name=args.deployment, timezone=args.timezone)
    elif args.action == "delete":
        delete_deployment(deployment_ref=args.deployment)
    elif args.action == "status":
        get_run_status(args.run_id)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prefect 完美示例")
    modules = parser.add_subparsers(dest="module", required=True)

    server_parser = modules.add_parser("server", help="启动 Prefect Server")
    server_parser.add_argument("--host", default="127.0.0.1", help="Server地址")
    server_parser.add_argument("--port", type=int, default=4200, help="Server端口")
    server_mode_group = server_parser.add_mutually_exclusive_group()
    server_mode_group.add_argument("--docker", action="store_true", help="使用 Docker 启动")
    server_mode_group.add_argument("--local", action="store_true", help="使用本地模式启动")

    executor_parser = modules.add_parser("executor", help="启动 Prefect Worker")
    executor_parser.add_argument("--host", default="127.0.0.1", help="Server地址")
    executor_parser.add_argument("--port", type=int, default=4200, help="Server端口")
    executor_parser.add_argument("--pool", default="task-pool", help="Work Pool名称")
    executor_parser.add_argument("--queue", default="task-queue", help="Work Queue名称")
    executor_parser.add_argument("--type", default="process", help="Worker类型")

    client_parser = modules.add_parser("client", help="调用 Prefect Client 操作")
    client_parser.add_argument("--host", default="127.0.0.1", help="Server地址")
    client_parser.add_argument("--port", type=int, default=4200, help="Server端口")
    client_actions = client_parser.add_subparsers(dest="action", required=True)

    run_parser = client_actions.add_parser("run", help="触发一次性任务")
    run_parser.add_argument("task_name", nargs="+", help="任务名称")
    run_parser.add_argument("--deployment", default="my-task-flow/task-run-deployment", help="Deployment名称")
    run_parser.add_argument("--timeout", type=int, default=60, help="等待超时(秒)")

    client_actions.add_parser("list", help="列出所有 deployments")

    schedule_parser = client_actions.add_parser("schedule", help="注册 cron 定时任务")
    schedule_parser.add_argument("--cron", required=True, help="cron 表达式，例如 '*/5 * * * *'")
    schedule_parser.add_argument(
        "--deployment",
        type=_parse_schedule_deployment,
        default="my-task-flow/task-run-deployment",
        help="Deployment 名称或引用，例如 task-run-deployment 或 flow/task-run-deployment",
    )
    schedule_parser.add_argument("--timezone", default="UTC", help="cron 时区")

    delete_parser = client_actions.add_parser("delete", help="删除 deployment")
    delete_parser.add_argument("--deployment", required=True, help="Deployment名称或引用")

    status_parser = client_actions.add_parser("status", help="查询 Flow Run 状态")
    status_parser.add_argument("--run-id", required=True, help="Flow Run ID")

    return parser


def main():
    parser = _build_parser()
    args = parser.parse_args()

    if args.module == "server":
        use_docker = True if args.docker else False if args.local else None
        run_server(host=args.host, port=args.port, use_docker=use_docker)
        return

    if args.module == "executor":
        api_url = f"http://{args.host}:{args.port}/api"
        run_executor(pool_name=args.pool, work_queue=args.queue, worker_type=args.type, api_url=api_url)
        return

    if args.module == "client":
        _handle_client_mode(args)


if __name__ == "__main__":
    main()
