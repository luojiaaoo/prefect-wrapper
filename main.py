#!/usr/bin/env python
"""主入口 - 统一启动 Prefect 3个子模块"""
import argparse
import os
import sys

from client import delete_deployment, get_run_status, list_deployments, register_cron_task, register_task
from executor import run_executor
from server import run_server

# 设置默认 API URL
os.environ.setdefault("PREFECT_API_URL", "http://127.0.0.1:4200/api")


def _handle_client_mode(args):
    os.environ["PREFECT_API_URL"] = f"http://{args.host}:{args.port}/api"

    action = args.action
    if action == "run":
        if not args.task_name:
            print("❌ run 需要任务名称")
            return
        register_task(" ".join(args.task_name), deployment_name=args.deployment, timeout=args.timeout)
    elif action == "list":
        list_deployments()
    elif action == "schedule":
        if not args.cron:
            print("❌ schedule 需要 --cron")
            return
        register_cron_task(cron=args.cron, deployment_name=args.deployment.split("/")[-1], timezone=args.timezone)
    elif action == "delete":
        delete_deployment(deployment_ref=args.deployment)
    elif action == "status":
        if not args.run_id:
            print("❌ status 需要 --run-id")
            return
        get_run_status(args.run_id)


def main():
    parser = argparse.ArgumentParser(description="Prefect 完美示例")
    parser.add_argument("module", choices=["server", "executor", "client"])
    parser.add_argument("task_name", nargs="*", default=[], help="任务名称")

    parser.add_argument("--host", default="127.0.0.1", help="Server地址")
    parser.add_argument("--port", type=int, default=4200, help="Server端口")

    parser.add_argument("--pool", default="task-pool", help="Work Pool名称")
    parser.add_argument("--queue", default="task-queue", help="Work Queue名称")
    parser.add_argument("--type", default="process", help="Worker类型")

    parser.add_argument("--deployment", default="my-task-flow/task-run-deployment", help="Deployment名称")
    parser.add_argument("--timeout", type=int, default=60, help="等待超时(秒)")

    parser.add_argument("--action", choices=["run", "list", "schedule", "delete", "status"], required=True)
    parser.add_argument("--cron", default=None, help="cron 表达式，例如 '*/5 * * * *'")
    parser.add_argument("--timezone", default="UTC", help="cron 时区")
    parser.add_argument("--run-id", default=None, help="Flow Run ID")

    args = parser.parse_args()

    if args.module == "server":
        run_server(host=args.host, port=args.port)
        return

    if args.module == "executor":
        api_url = f"http://{args.host}:{args.port}/api"
        run_executor(pool_name=args.pool, work_queue=args.queue, worker_type=args.type, api_url=api_url)
        return

    if args.module == "client":
        _handle_client_mode(args)


if __name__ == "__main__":
    main()
