import argparse
import os

from . import (
    cancel_schedule,
    create_deployment,
    delete_deployment,
    get_run_status,
    list_deployments,
    trigger_run,
    update_schedule,
)


def _parse_schedule_deployment(value: str) -> str:
    deployment_ref = value.strip()
    if not deployment_ref:
        raise argparse.ArgumentTypeError("--deployment 不能为空")

    if "/" in deployment_ref:
        raise argparse.ArgumentTypeError("--deployment 仅支持 deployment 名，不支持 flow/name")

    return deployment_ref


def _configure_client_parser(client_parser: argparse.ArgumentParser) -> None:
    client_parser.add_argument("--host", default="127.0.0.1", help="Server地址")
    client_parser.add_argument("--port", type=int, default=4200, help="Server端口")
    client_actions = client_parser.add_subparsers(dest="action", required=True)

    trigger_parser = client_actions.add_parser("trigger", help="触发一次性任务")
    trigger_parser.add_argument("task_name", nargs="+", help="任务名称")
    trigger_parser.add_argument("--deployment", required=True, help="Deployment名称或引用")
    trigger_parser.add_argument("--timeout", type=int, default=60, help="等待超时(秒)")

    client_actions.add_parser("list", help="列出所有 deployments")

    schedule_update_parser = client_actions.add_parser("schedule-update", help="更新 cron 定时任务")
    schedule_update_parser.add_argument("--cron", required=True, help="cron 表达式，例如 '*/5 * * * *'")
    schedule_update_parser.add_argument(
        "--deployment",
        type=_parse_schedule_deployment,
        required=True,
        help="Deployment 名称或引用，例如 task-run-deployment 或 flow/task-run-deployment",
    )
    schedule_update_parser.add_argument("--timezone", default=None, help="时区，例如 Asia/Shanghai")

    schedule_cancel_parser = client_actions.add_parser("schedule-cancel", help="取消 cron 定时任务")
    schedule_cancel_parser.add_argument(
        "--deployment",
        type=_parse_schedule_deployment,
        required=True,
        help="Deployment 名称或引用，例如 task-run-deployment 或 flow/task-run-deployment",
    )

    create_parser = client_actions.add_parser("create", help="创建或更新 deployment")
    create_parser.add_argument("--deployment", required=True, help="Deployment名称或引用")
    create_parser.add_argument("--entrypoint", required=True, help="Flow 入口，例如 flows.task_flow:my_task_flow")

    delete_parser = client_actions.add_parser("delete", help="删除 deployment")
    delete_parser.add_argument("--deployment", required=True, help="Deployment名称或引用")

    status_parser = client_actions.add_parser("status", help="查询 Flow Run 状态")
    status_parser.add_argument("--run-id", required=True, help="Flow Run ID")


def add_client_module_parser(modules) -> None:
    client_parser = modules.add_parser("client", help="调用 Prefect Client 操作")
    _configure_client_parser(client_parser)


def handle_client_mode(args) -> None:
    os.environ["PREFECT_API_URL"] = f"http://{args.host}:{args.port}/api"

    if args.action == "trigger":
        trigger_run(
            " ".join(args.task_name),
            deployment_name=args.deployment,
        )
    elif args.action == "list":
        list_deployments()
    elif args.action == "schedule-update":
        update_schedule(
            deployment_ref=args.deployment,
            cron=args.cron,
            timezone=args.timezone,
        )
    elif args.action == "schedule-cancel":
        cancel_schedule(deployment_ref=args.deployment)
    elif args.action == "create":
        create_deployment(
            deployment_name=args.deployment,
            entrypoint=args.entrypoint,
        )
    elif args.action == "delete":
        delete_deployment(deployment_ref=args.deployment)
    elif args.action == "status":
        get_run_status(args.run_id)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prefect Client 操作")
    _configure_client_parser(parser)
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    handle_client_mode(args)
