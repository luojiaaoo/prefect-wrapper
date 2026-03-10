import argparse
import os

from . import delete_deployment, get_run_status, list_deployments, register_cron_task, register_task


def _parse_schedule_deployment(value: str) -> str:
    deployment_ref = value.strip()
    if not deployment_ref:
        raise argparse.ArgumentTypeError("--deployment 不能为空")

    deployment_name = deployment_ref.rsplit("/", 1)[-1]
    if not deployment_name:
        raise argparse.ArgumentTypeError("--deployment 格式非法")

    return deployment_name


def _configure_client_parser(client_parser: argparse.ArgumentParser) -> None:
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
    schedule_parser.add_argument("--entrypoint", required=True, help="Flow 入口，例如 flows.task_flow:my_task_flow")

    delete_parser = client_actions.add_parser("delete", help="删除 deployment")
    delete_parser.add_argument("--deployment", required=True, help="Deployment名称或引用")

    status_parser = client_actions.add_parser("status", help="查询 Flow Run 状态")
    status_parser.add_argument("--run-id", required=True, help="Flow Run ID")


def add_client_module_parser(modules) -> None:
    client_parser = modules.add_parser("client", help="调用 Prefect Client 操作")
    _configure_client_parser(client_parser)


def handle_client_mode(args) -> None:
    os.environ["PREFECT_API_URL"] = f"http://{args.host}:{args.port}/api"

    if args.action == "run":
        register_task(" ".join(args.task_name), deployment_name=args.deployment, timeout=args.timeout)
    elif args.action == "list":
        list_deployments()
    elif args.action == "schedule":
        register_cron_task(
            cron=args.cron,
            entrypoint=args.entrypoint,
            deployment_name=args.deployment,
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
