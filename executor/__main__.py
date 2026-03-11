import argparse

from . import run_executor


def main():
    parser = argparse.ArgumentParser(description="启动 Prefect Worker")
    parser.add_argument("--host", default="127.0.0.1", help="Server地址")
    parser.add_argument("--port", type=int, default=4200, help="Server端口")
    parser.add_argument("--pool", default="default-task-pool", help="Work Pool名称")
    parser.add_argument("--queue", default="default-task-queue", help="Work Queue名称")
    parser.add_argument("--type", default="process", help="Worker类型")

    args = parser.parse_args()
    api_url = f"http://{args.host}:{args.port}/api"
    run_executor(pool_name=args.pool, work_queue=args.queue, worker_type=args.type, api_url=api_url)


if __name__ == "__main__":
    main()
