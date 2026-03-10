import argparse

from . import run_server


def main():
    parser = argparse.ArgumentParser(description="启动 Prefect Server")
    parser.add_argument("--host", default="127.0.0.1", help="Server地址")
    parser.add_argument("--port", type=int, default=4200, help="Server端口")
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--docker", action="store_true", help="使用 Docker 启动")
    mode_group.add_argument("--local", action="store_true", help="使用本地模式启动")

    args = parser.parse_args()
    use_docker = True if args.docker else False if args.local else None
    run_server(host=args.host, port=args.port, use_docker=use_docker)


if __name__ == "__main__":
    main()
