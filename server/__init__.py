"""Prefect Server 模块 - 启动服务 + UI"""
import os
import subprocess
import sys
import time

# 获取项目根目录
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PREFECT_DATA = os.path.join(PROJECT_ROOT, ".prefect_data")


def is_docker_running():
    """检查 Docker 是否运行"""
    try:
        result = subprocess.run(["docker", "info"], capture_output=True, timeout=5)
        return result.returncode == 0
    except Exception:
        return False


def run_server_docker(host: str = "127.0.0.1", port: int = 4200):
    """使用 Docker 启动 Prefect Server (推荐，避免 SQLite 锁定问题)"""
    if not is_docker_running():
        print("❌ Docker 未运行，请先启动 Docker Desktop")
        return

    print("=" * 50)
    print("🐳 使用 Docker 启动 Prefect Server")
    print("=" * 50)
    print(f"📊 UI 地址: http://{host}:{port}")
    print(f"📡 API 地址: http://{host}:{port}/api")
    print("=" * 50)

    # 停止并移除旧容器
    subprocess.run(["docker", "stop", "prefect-server"], capture_output=True)
    subprocess.run(["docker", "rm", "prefect-server"], capture_output=True)

    # 启动新容器
    subprocess.run([
        "docker", "run", "-d", "--rm",
        "--name", "prefect-server",
        "-p", f"{port}:4200",
        "prefecthq/prefect:3-latest",
        "--",
        "prefect", "server", "start",
        "--host", "0.0.0.0",
    ])

    print("⏳ 等待 Server 启动...")
    time.sleep(5)
    print("✅ Server 启动完成!")
    print(f"📊 访问 http://localhost:{port}")


def run_server_local(host: str = "127.0.0.1", port: int = 4200):
    """本地启动 Prefect Server (SQLite，有并发锁定问题)"""
    print("=" * 50)
    print("⚠️  本地模式启动 (可能有 SQLite 锁定问题)")
    print("=" * 50)
    print(f"📊 UI 地址: http://{host}:{port}")
    print(f"📡 API 地址: http://{host}:{port}/api")
    print("💡 推荐: 使用 Docker 模式 (python -m server --docker)")
    print("=" * 50)

    os.makedirs(PREFECT_DATA, exist_ok=True)
    os.environ["PREFECT_HOME"] = PREFECT_DATA

    subprocess.run([
        sys.executable, "-m", "prefect", "server", "start",
        "--host", host,
        "--port", str(port)
    ], env={**os.environ, "PREFECT_HOME": PREFECT_DATA})


def run_server(host: str = "127.0.0.1", port: int = 4200, use_docker: bool = None):
    """启动 Prefect Server"""
    if use_docker is None:
        use_docker = is_docker_running()

    if use_docker:
        run_server_docker(host, port)
    else:
        run_server_local(host, port)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4200)
    parser.add_argument("--docker", action="store_true", help="使用 Docker 启动")
    parser.add_argument("--local", action="store_true", help="使用本地启动")
    args = parser.parse_args()

    use_docker = args.docker if args.docker else (not args.local and is_docker_running())
    run_server(args.host, args.port, use_docker)