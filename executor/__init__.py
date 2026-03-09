"""Prefect Worker 执行机模块"""
import os
import subprocess
import sys

# 获取项目根目录
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Worker 使用独立目录，不与 Server 共用
PREFECT_WORKER_DIR = os.path.join(PROJECT_ROOT, ".prefect_worker")


def run_executor(
    pool_name: str = "task-pool",
    work_queue: str = "task-queue",
    worker_type: str = "process",
    api_url: str = "http://127.0.0.1:4200/api"
):
    """启动 Prefect Worker"""
    os.makedirs(PREFECT_WORKER_DIR, exist_ok=True)

    print("=" * 50)
    print("🔧 启动 Prefect Worker 执行机")
    print("=" * 50)
    print(f"📁 数据目录: {PREFECT_WORKER_DIR}")
    print(f"📦 Work Pool: {pool_name}")
    print(f"📋 Work Queue: {work_queue}")
    print(f"🔨 Worker Type: {worker_type}")
    print(f"🔗 API URL: {api_url}")
    print("=" * 50)

    # 设置环境变量
    env = os.environ.copy()
    env["PREFECT_HOME"] = PREFECT_WORKER_DIR
    env["PREFECT_API_URL"] = api_url

    # 启动 Worker
    subprocess.run([
        sys.executable, "-m", "prefect", "worker",
        "start",
        "--pool", pool_name,
        "--work-queue", work_queue,
        "--type", worker_type
    ], env=env)


if __name__ == "__main__":
    run_executor()