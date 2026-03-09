"""Prefect Flow 定义"""
from prefect import flow, get_run_logger
import time


@flow(name="my-task-flow", log_prints=True)
def my_task_flow(task_name: str = "default-task"):
    """示例任务流"""
    time.sleep(30)
    logger = get_run_logger()

    logger.info(f"🚀 开始执行任务: {task_name}")

    # 模拟任务执行
    result = f"✅ 任务 '{task_name}' 执行完成!"

    logger.info(result)

    return result


if __name__ == "__main__":
    # 本地测试
    my_task_flow("test-task")