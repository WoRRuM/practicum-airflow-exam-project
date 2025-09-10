from __future__ import annotations

from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.models import Variable


def _get_telegram_hook() -> TelegramHook:
    token = Variable.get("8069650062:AAEnV1PUkJjd65WbRCWRzTdAxw8ySFLGWOk")
    chat_id = Variable.get("322834425")
    return TelegramHook(token=token, chat_id=chat_id)


def send_telegram_success_message(context: dict) -> None:
    hook = _get_telegram_hook()
    dag_id = context["dag"].dag_id if "dag" in context else "unknown_dag"
    run_id = context.get("run_id", "unknown_run")
    ti_key = context.get("task_instance_key_str", "ti_unknown")

    message = (
        f"✅ DAG *{dag_id}*\n"
        f"Run: `{run_id}`\n"
        f"Last TI: `{ti_key}`\n\n"
        f"Статус: *SUCCESS*"
    )
    hook.send_message({"chat_id": hook.chat_id, "text": message, "parse_mode": "Markdown"})


def send_telegram_failure_message(context: dict) -> None:
    hook = _get_telegram_hook()
    dag_id = context["dag"].dag_id if "dag" in context else "unknown_dag"
    run_id = context.get("run_id", "unknown_run")
    ti_key = context.get("task_instance_key_str", "ti_unknown")
    task_id = context.get("task_instance").task_id if context.get("task_instance") else "unknown_task"
    exception = context.get("exception")

    message = (
        f"❌ DAG *{dag_id}*\n"
        f"Run: `{run_id}`\n"
        f"Task: `{task_id}`\n"
        f"TI: `{ti_key}`\n\n"
        f"Статус: *FAILURE*\n"
        f"Error: `{exception}`"
    )
    hook.send_message({"chat_id": hook.chat_id, "text": message, "parse_mode": "Markdown"})

