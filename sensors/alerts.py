import os
import smtplib
from email.message import EmailMessage

from dagster import (
    DefaultSensorStatus,
    RunFailureSensorContext,
    run_failure_sensor,
)


def _smtp_config() -> dict | None:
    """Return SMTP settings from env, or None when alerting is not configured."""
    email_from = os.environ.get("ALERT_EMAIL_FROM")
    email_to = os.environ.get("ALERT_EMAIL_TO")
    if not email_from or not email_to:
        return None
    return {
        "from": email_from,
        "to": [addr.strip() for addr in email_to.split(",") if addr.strip()],
        "password": os.environ.get("ALERT_EMAIL_PASSWORD", ""),
        "host": os.environ.get("ALERT_SMTP_HOST", "smtp.gmail.com"),
        "port": int(os.environ.get("ALERT_SMTP_PORT", "465")),
        "smtp_type": os.environ.get("ALERT_SMTP_TYPE", "SSL").upper(),
        "user": os.environ.get("ALERT_SMTP_USER", email_from),
    }


@run_failure_sensor(
    name="email_on_run_failure",
    default_status=DefaultSensorStatus.RUNNING,
)
def email_on_run_failure(context: RunFailureSensorContext) -> None:
    """Email the alert address when any run fails (no-op if email is not configured)."""
    config = _smtp_config()
    if config is None:
        context.log.warning(
            "Run failed, but email alerting is not configured "
            "(set ALERT_EMAIL_FROM and ALERT_EMAIL_TO)."
        )
        return

    run = context.dagster_run
    message = EmailMessage()
    message["Subject"] = f"[Dagster] Run failed: {run.job_name}"
    message["From"] = config["from"]
    message["To"] = ", ".join(config["to"])
    message.set_content(
        f"Job: {run.job_name}\n"
        f"Run ID: {run.run_id}\n"
        f"Error: {context.failure_event.message}\n"
    )

    try:
        if config["smtp_type"] == "SSL":
            with smtplib.SMTP_SSL(config["host"], config["port"]) as server:
                if config["password"]:
                    server.login(config["user"], config["password"])
                server.send_message(message)
        else:
            with smtplib.SMTP(config["host"], config["port"]) as server:
                server.starttls()
                if config["password"]:
                    server.login(config["user"], config["password"])
                server.send_message(message)
        context.log.info(f"Sent failure email for run {run.run_id}")
    except Exception as error:
        context.log.error(f"Failed to send failure email: {error}")
