"""Alert notifications for pipeline restart (stdlib only)."""

from __future__ import annotations

import os
import smtplib
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Any


def format_pipeline_restart_alert(
    pipeline_display_name: str,
    pipeline_id: str,
    failed_at: datetime | None,
    error_message: str,
    restart_at: datetime | None = None,
) -> tuple[str, str]:
    """Return (subject, body) for a pipeline restart notification."""
    failed_ts = (
        failed_at.strftime("%Y-%m-%d %H:%M:%S %Z")
        if failed_at
        else "unknown (see process_log)"
    )
    restart_ts = (
        restart_at.strftime("%Y-%m-%d %H:%M:%S %Z")
        if restart_at
        else datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
    )
    brief_error = (error_message or "No detail in process_log").strip()
    if len(brief_error) > 500:
        brief_error = brief_error[:497] + "..."

    subject = f"[ipac_delta_sync] Pipeline FAILED — restart started: {pipeline_display_name}"
    body = (
        f"iPAC Delta Sync pipeline restart alert\n\n"
        f"Pipeline: {pipeline_display_name}\n"
        f"Pipeline ID: {pipeline_id}\n"
        f"Failed at: {failed_ts}\n"
        f"Error: {brief_error}\n\n"
        f"Action: Restart requested at {restart_ts}.\n"
        f"The continuous pipeline update API was called because process_log showed FAILED "
        f"and no active run was in progress.\n"
    )
    return subject, body


def send_alert_email(
    to_email: str,
    subject: str,
    body: str,
    from_email: str | None = None,
) -> bool:
    """
    Send alert via SMTP when SMTP_HOST is configured (env vars).
    Returns True if sent, False if SMTP not configured (caller should log body).
    """
    to_addr = str(to_email or "").strip()
    if not to_addr:
        print("WARN alert email skipped: no recipient")
        return False

    host = os.environ.get("SMTP_HOST", "").strip()
    if not host:
        return False

    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ.get("SMTP_USER", "").strip()
    password = os.environ.get("SMTP_PASSWORD", "")
    sender = (from_email or os.environ.get("SMTP_FROM", "") or user or to_addr).strip()

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = to_addr
    msg.set_content(body)

    try:
        with smtplib.SMTP(host, port, timeout=30) as smtp:
            if os.environ.get("SMTP_STARTTLS", "true").lower() in ("1", "true", "yes"):
                smtp.starttls()
            if user and password:
                smtp.login(user, password)
            smtp.send_message(msg)
        print(f"Alert email sent to {to_addr}")
        return True
    except Exception as exc:
        print(f"WARN failed to send alert email to {to_addr}: {exc}")
        return False


def notify_pipeline_restart(
    to_email: str,
    pipeline_display_name: str,
    pipeline_id: str,
    failed_at: datetime | None,
    error_message: str,
    restart_at: datetime | None = None,
) -> None:
    """Send restart alert email or print body when SMTP is not configured."""
    subject, body = format_pipeline_restart_alert(
        pipeline_display_name,
        pipeline_id,
        failed_at,
        error_message,
        restart_at=restart_at,
    )
    if send_alert_email(to_email, subject, body):
        return
    print("--- ALERT EMAIL (SMTP not configured; set SMTP_HOST etc.) ---")
    print(f"To: {to_email}")
    print(f"Subject: {subject}")
    print(body)
    print("--- end alert ---")


def configure_smtp_from_dbutils(dbutils: Any, scope: str = "ipac-alerts") -> None:
    """Optional: load SMTP_* from a Databricks secret scope into os.environ."""
    if dbutils is None:
        return
    keys = ("SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASSWORD", "SMTP_FROM", "SMTP_STARTTLS")
    for key in keys:
        secret_key = key.lower().replace("_", "-")
        try:
            value = dbutils.secrets.get(scope=scope, key=secret_key)
            if value:
                os.environ[key] = str(value)
        except Exception:
            continue
