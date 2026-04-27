from __future__ import annotations

import smtplib
from email.message import EmailMessage

from ca_geo_weather.report import preheader_line
from ca_geo_weather.weather import GeoSeries, forecast_date_range


def send_report(
    *,
    run_date,
    body: str,
    preheader: str,
    subj: str,
    csv_by_event: dict[str, str],
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str,
    mail_from: str,
    mail_to: str,
    use_tls: bool = True,
) -> None:
    msg = EmailMessage()
    msg["Subject"] = subj
    msg["From"] = mail_from
    msg["To"] = mail_to
    msg["X-Preheader"] = preheader
    msg.set_content(body + "\n\n--\nCSV attachments (if any) are per event type.\n")

    for ev, content in sorted(csv_by_event.items()):
        fn = f"{ev}.csv"
        msg.add_attachment(
            content.encode("utf-8"),
            maintype="text",
            subtype="csv",
            filename=fn,
        )

    with smtplib.SMTP(smtp_host, smtp_port, timeout=60) as smtp:
        if use_tls:
            smtp.starttls()
        if smtp_user:
            smtp.login(smtp_user, smtp_password)
        smtp.send_message(msg)


def build_preheader_from_series(source: str, series: list[GeoSeries]) -> str:
    dates = forecast_date_range(series)
    if not dates:
        from datetime import date

        t = date.today()
        return preheader_line(source, t, t)
    return preheader_line(source, dates[0], dates[-1])
