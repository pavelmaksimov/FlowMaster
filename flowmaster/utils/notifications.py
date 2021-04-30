import requests


def send_codex_telegram_message(links: list[str], message: str):
    for link in links:
        requests.post(
            link,
            data={"message": message.encode("utf-8")},
        )


def send_email_message(emails: list, subject: str, body: str):
    from email.message import EmailMessage
    import smtplib

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        # server.set_debuglevel(1)  # Необязательно; так будут отображаться данные с сервера в консоли
        server.ehlo()
        server.login(user="mail@gmail.com", password="password")

        msg = EmailMessage()
        msg.set_content(body)
        msg["From"] = "mail@gmail.com"
        msg["Subject"] = subject
        msg["To"] = emails

        server.send_message(msg, from_addr="mail@gmail.com", to_addrs=emails)
