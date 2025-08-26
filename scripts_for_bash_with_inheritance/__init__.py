import os
import requests
from notifiers import get_notifier


LIST_MONTHS: list = [
    "январь",
    "февраль",
    "март",
    "апрель",
    "май",
    "июнь",
    "июль",
    "август",
    "сентябрь",
    "октябрь",
    "ноябрь",
    "декабрь"
]


def get_my_env_var(var_name: str) -> str:
    try:
        return os.environ[var_name]
    except KeyError as e:
        raise MissingEnvironmentVariable(f"{var_name} does not exist") from e


class MissingEnvironmentVariable(Exception):
    pass

def send_email_notifiers(message: str, subject: str = "Уведомление от системы экспорта"):
    """
    Отправка email через Mail.ru
    """
    try:
        email = get_notifier('email')
        email.notify(
            to=get_my_env_var('RECIPIENT_EMAIL'),
            subject=subject,
            message=message,
            from_=get_my_env_var('EMAIL_USER'),
            host='smtp.mail.ru',
            port=587,
            username=get_my_env_var('EMAIL_USER'),
            password=get_my_env_var('EMAIL_PASSWORD'),
            tls=True
        )
        print(f"Email успешно отправлен на {get_my_env_var('RECIPIENT_EMAIL')}")
    except Exception as e:
        print(f"Ошибка при отправке email: {e}")


def telegram(message):
    # teg = get_notifier('telegram')
    # teg.notify(token=get_my_env_var('TOKEN'), chat_id=get_my_env_var('CHAT_ID'), message=message)
    chat_id = get_my_env_var('CHAT_ID')
    token = get_my_env_var('TOKEN_TELEGRAM')
    topic = get_my_env_var('TOPIC')
    message_id = get_my_env_var('ID')
    # teg.notify(token=get_my_env_var('TOKEN'), chat_id=get_my_env_var('CHAT_ID'), message=message)
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    params = {"chat_id": f"{chat_id}/{topic}", "text": message,
              'reply_to_message_id': message_id}
    send_email_notifiers(params.get('text'))# Добавляем /2 для указания второго подканала
    response = requests.get(url, params=params)
