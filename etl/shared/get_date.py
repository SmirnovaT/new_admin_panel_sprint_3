import datetime


def date_now() -> str:
    """Функция получение текущего времени минус одна секунда"""
    now = datetime.datetime.now()
    one_second_ago = now - datetime.timedelta(seconds=1)
    return str(one_second_ago)
