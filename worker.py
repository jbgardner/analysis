import time
from celery import Celery
from celery.schedules import crontab
from app.internals.supabase_helper import get_trades_without_return, \
    update_trades_without_returns
from app.internals.utils import calculate_returns
from app.internals.resend_helper import daily_digest as d, \
    weekly_sector_report as w


celery = Celery(
    __name__,
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/1",
)


@celery.task(name="daily_digest")
def daily_digest_task():
    return d()


@celery.task(name="weekly_sector_report")
def weekly_sector_report():
    return w()


@celery.task(name="weekly_returns")
def weekly_returns():
    data = get_trades_without_return("W")
    final_data = [
        {**item, **calculate_returns(item['ticker'], item['disclosed_date'],
                                     item['total_shares'], item['share_price'])}
        for item in data
    ]
    update_trades_without_returns(final_data)
    return True


@celery.task(name="monthly_returns")
def monthly_returns():
    data = get_trades_without_return("M")
    final_data = [
        {**item, **calculate_returns(item['ticker'], item['disclosed_date'],
                                     item['total_shares'], item['share_price'])}
        for item in data
    ]
    update_trades_without_returns(final_data)
    return True


@celery.task(name="semi_yearly_returns")
def semi_yearly_returns():
    data = get_trades_without_return("S")
    final_data = [
        {**item, **calculate_returns(item['ticker'], item['disclosed_date'],
                                     item['total_shares'], item['share_price'])}
        for item in data
    ]
    update_trades_without_returns(final_data)
    return True


celery.conf.beat_schedule = {
    'daily-task': {
        'task': 'daily_digest',
        'schedule': crontab(minute=0, hour=23),
    },
    'weekly-returns-daily-task': {
        'task': 'weekly_returns',
        'schedule': crontab(minute=0, hour=0),
    },
    'monthly-returns-daily-task': {
        'task': 'monthly_returns',
        'schedule': crontab(minute=0, hour=0),
    },
    'semi-yearly-returns-daily-task': {
        'task': 'semi_yearly_returns',
        'schedule': crontab(minute=0, hour=0),
    },
    'weekly-task': {
        'task': 'weekly_sector_report',
        'schedule': crontab(minute=0, hour=0, day_of_week=1),
    },
}
