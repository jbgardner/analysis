import os
import resend
from datetime import datetime
from dotenv import load_dotenv
from jinja2 import Environment, select_autoescape, FileSystemLoader
from .supabase_helper import get_recent_trades, get_user_emails, \
    get_weekly_sector_data, get_ticker_track_record, \
    get_users_for_notification
from .twilio_helper import send_message_notification
from .jinja_helper import env
from .formatters import email_formatter
from .constants import FROM_EMAIL


load_dotenv()
current_data = datetime.now().strftime("%Y-%m-%d")
FRONTEND_URL = os.getenv('FRONTEND_URL')
BACKEND_URL = os.getenv('BACKEND_URL')
RESEND_API_KEY = os.getenv("RESEND_API_KEY_LIVE")


DASHBOARD = f"{FRONTEND_URL}/dashboard"
SEARCH_LINK = f"{FRONTEND_URL}/search?offset=0"
PURCHASE_LINK = F"{SEARCH_LINK}&disclosed_date=2024-01-02&transaction_type=P"
SALE_LINK = F"{SEARCH_LINK}&disclosed_date=2024-01-02&transaction_type=S"
resend.api_key = RESEND_API_KEY


def daily_digest():
    """
    Generate a daily digest by pulling data from Supabase and sending an email.

    Retrieves the most recent trades using the 'get_recent_trades' function and formats the data
    for purchase and sale transactions. Uses a Jinja template ('daily_digest.html') to generate
    an HTML representation of the daily digest, which includes details about recent trades.
    Sends the generated HTML email to specified recipients.

    Returns:
        bool: True if the daily digest is generated and the email is sent successfully,
              or if there are no trades found for the day.

    Example:
        result = daily_digest()
    """
    try:
        has_trades, trades_p, trades_s = get_recent_trades()
        users = get_user_emails('D')
        template = env.get_template('daily_digest.html')

        if not has_trades or not len(users):
            print(
                "No Trades Found Today or 0 Users Found with active settings!", flush=True)
            return True

        trades = {
            'purchase': {
                'data': email_formatter(trades_p.data),
                'count': trades_p.count
            },
            'sale': {
                'data': email_formatter(trades_s.data),
                'count': trades_s.count
            }
        }
        # return trades
        for user in users:
            html = template.render(
                purchase=trades['purchase'], sale=trades['sale'],
                p_link=PURCHASE_LINK, s_link=SALE_LINK, dashboard=DASHBOARD,
                cancellation_url=f"{BACKEND_URL}/emails/cancel?email_type=d&user_id={user['id']}")
            params = {
                "from": FROM_EMAIL,
                "to": user['email'],
                "subject": "Daily Digest",
                "html": html,
            }
            email = resend.Emails.send(params)
        return True
    except Exception as error:
        print(
            f"Faild To Send Daily Digest Email on Date: {datetime.now().date()} with Exception: {str(error)}", flush=True)
        return False


def weekly_sector_report():
    try:
        data = get_weekly_sector_data()
        users = get_user_emails('W')
        template = env.get_template('weekly_sector_report.html')

        if not bool(data and any(data.values())) or not len(users):
            print(
                "No Trades Found Today or 0 Users Found with active settings!", flush=True)
            return True

        week_number = datetime.now().isocalendar()[1]
        for user in users:
            html = template.render(trades=data, week_number=week_number,
                                   search_link=SEARCH_LINK, dashboard=DASHBOARD,
                                   cancellation_url=f"{BACKEND_URL}/emails/cancel?email_type=w&user_id={user['id']}")
            params = {
                "from": FROM_EMAIL,
                "to": user['email'],
                "subject": f"Weekly Sector Report - Week {week_number}",
                "html": html,
            }

            email = resend.Emails.send(params)
        return True
    except Exception as error:
        print(
            f"Faild To Send Weekly Sector Report Email on Date: {datetime.now().date()} with Exception: {str(error)}", flush=True)
        return False


def signal_notification(items):
    try:
        for data in items:
            users = get_users_for_notification(data['ticker'], data)
            track_record = get_ticker_track_record(data['ticker'])
            template = env.get_template('signal_email.html')

            if len(users['emails']):
                formatted_data = email_formatter([data])[0]
                _type = "acquired" if data['transaction_type'].lower(
                ) == 'p' else "disposed"

                for email in users['emails']:
                    html = template.render(
                        data=formatted_data,
                        track_record=email_formatter(track_record),
                        cancellation_url=f"{FRONTEND_URL}/dashboard")

                    params = {
                        "from": FROM_EMAIL,
                        "to": email,
                        "subject": f"{data['ticker'].upper()}'s CEO just {_type} {formatted_data['total_shares']} shares!",
                        "html": html,
                    }
                    email = resend.Emails.send(params)
            else:
                print('No Users Which Ticker Inside their watchlist', flush=True)

            # Disableing Phone Notification
            # This will be fully removed in the weekend.
            # if len(users['phones']):
            #     for phone in users['phones']:
            #         send_message_notification(phone, formatted_data)
            # else:
            #     print('No Users with phone numbers')

        return True
    except Exception as error:
        print(
            f"Faild To Send Signal Email on Date: {datetime.now().date()} with Exception: {str(error)}", flush=True)
        return False
