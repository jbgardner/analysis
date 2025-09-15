import os
from dotenv import load_dotenv
from twilio.rest import Client
from .formatters import email_formatter

load_dotenv()
T_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID')
T_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN')
T_VERIFY_SID = os.getenv('TWILIO_VERIFY_SID')
T_MESSAGE_SID = os.getenv('TWILIO_MESSAGE_SID')


client = Client(T_ACCOUNT_SID, T_AUTH_TOKEN)


def send_verifiction_otp(phone_number):
    verification = client.verify.v2.services(T_VERIFY_SID) \
        .verifications \
        .create(to=phone_number, channel="sms")
    return verification.status


def verify_otp(phone_number, code):
    verification_check = client.verify \
        .v2 \
        .services(T_VERIFY_SID) \
        .verification_checks \
        .create(to=phone_number, code=code)
    return verification_check.status


def send_message_notification(phone_number, data):
    try:
        transaction_text = "purchased" if data['transaction_type'].lower(
        ) == 'p' else 'Sold'
        body = f"{data['ceo_name']}, the CEO of {data['company_name']}, {transaction_text} {data['total_shares']} shares at ${data['share_price']} for a total amount of ${data['total_amount_spent']}. The transaction took place on {data['periodOfReport']}. The CEO now owns {data['total_shares_after_transaction']} shares ({data['change_in_shares_percentage']}%)"
        print(body)
        message = client.messages \
            .create(
                body=body,
                messaging_service_sid=T_MESSAGE_SID,
                to=phone_number,
                from_="+18886715703"
            )
    except Exception as e:
        print(
            f'FAILED TO SEND SMS NOTIFICATION TO USERS WITH THE FOLLOWING EXCEPTION: {str(e)}', flush=True)
