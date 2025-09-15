import os
import json
import stripe
from typing import Optional, Annotated
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sec_api import InsiderTradingApi, FullTextSearchApi, QueryApi
from logsnag import LogSnag


# Constants & Types
from app.internals.types import CheckoutSession, RequestOtp, VerifyOtp
from app.internals.twilio_helper import send_verifiction_otp, verify_otp, send_message_notification
from app.internals.constants import sort_options, plan_names, USERS_TABLE
from app.internals.supabase_helper import supabase, get_insider_trades, \
    cancel_email_subscription, get_trades_without_return, \
    update_trades_without_returns, insert_data_into_table, get_recent_trades
from app.internals.utils import isLifetime, calculate_returns, calculate_amount
from app.internals.resend_helper import signal_notification

app = FastAPI()
load_dotenv()

# GET ENVS
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
SEC_API_KEY = os.getenv("SEC_API_KEY")
FRONTEND_URL = os.getenv('FRONTEND_URL')
LOG_SNAG_API = os.getenv('LOGSNAG_APIKEY')
LOG_SNAG_PROJECT = os.getenv('LOGSNAG_PROJECT')
BACKEND_URL = os.getenv('BACKEND_URL')
APP_NAME = os.getenv('APP_NAME')
FROM_EMAIL = os.getenv("FROM_EMAIL")
STRIPE_PLANS = {
    'monthly': os.getenv('PRICE_MONTHLY'),
    'yearly': os.getenv('PRICE_YEARLY'),
    'lifetime': os.getenv("PRICE_LIFETIME")
}

insiderTradingApi = InsiderTradingApi(SEC_API_KEY)
queryApi = QueryApi(SEC_API_KEY)
logsnag = LogSnag(token=LOG_SNAG_API, project=LOG_SNAG_PROJECT)


origins = [
    "*",
    "http://localhost:8000",
    "http://192.168.100.4:3700"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def status():
    return f"The API is working fine - {datetime.now()}".upper()


# ============================================
# ============================================
#                 STRIPE APIs
# ============================================
# ============================================
@app.post('/stripe/checkout-session/')
async def stripe_checkout_session(data: CheckoutSession):
    try:
        stripe.api_key = STRIPE_SECRET_KEY
        mode = "payment" if isLifetime(data.plan) else 'subscription'
        session = stripe.checkout.Session.create(
            cancel_url=f"{FRONTEND_URL}/",
            allow_promotion_codes=True,
            success_url=f"{BACKEND_URL}" + "/stripe/subscription-success?session_id={CHECKOUT_SESSION_ID}" +
            f"&user_id={data.user_id}",
            line_items=[
                {
                    "price": STRIPE_PLANS[data.plan],
                    "quantity": 1
                }
            ],
            mode=mode,
            metadata={
                "plan": data.plan,
                "plan_verbose": plan_names[data.plan]
            }
        )
        return JSONResponse(status_code=200, content={
            "session_url": session.url,
            "session_id": session.id
        })
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})


@app.get("/stripe/subscription-success")
async def subscription_success(session_id: str, user_id: str):
    try:
        stripe.api_key = STRIPE_SECRET_KEY
        session = stripe.checkout.Session.retrieve(session_id)

        plan = session['metadata']['plan']
        plan_full_name = session['metadata']['plan_verbose']

        subscription = session['subscription'] if not isLifetime(
            plan) else None

        supabase.table(USERS_TABLE).update({
            "customer_id": session['customer'],
            "subscription_id": subscription,
            "is_active": True,
            "plan": plan_full_name,
            "has_lifetime_access": isLifetime(plan)
        }).eq('user_id', user_id).execute()

        # send logsnag notification
        try:
            amount = calculate_amount(stripe, session)
            customer_name = session.get('customer_details', {}).get("name", "")
            customer_email = session.get(
                'customer_details', {}).get("email", "")
            lifetime_text = 'Lifetime' if isLifetime(plan) else ""
            description = f"Hooray! New Customer {customer_name} bought {lifetime_text} Subscription for ${amount} on {APP_NAME}."

            logsnag.track(
                channel="new-subscription",
                event="New Subscription",
                user_id=user_id,
                description=description,
                icon="💰",
                notify=True,
                tags={
                        'app': APP_NAME,
                        'date': str(datetime.now().date()),
                        'customer_name': customer_name,
                        'customer_email': customer_email
                }
            )

        except Exception as e:
            print(
                f"Failed To Send Log Snag Notification Due To Following Exception: {str(e)}")

        return RedirectResponse(url=FRONTEND_URL)
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})


@app.get('/stripe/customer_portal')
async def create_stripe_portal(customer_id: str):
    try:
        stripe.api_key = STRIPE_SECRET_KEY
        portal_session = stripe.billing_portal.Session.create(customer=customer_id,
                                                              return_url=FRONTEND_URL,)
        return JSONResponse(status_code=200, content={
            "session_url": portal_session.url,
        })
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})


@app.get('/stripe/verify-subscription')
async def verify_subscription(user_id: str, subscription_id: str):
    try:
        stripe.api_key = STRIPE_SECRET_KEY
        subscriptions = stripe.Subscription.retrieve(subscription_id)

        if subscriptions['status'] == "active":
            return JSONResponse(status_code=200, content={
                "status": True,
                "message": "Subscription Successfully verified."
            })

        supabase.table(USERS_TABLE).update({
            'is_active': False
        }).eq('id', user_id).execute()
        return JSONResponse(status_code=200, content={
            "status": False,
            "message": "User Does not have an active subscriptions."
        })

    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})


# ============================================
# ============================================
#                 Search APIs
# ============================================
# ============================================
@app.get("/search")
async def search(
    sort: int = 1,
    ticker: str = None,
    q: str = None,
    sector: int = None,
    market_cap: int = None,
    share_count_min: int = None,
    share_count_max: int = None,
    share_price_min: float = None,
    share_price_max: float = None,
    total_amount_min: int = None,
    total_amount_max: int = None,
    total_share_min: int = None,
    total_share_max: int = None,
    ownership_increase_min: float = None,
    ownership_increase_max: float = None,
    offset: int = 0,
    disclosed_date: str = None,
    transaction_type: str = "P"
):
    try:
        result = get_insider_trades(offset,
                                    ticker=ticker,
                                    q=q,
                                    sort=sort,
                                    sector=sector,
                                    market_cap=market_cap,
                                    share_count_min=share_count_min,
                                    share_count_max=share_count_max,
                                    share_price_min=share_price_min,
                                    share_price_max=share_price_max,
                                    total_amount_min=total_amount_min,
                                    total_amount_max=total_amount_max,
                                    total_share_min=total_share_min,
                                    total_share_max=total_share_max,
                                    ownership_increase_min=ownership_increase_min,
                                    ownership_increase_max=ownership_increase_max,
                                    transaction_type=transaction_type,
                                    disclosed_date=disclosed_date
                                    )

        return JSONResponse(status_code=200, content={**{
            'meta': {
                'offset': offset,
                'length': len(result['data']),
                'total': {
                    'value': result['total']
                }
            },
            'data': result['data']
        }})

    except Exception as e:
        return JSONResponse(status_code=200, content={'error': str(e)})


# ============================================
# ============================================
#                EMAIL & MESSAGES
# ============================================
# ============================================
@app.get("/emails/cancel")
async def cancellation(email_type: str, user_id: int, ticker: str = None):
    try:
        cancel_email_subscription(email_type, user_id, ticker=ticker)
        return RedirectResponse(url=FRONTEND_URL)

    except Exception as e:
        return JSONResponse(status_code=200, content={'error': str(e)})


@app.post('/phone/request_verification/')
async def request_verification(data: RequestOtp):
    try:
        status = send_verifiction_otp(data.phone)
        return JSONResponse(status_code=200, content={**{
            'status': status
        }})

    except Exception as e:
        print(f"FAILED TO SEND OPT WITH FOLLOWING EXCEPTION: {str(e)}")
        return JSONResponse(status_code=200, content={'error': "Failed To Send OTP Try Again."})


@app.post('/phone/verfiy/')
async def verfiy(data: VerifyOtp):
    try:
        status = verify_otp(data.phone, data.code)
        return JSONResponse(status_code=200, content={**{
            'status': status
        }})

    except Exception as e:
        print(f"FAILED TO VERIFY OPT WITH FOLLOWING EXCEPTION: {str(e)}")
        return JSONResponse(status_code=200, content={'error': "Failed To Verify OTP! Try Again."})


# ============================================
# ============================================
#                  TESTING
# ============================================
# ============================================
@app.get("/test_email")
async def test_email():
    try:
        data = [{
            "accessionNo": "0001494730-20-0000011",
            "market_cap": "Large Cap",
            "sector": "Consumer Cyclical",
            "ticker": "TSLA",
            "q": "Musk elon - Tesla inc.",
            "periodOfReport": "2020-02-14",
            "transaction_type": "P",
            "ceo_name": "Musk elon",
            "company_name": "Tesla, inc.",
            "total_shares": 13037.0,
            "share_price": 767.0,
            "disclosed_date": "2020-02-19",
            "total_amount_spent": 9999379.0,
            "total_shares_after_transaction": 34098597.0,
            "change_in_shares_percentage": 0.038200000000000005,
            "one_week_return": -15.109764339668818,
            "one_month_return": -53.398658498219554,
            "six_months_return": 100.08720722087266,
            "filling": "1412572528299d2cb25ee85941ac2f36",
            "link": "https://www.sec.gov/Archives/edgar/data/1318605/000149473020000001/xslF345X03/edgardoc.xml",
            "cik": "1318605",
        }]
        # data = signal_email_message(data)
        # json_str = json.dumps(data)
        # data_ = signal_notification(data)
        # data = get_trades_without_return("S")
        # final_data = []

        # for item in data:
        #     returns = calculate_returns(item['ticker'], item['disclosed_date'],
        #                       item['total_shares'], item['share_price'])
        #     item['one_week_return'] = returns['one_week_return']
        #     item['one_month_return'] = returns['one_month_return']
        #     item['six_months_return'] = returns['six_months_return']
        #     final_data.append(item)

        # s = update_trades_without_returns(final_data)
        # send_verifiction_otp("+16316060389")
        # send_message_notification("+16316060389", data)
        # signal_notification(data)
        # 563709
        # has_trades, a, b = get_recent_trades()
        # data = daily_digest()
        # data = weekly_sector_report()
        return JSONResponse(status_code=200, content={**{
            'data': True
        }})

    except Exception as e:
        return JSONResponse(status_code=200, content={'error': str(e)})
