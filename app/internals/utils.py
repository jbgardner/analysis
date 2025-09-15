import yfinance as yf
from collections import Counter
from datetime import datetime, timedelta
from itertools import groupby
from workalendar.usa import UnitedStates
from concurrent.futures import ThreadPoolExecutor
from .formatters import get_sector, get_market_cap


# ============================================
# ============================================
#                 Helpers
# ============================================
# ============================================
def download_stock_data(ticker, start_date, end_date):
    return yf.download(ticker, start=start_date, end=end_date, threads=True)


def calculate_return(purchase_price, current_price, number_of_shares):
    """
    Calculate the return on investment.

    Parameters:
    - purchase_price: The price at which the shares were purchased.
    - current_price: The current price of the shares.
    - number_of_shares: The number of shares purchased.

    Returns:
    - The return on investment as a percentage.
    """

    return_percentage = (
        (current_price - purchase_price) / purchase_price) * 100
    return return_percentage


def calculate_returns(ticker, disclosed_date, total_shares, share_price):
    # Helper function to get the next weekday
    def get_next_business_day(calendar, date):
        while not calendar.is_working_day(date):
            date += timedelta(days=1)
        return date

    # Convert disclosed_date to datetime
    start_date = datetime.fromisoformat(disclosed_date)
    calendar = UnitedStates()

    # Calculate end dates
    start_date_week = get_next_business_day(
        calendar, start_date + timedelta(days=7)).strftime("%Y-%m-%d")
    start_date_month = get_next_business_day(
        calendar, start_date + timedelta(days=30)).strftime("%Y-%m-%d")
    start_date_six_months = get_next_business_day(
        calendar, start_date + timedelta(days=180)).strftime("%Y-%m-%d")

    # Use Yahoo Finance to download historical stock prices
    # print('working fine', ticker)

    data = download_stock_data(ticker, start_date.strftime(
        "%Y-%m-%d"), (start_date + timedelta(days=190)).strftime("%Y-%m-%d"))

    # Extract adjusted closing prices for the relevant times
    share_price = data['Adj Close'].get(
        start_date.strftime("%Y-%m-%d"), share_price)
    closing_price_week = data['Adj Close'].get(start_date_week, None)
    closing_price_month = data['Adj Close'].get(start_date_month, None)
    closing_price_six_months = data['Adj Close'].get(
        start_date_six_months, None)

    # Calculate returns
    returns = {
        'one_week_return': calculate_return(share_price, closing_price_week, total_shares) if closing_price_week is not None else None,
        'one_month_return': calculate_return(share_price, closing_price_month, total_shares) if closing_price_month is not None else None,
        'six_months_return': calculate_return(share_price, closing_price_six_months, total_shares) if closing_price_six_months is not None else None
    }

    return returns


def isLifetime(plan):
    return plan == 'lifetime'


def extract_last_item(group):
    return group[-1].get('sharesOwnedFollowingTransaction', 0) if group else 0


def get_transaction_coding(transactions):
    codings = [transaction['coding']['code'] for transaction in transactions]
    counts = Counter(codings)
    most_common = counts.most_common(1)
    if most_common:
        return most_common[0][0]
    else:
        return None


def group_transaction_by_coding(transactions):
    combined_transactions = {}

    for transaction in transactions:
        coding_code = transaction['coding']['code']

        # If the coding code is not in the combined_transactions dictionary, create a new entry
        if coding_code not in combined_transactions:
            combined_transactions[coding_code] = []

        combined_transactions[coding_code].append(transaction)
    return combined_transactions


def get_all_codings(transactions):
    codings = [transaction['coding']['code'] for transaction in transactions]
    return codings


def retrieve_subscription_amount(stripe, subscription_id):
    try:
        subscription = stripe.Subscription.retrieve(subscription_id)
        plan = subscription.get('plan', {})
        if 'amount' in plan:
            return '{:.2f}'.format(plan['amount'] / 100)
    except stripe.error.StripeError as e:
        print(f"Error retrieving subscription: {e}")
    return None


def calculate_amount(stripe, session):
    amount = None

    if session.get('mode') == 'subscription':
        subscription_id = session.get('subscription')
        if isinstance(subscription_id, str):
            amount = retrieve_subscription_amount(stripe, subscription_id)
    elif session.get('mode') == 'payment':
        total_amount = session.get('amount_total')
        if isinstance(total_amount, int):
            amount = '{:.2f}'.format(total_amount / 100)

    return amount


# ============================================
# ============================================
#                Main Functions
# ============================================
# ============================================
def extract_insider_trades_info_single(insider_trade, returns=False):
    ticker = insider_trade["issuer"]["tradingSymbol"]
    ceo_name = insider_trade["reportingOwner"]["name"]
    company_name = insider_trade["issuer"]["name"]
    total_shares = 0
    total_amount_spent = 0
    results = []

    # for transaction in transactions:
    for code, transactions in group_transaction_by_coding(insider_trade["nonDerivativeTable"]["transactions"]).items():
        if code in ["P", "S"]:
            grouped_transactions = {'D': [], 'indirect': {}}

            for transaction in transactions:
                total_shares += transaction["amounts"]["shares"]
                total_amount_spent += transaction["amounts"]["shares"] * \
                    transaction["amounts"].get("pricePerShare", 0)

                # Group transactions
                ownership_type = transaction["ownershipNature"]["directOrIndirectOwnership"]
                nature_of_ownership = transaction["ownershipNature"].get(
                    "natureOfOwnership")

                post_transaction_amounts = transaction["postTransactionAmounts"]

                if ownership_type == "I":
                    if nature_of_ownership not in grouped_transactions['indirect']:
                        grouped_transactions['indirect'][nature_of_ownership] = [
                        ]

                    grouped_transactions['indirect'][nature_of_ownership].append(
                        post_transaction_amounts)
                else:
                    grouped_transactions['D'].append(post_transaction_amounts)

            # Calculate total shares after purchase
            last_items_d = extract_last_item(grouped_transactions['D'])
            last_items_indirect = {key: extract_last_item(
                nested_group) for key, nested_group in grouped_transactions['indirect'].items()}
            last_items_combined = [last_items_d] + \
                list(last_items_indirect.values())
            total_shares_after_purchase = sum(last_items_combined)

            disclosed_date = insider_trade["filedAt"]
            share_price = insider_trade["nonDerivativeTable"]["transactions"][0]["amounts"].get(
                "pricePerShare", 0)

            if insider_trade["nonDerivativeTable"].get('holdings', False):
                if isinstance(insider_trade['nonDerivativeTable']['holdings'], list):
                    for x in insider_trade['nonDerivativeTable']['holdings']:
                        if "postTransactionAmounts" in x and "sharesOwnedFollowingTransaction" in x['postTransactionAmounts']:
                            total_shares_after_purchase += x["postTransactionAmounts"]["sharesOwnedFollowingTransaction"]

            change_in_shares_percentage = (
                total_shares / total_shares_after_purchase) * 100 if total_shares_after_purchase != 0 else 0

            trade_returns = {'one_week_return': None,
                             'one_month_return': None, 'six_months_return': None}

            results.append({
                "filling": insider_trade["id"],
                "ticker": ticker,
                "cik": insider_trade['issuer']['cik'],
                "q": f"{ceo_name} - {company_name}",
                "sector": get_sector(ticker),
                "market_cap": get_market_cap(ticker),
                "accessionNo": insider_trade['accessionNo'],
                "periodOfReport": insider_trade['periodOfReport'],
                "transaction_type": code,
                "ceo_name": ceo_name,
                "company_name": company_name,
                "total_shares": total_shares,
                "share_price": share_price,
                "disclosed_date": disclosed_date,
                "total_amount_spent": total_amount_spent,
                "total_shares_after_transaction": total_shares_after_purchase,
                "link": insider_trade.get('link', None),
                "change_in_shares_percentage": round(change_in_shares_percentage, 4),
                **trade_returns
            })
    return results


def extract_insider_trades_info_parallel(data, **kwargs):
    result = []
    max_workers = 10
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(
            extract_insider_trades_info_single, insider_trade) for insider_trade in data]
        for future in futures:
            try:
                result.append(future.result())
            except Exception as e:
                print(f"Exception in parallel processing: {e}")
    return result
