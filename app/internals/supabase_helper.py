import os
import json
from datetime import datetime, timedelta
from supabase import create_client
from dotenv import load_dotenv
from .formatters import custom_notification_formatter, email_formatter, get_sector_key
from .constants import sort_options, market_cap_options, \
    sector_options, SALES_TABLE, PURCHASE_TABLE, USERS_TABLE, WATCHLIST_TABLE, \
    NOTIFICATION_TABLE


load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_PROJECT_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# ============================================
# ============================================
#               Utilities
# ============================================
# ============================================
def get_database_table(trasaction_type):
    """
    Get the appropriate database table based on the transaction type.

    Args:
        transaction_type (str): The transaction type, either 'P' for purchase or 'S' for sales.

    Returns:
        str: The name of the corresponding database table.

    Example:
        table_name = get_database_table('P')
    """
    if trasaction_type.lower() == 'p':
        return PURCHASE_TABLE
    return SALES_TABLE


def get_recent_trades():
    """
    Retrieve the most recent trades from the purchase and sales tables.

    This function queries the Supabase database to fetch the most recent trades
    recorded on the disclosed date '2024-01-02' from both the purchase and sales tables.
    It limits the result to the top 5 entries for each table.

    Returns:
        tuple: A tuple containing three elements:
            - bool: True if the operation is successful, indicating that the trades were retrieved.
            - dict: A dictionary containing the response from the purchase table query.
            - dict: A dictionary containing the response from the sales table query.
    """
    current_date = datetime.now().strftime("%Y-%m-%d")
    resp_p = supabase.table(PURCHASE_TABLE).select(
        "*", count='exact').gte('disclosed_date', current_date).limit(5).execute()
    resp_s = supabase.table(SALES_TABLE).select(
        "*", count='exact').gte('disclosed_date', current_date).limit(5).execute()

    has_trades = resp_p.count or resp_s.count
    return has_trades, resp_p, resp_s


def get_weekly_sector_data():
    """
    Retrieve weekly sector data from both purchase and sale transactions.

    Returns:
        dict: A dictionary where keys are sectors and values are dictionaries
              containing 'p' and 's' lists representing purchase and sale transactions.
    """
    # Define the start date for the week
    one_week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    # Query for purchase transactions
    query_p = supabase.table(PURCHASE_TABLE).select("*").filter(
        'disclosed_date', 'gte', one_week_ago).neq(
        'sector', 'null').neq('sector', 'N/A').order(
        'total_amount_spent', desc=True)

    # Query for sale transactions
    query_s = supabase.table(SALES_TABLE).select("*").filter(
        'disclosed_date', 'gte', one_week_ago).neq(
        'sector', 'null').neq('sector', 'N/A').order(
        'total_amount_spent', desc=True)

    # Execute both queries
    result_p = query_p.execute().data
    result_s = query_s.execute().data

    # Format the results
    formatted_result_p = email_formatter(result_p)
    formatted_result_s = email_formatter(result_s)

    # Combine 'p' and 's' results while ensuring the desired distribution
    trades_by_sector = {}

    for trade in formatted_result_p + formatted_result_s:
        sector = trade['sector']
        transaction_type = trade['transaction_type'].lower()

        if sector not in trades_by_sector:
            trades_by_sector[sector] = {
                'p': [], 's': [], 'sector_id': get_sector_key(sector)}

        if transaction_type == 'p' and len(trades_by_sector[sector]['p']) < 2:
            trades_by_sector[sector]['p'].append(trade)
        elif transaction_type == 's' and len(trades_by_sector[sector]['s']) < 1:
            trades_by_sector[sector]['s'].append(trade)

    return trades_by_sector


def cancel_email_subscription(email_type, user_id, **kwargs):
    """
    Get Users Information From supabase based on the type of email 
    and update his settings

    Args:
        email_type (str): type in [D, W, C]
        D = Daily Digest
        W = Weekly Sector Report
        C = Custom Notification
        T = Ticker Based (watchlist)
    """
    settings = get_user(user_id, "settings")['settings']
    query = supabase.table(USERS_TABLE)

    if email_type.upper() == 'D':
        settings['daily_digest'] = False
        query.update({
            'settings': settings
        }).eq('id', user_id).execute()

    elif email_type.upper() == 'W':
        settings['weekly_sector_report'] = False
        query.update({
            'settings': settings
        }).eq('id', user_id).execute()
    return True


# ============================================
# ============================================
#                   Trades
# ============================================
# ============================================
def get_insider_trades(offset, **kwargs):
    """
    Retrieve insider trades from the database based on specified filters.

    Args:
        offset (int): The offset for paginating the results.
        **kwargs: Additional keyword arguments to filter the insider trades.
            - transaction_type (str): Type of transaction (e.g., 'P' for purchase).
            - q (str): Search query to filter by company name.
            - ticker (str): Ticker symbol to filter by.
            - sector (str): Sector to filter by.
            - market_cap (str): Market capitalization to filter by.
            - share_count_min (int): Minimum total shares to filter by.
            - share_count_max (int): Maximum total shares to filter by.
            - share_price_min (float): Minimum share price to filter by.
            - share_price_max (float): Maximum share price to filter by.
            - total_amount_min (float): Minimum total amount spent to filter by.
            - total_amount_max (float): Maximum total amount spent to filter by.
            - total_share_min (int): Minimum total shares after transaction to filter by.
            - total_share_max (int): Maximum total shares after transaction to filter by.
            - ownership_increase_min (float): Minimum ownership increase percentage (for purchases).
            - ownership_increase_max (float): Maximum ownership increase percentage (for purchases).
            - sort (str): Sorting option (e.g., 'disclosed_date-asc').
            - disclosed_date (str) 2024-01-04

    Returns:
        dict: A dictionary containing the fetched data and the total count.
            - 'data': List of insider trade data.
            - 'total': Total count of insider trades matching the filters.

    Example:
        get_insider_trades(
            offset=0,
            transaction_type='P',
            ticker='AAPL',
            sector='Technology',
            share_count_min=1000,
            share_price_max=200.0,
            total_amount_min=5000.0,
            sort='disclosed_date-desc'
        )
    """
    # Initialize a query with the table name
    transaction_type = kwargs.get('transaction_type')
    TABLE = get_database_table(transaction_type)
    query = supabase.table(TABLE).select(
        "*", count='exact')

    # Extract filters from kwargs
    search_query = kwargs.get('q')
    ticker = kwargs.get('ticker')
    sector = kwargs.get('sector')
    market_cap = kwargs.get('market_cap')
    share_count_min = kwargs.get('share_count_min')
    share_count_max = kwargs.get('share_count_max')
    share_price_min = kwargs.get('share_price_min')
    share_price_max = kwargs.get('share_price_max')
    total_amount_min = kwargs.get('total_amount_min')
    total_amount_max = kwargs.get('total_amount_max')
    total_share_min = kwargs.get('total_share_min')
    total_share_max = kwargs.get('total_share_max')
    ownership_increase_min = kwargs.get('ownership_increase_min')
    ownership_increase_max = kwargs.get('ownership_increase_max')
    disclosed_date = kwargs.get('disclosed_date')

    # Apply filters to the query
    if ticker is not None:
        tickers_list = [t.strip() for t in ticker.split(',')]
        query = query.in_('ticker', tickers_list)

    if sector is not None:
        if sector in sector_options:
            sector = sector_options[sector]
            query = query.eq('sector', sector)

    if market_cap is not None:
        if market_cap in market_cap_options:
            market_cap = market_cap_options[market_cap]
            query = query.eq('market_cap', market_cap)

    if TABLE == PURCHASE_TABLE:
        if ownership_increase_min is not None:
            query = query.gte('change_in_shares_percentage',
                              ownership_increase_min)
        if ownership_increase_max is not None:
            query = query.lte('change_in_shares_percentage',
                              ownership_increase_max)

    if share_count_min is not None:
        query = query.gte('total_shares_after_transaction', share_count_min)
    if share_count_max is not None:
        query = query.lte('total_shares_after_transaction', share_count_max)
    if share_price_min is not None:
        query = query.gte('share_price', share_price_min)
    if share_price_max is not None:
        query = query.lte('share_price', share_price_max)
    if total_amount_min is not None:
        query = query.gte('total_amount_spent', total_amount_min)
    if total_amount_max is not None:
        query = query.lte('total_amount_spent', total_amount_max)
    if total_share_min is not None:
        query = query.gte('total_shares', total_share_min)
    if total_share_max is not None:
        query = query.lte('total_shares', total_share_max)
    if disclosed_date is not None:
        query = query.eq('disclosed_date', disclosed_date)
    if search_query is not None:
        query = query.ilike('q', f"%{search_query}%")

    # Extract sort column and order from the sort_option
    sort = kwargs.get('sort', None)
    if sort is not None:
        if sort in sort_options:
            sort_option = sort_options[sort]
            sort_column, sort_order = sort_option.split('-')

            # Apply sorting to the query
            query = query.order(sort_column, desc=(sort_order == 'desc'))

    query = query.offset(offset)
    query = query.limit(20)
    response = query.execute()
    return {'data': response.data, 'total': response.count}


def insert_data_into_table(items, **kwargs):
    """
    Insert data into the appropriate table in the database based on transaction type.

    Args:
        data (list): A list of dictionaries containing information to be inserted into the database.
        **kwargs: Additional keyword arguments.

    Notes:
        The 'data' dictionary should contain the following keys:
        - 'filling'
        - 'accessionNo'
        - 'ticker'
        - 'sector'
        - 'q'
        - 'market_cap'
        - 'periodOfReport'
        - 'transaction_type'
        - 'ceo_name'
        - 'company_name'
        - 'total_shares'
        - 'disclosed_date'
        - 'total_amount_spent'
        - 'total_shares_after_transaction'
        - 'change_in_shares_percentage'
        - 'link'

        If the 'transaction_type' is 'P' (Purchase), the 'data' dictionary should also include:
        - 'one_week_return'
        - 'one_month_return'
        - 'six_months_return'

    Returns:
        None: The function updates the database with the provided data.

    Example:
        insert_data_into_table({
            'filling': 'example_filling',
            'accessionNo': 'example_accessionNo',
            'ticker': 'example_ticker',
            # ... (other required keys)
        })
    """
    for data in items:
        transaction_type = data.get('transaction_type')
        TABLE = get_database_table(transaction_type)

        row = {
            "filling": data['filling'],
            "cik": data['cik'],
            "q": f"{data['ceo_name']} - {data['company_name']}",
            "share_price": data['share_price'],
            "accessionNo": data['accessionNo'],
            "ticker": data['ticker'],
            "sector": data['sector'],
            "market_cap": data['market_cap'],
            "periodOfReport": data['periodOfReport'],
            "transaction_type": data['transaction_type'],
            "ceo_name": data['ceo_name'],
            "company_name": data['company_name'],
            "total_shares": data['total_shares'],
            "disclosed_date": data['disclosed_date'],
            "total_amount_spent": data['total_amount_spent'],
            "total_shares_after_transaction": data['total_shares_after_transaction'],
            "change_in_shares_percentage": data['change_in_shares_percentage'],
            "link": data['link'],
        }

        if transaction_type.lower() == 'p':
            row['one_week_return'] = data['one_week_return']
            row['one_month_return'] = data['one_month_return']
            row['six_months_return'] = data['six_months_return']

        if transaction_type.lower() == 's':
            if row.get('one_week_return', ''):
                del row['one_week_return']
            if row.get('one_month_return', ''):
                del row['one_month_return']
            if row.get('six_months_return', ''):
                del row['six_months_return']

        supabase.table(TABLE).insert(row).execute()


def get_trades_without_return(return_type):
    """
    Refreshes the data fetch from supabase with three types of return for 
    Purchase trades.

    Args:
        return_type (str): return_type in [W, M, S]
        W = One Week Return
        M = One Month Return
        S = Semi Yearly Return
    """
    column = None
    time = None
    if return_type.upper() == "W":
        column = "one_week_return"
        time = datetime.now() - timedelta(weeks=2)

    if return_type.upper() == "M":
        column = "one_month_return"
        time = datetime.now() - timedelta(weeks=5)

    if return_type.upper() == "S":
        column = "six_months_return"
        time = datetime.now() - timedelta(weeks=7)

    res = supabase.table(PURCHASE_TABLE).select("*"
                                                ).gt('disclosed_date', f"{time.isoformat()}"
                                                     ).filter(f'{column}', 'is', 'null').limit(300
                                                                                               ).order('disclosed_date', desc=True).execute()
    return res.data


def update_trades_without_returns(data):
    return supabase.table(PURCHASE_TABLE).upsert(data).execute()


# ============================================
# ============================================
#               Users
# ============================================
# ============================================
def get_user_emails(email_type, **kwargs):
    """
    Get Users Information From supabase based on the type of email.

    Args:
        email_type (str): type in [D, W, C, T, A]
        D = Daily Digest
        W = Weekly Sector Report
        C = Custom Notification
        T = Ticker Based (watchlist)
        A = Activity Based Notification
    """
    def merge_result(list1, list2):
        return [item for sublist in [list1, list2] for item in sublist]

    if email_type == 'D':
        resp = supabase.table(USERS_TABLE).select("id, email").eq(
            'settings -> daily_digest', 'true').eq('is_active', True).execute()
        return resp.data

    elif email_type == "W":
        resp = supabase.table(USERS_TABLE).select("id, email").eq(
            'settings -> weekly_sector_report', 'true').eq('is_active', True).execute()
        return resp.data

    elif email_type == 'T':
        ticker = kwargs.get('ticker', None)
        if ticker is not None:
            resp = supabase.table(WATCHLIST_TABLE).select(
                'name, users(email, phone, watchlist_notification)').eq('users.is_active', True).like('name', f'%{ticker}%').execute()
            return resp.data
        else:
            return []

    elif email_type == 'A':
        resp_a = supabase.table(USERS_TABLE).select(
            "email, phone, settings").eq('is_active', True).execute()
        return resp_a.data


def get_user(user_id, columns=None):
    """
    Retrieve user information based on the user_id.

    Args:
        user_id (int): The unique identifier for the user.
        columns (list or None): Optional. A list of specific columns to retrieve for the user.
                                If None, retrieves all columns.

    Returns:
        list or None: A list containing the user information as dictionaries.
                      Example: [{'id': 123, 'username': 'example_user', 'email': 'user@example.com', ...}]
                      Returns None if no user with the specified user_id is found.
    """
    query = supabase.table(USERS_TABLE)

    if columns is not None:
        query = query.select(columns)
    else:
        query = query.select("*")

    query = query.eq('id', user_id).limit(1).single()
    result = query.execute().data

    return result


def get_users_for_notification(ticker, data):
    """
    Reterieve WatchList Users, Activity Users, and Custom Notification Users

    Args:
        ticker (str): Ticker Of the Company

    Returns:
        dict: A dict containing unique user emails and phone numbers to send signal
        notification email.
        Example : { emails: [], phones: [] }
    """
    res = {
        'emails': [],
        'phones': []
    }

    try:
        watch_list_users = get_user_emails("T", ticker=ticker)
        for entry in watch_list_users:
            if entry['users']['watchlist_notification']['email_notification'] and entry['users']['email'] not in res['emails']:
                res['emails'].append(entry['users']['email'])

            if entry['users']['watchlist_notification']['text_notification'] and entry['users']['phone'] not in res['phones']:
                res['phones'].append(entry['users']['phone'])
    except Exception as e:
        print(
            f'Failed To Get Users For watchlist notifications with exception: {str(e)}', flush=True)

    try:
        activity_users = get_user_emails("A")
        for user in activity_users:
            if user['settings']['email_notification'] and user['email'] not in res['emails']:
                res['emails'].append(user['email'])
            if user['settings']['text_notification'] and user['phone'] not in res['phones']:
                res['phones'].append(user['phone'])
    except Exception as e:
        print(
            f'Failed To Get Users For Activity notifications with exception: {str(e)}', flush=True)

    try:
        item = custom_notification_formatter(data)
        print(item)
        custom_resp = supabase.rpc('get_users_for_custom_notifications', {
                                   'data': item}).execute()
        for user in custom_resp.data:
            if user['settings']['email_notification'] and user['email'] not in res['emails']:
                res['emails'].append(user['email'])
            if user['settings']['text_notification'] and user['phone'] not in res['phones']:
                res['phones'].append(user['phone'])
    except Exception as e:
        print(
            f'Failed To Get Users For Custom notifications with exception: {str(e)}', flush=True)

    return res


def get_ticker_track_record(ticker):
    """
    Retrieve the track record of a specific ticker by fetching the latest 5 purchase ('p') and sale ('s') transactions.

    Args:
        ticker (str): The ticker symbol for the company.

    Returns:
        list: A list containing the data for the latest 5 purchase and sale transactions.
              Each transaction is represented as a dictionary with details like disclosed_date, sector, etc.
    """
    resp_p = supabase.table(get_database_table("p")).select(
        "*").eq('ticker', ticker).limit(5).order('disclosed_date', desc=True).execute()
    resp_s = supabase.table(get_database_table("s")).select(
        "*").eq('ticker', ticker).limit(5).order('disclosed_date', desc=True).execute()

    return resp_p.data + resp_s.data
