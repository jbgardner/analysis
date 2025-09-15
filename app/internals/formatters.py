from datetime import datetime
from .constants import sectors_with_ticker, market_cap_with_ticker, sector_options, \
    market_cap_options


def get_sector(ticker):
    """
    Get the sector for a given stock ticker.

    Args:
        ticker (str): The stock ticker symbol.

    Returns:
        str: The sector category, or 'N/A' if the ticker is not found.

    Example:
        sector_category = get_sector('AAPL')
    """
    for key, value in sectors_with_ticker.items():
        if ticker in value:
            return key
    return "N/A"


def get_sector_key(sector_name):
    """
    Get the key of a sector based on its name.

    Args:
        sector_name (str): The name of the sector.

    Returns:
        int or None: The key of the sector if found, or None if not found.
    """
    for key, name in sector_options.items():
        if name == sector_name:
            return key
    return None


def get_market_cap(ticker):
    """
    Get the market capitalization for a given stock ticker.

    Args:
        ticker (str): The stock ticker symbol.

    Returns:
        str: The market capitalization category (e.g., 'Large Cap', 'Mid Cap', 'Small Cap'),
             or 'N/A' if the ticker is not found.

    Example:
        market_cap_category = get_market_cap('AAPL')
    """
    for key, value in market_cap_with_ticker.items():
        if ticker in value:
            return key
    return "N/A"


def get_market_cap_key(market_cap):
    """
    Get the key of a market_cap based on its name.

    Args:
        market_cap (str): The name of the market_cap.

    Returns:
        int or None: The key of the sector if found, or None if not found.
    """
    for key, name in market_cap_options.items():
        if name == market_cap:
            return key
    return None


def email_formatter(data):
    """
    Format insider trade data for Email.

    Args:
        data (list): List of insider trade data dictionaries.

    Returns:
        list: A list of dictionaries containing formatted insider trade data.
            Each dictionary includes the following keys:
            - 'filling'
            - 'accessionNo'
            - 'ticker'
            - 'sector'
            - 'market_cap'
            - 'periodOfReport': Formatted as '%b %d' (e.g., 'Jan 25').
            - 'transaction_type'
            - 'ceo_name'
            - 'company_name'
            - 'total_shares': Formatted with commas and rounded to 2 decimal places.
            - 'share_price': Rounded to 2 decimal places.
            - 'disclosed_date'
            - 'total_amount_spent': Formatted with commas and rounded to 0 decimal places.
            - 'total_shares_after_transaction': Formatted with commas and rounded to 2 decimal places.
            - 'change_in_shares_percentage': Rounded to 3 decimal places.
            - 'link'

    Example:
        formatted_data = get_formated_data([
            {'filling': 'example_filling', 'accessionNo': 'example_accessionNo', ...},
            {'filling': 'another_filling', 'accessionNo': 'another_accessionNo', ...},
            # ...
        ])
    """
    return [{
        "filling": item["filling"],
        "accessionNo": item["accessionNo"],
        "ticker": item["ticker"],
        "sector": item["sector"],
        "market_cap": item["market_cap"],
        "periodOfReport": datetime.fromisoformat(item["periodOfReport"]).strftime('%b %d'),
        "transaction_type": item["transaction_type"].lower(),
        "ceo_name": item["ceo_name"].title(),
        "company_name": item["company_name"].title(),
        "total_shares": '{:,}'.format(int(round(item['total_shares'], 0))),
        "share_price": round(item['share_price'], 2),
        "disclosed_date": item["disclosed_date"],
        "total_amount_spent": '{:,}'.format(int(round(item["total_amount_spent"], 0))),
        "total_shares_after_transaction": '{:,}'.format(int(round(item["total_shares_after_transaction"], 0))),
        "change_in_shares_percentage": round(item["change_in_shares_percentage"], 3),
        "link": item["link"],
    } for item in data]
    
    
def custom_notification_formatter(item):
    return {
        "ticker": item["ticker"],
        "sector": get_sector_key(item["sector"]),
        "market_cap": get_market_cap_key(item["market_cap"]),
        "transaction_type": item["transaction_type"].upper(),
        "q": item["q"],
        "total_shares": item['total_shares'],
        "share_price": item['share_price'],
        "total_amount_spent": item["total_amount_spent"],
        "total_shares_after_transaction": item["total_shares_after_transaction"],
        "change_in_shares_percentage": item["change_in_shares_percentage"],
    }