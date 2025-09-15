## Postgres Custom Notification Function

```
CREATE OR REPLACE FUNCTION get_users_for_custom_notifications(data jsonb)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
BEGIN
    -- Get matching users based on the trade data
    RETURN (
        SELECT jsonb_agg(DISTINCT jsonb_build_object(
            'email', u.email,
            'phone', u.phone,
            'settings', cn.settings
        ))
        FROM custom_notifications cn
        JOIN users u ON cn.user_id = u.id
        WHERE
            (cn.ticker IS NULL OR cn.ticker = data->>'ticker') AND
            (cn.sector IS NULL OR cn.sector = data->>'sector') AND
            (cn.market_cap IS NULL OR cn.market_cap::text = data->>'market_cap') AND
            (cn.transaction_type IS NULL OR cn.transaction_type = data->>'transaction_type') AND
            (cn.share_price_min IS NULL OR cn.share_price_min <= (data->>'share_price')::numeric) AND
            (cn.share_price_max IS NULL OR cn.share_price_max >= (data->>'share_price')::numeric) AND
            (cn.total_amount_min IS NULL OR cn.total_amount_min <= (data->>'total_amount_spent')::numeric) AND
            (cn.total_amount_max IS NULL OR cn.total_amount_max >= (data->>'total_amount_spent')::numeric) AND
            (cn.total_share_min IS NULL OR cn.total_share_min <= (data->>'total_shares')::numeric) AND
            (cn.total_share_max IS NULL OR cn.total_share_max >= (data->>'total_shares')::numeric) AND
            (cn.share_count_min IS NULL OR cn.share_count_min <= (data->>'total_shares_after_transaction')::numeric) AND
            (cn.share_count_max IS NULL OR cn.share_count_max >= (data->>'total_shares_after_transaction')::numeric) AND
            (cn.ownership_increase_min IS NULL OR cn.ownership_increase_min <= (data->>'change_in_shares_percentage')::numeric) AND
            (cn.ownership_increase_max IS NULL OR cn.ownership_increase_max >= (data->>'change_in_shares_percentage')::numeric)
    );
END;
$$;
```

## Mapping of keys

The above Mapping has to be very carefully followed which is also provided below in an very semantic way.

```
query_mapping = {
    "ticker": "ticker",
    "sector": "sector",
    "market_cap": "market_cap",
    "transaction_type: "transaction_type",
    "share_price": "share_price_min & max",
    "total_amount_spent: "total_amount_min & max",
    "total_shares": "total_share_min & max",
    "total_share_after_transaction": "share_count_min & max",
    "change_in_share_percentage": "ownership_increase_min & max",
    "q": "q"
}
```

## Mapping for Sector & Market Cap

Inside the constants.py file the mapping for the following is defined and needs to be strictly followed throughout this application

## setting up application

1- Create venv with python 3.11
2- Activate venv and Install the requriements
3- Setup the .env file
5- Run the Following Command

```
    python main.py
```

6- Start the Streaming Module
7- Start celery worker & beat

## Commands to run the celery worker & beat

```
    celery -A worker worker --loglevel=info
    celery -A worker beat --loglevel=info
```

## Helpers

1- https://www.slingacademy.com/article/deploying-fastapi-on-ubuntu-with-nginx-and-lets-encrypt/
2- https://ahmadalsajid.medium.com/daemonizing-celery-beat-with-systemd-97f1203e7b32
3- Flower URL: http://api.ceobuysell.com:5555
