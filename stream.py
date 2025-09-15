import socketio
import asyncio
import websockets
import time
import json
from app.internals.utils import extract_insider_trades_info_single, get_all_codings
from app.internals.resend_helper import signal_notification
from app.internals.constants import sectors_with_ticker, officer_titles
from app.internals.supabase_helper import insert_data_into_table
from app.app import SEC_API_KEY, insiderTradingApi

API_KEY = SEC_API_KEY
SERVER_URL = "wss://stream.sec-api.io"

WS_ENDPOINT = SERVER_URL + "?apiKey=" + API_KEY


async def fetch_insider_trades(accessionNo, ticker, link):
    await asyncio.sleep(15)  # Introduce a delay of 15 seconds
    insider_trades = insiderTradingApi.get_data({
        "query": {
            "query_string": {
                "query": f"accessionNo:{accessionNo}"
            },
        },
        "from": 0,
        "size": 1,
        "sort": [{"filedAt": {"order": "desc"}}]
    })

    print("====================================")
    print("====================================")
    print(
        f"FETCHED {insider_trades['total']['value']} RECORDS FROM INSIDER TRADE API FOR TICKER = {ticker}")

    if insider_trades['total']['value']:
        data = insider_trades['transactions'][0]
        is_officer = data.get('reportingOwner', {}).get(
            'relationship', {}).get('isOfficer', False)

        if is_officer:
            officer_title = data.get('reportingOwner', {}).get(
                'relationship', {}).get('officerTitle', "")
            codings = get_all_codings(
                data["nonDerivativeTable"]["transactions"])
            is_sale_or_purchase = "P" in codings or "S" in codings

            if (officer_title.find("CEO") != -1 or officer_title in officer_titles) and is_sale_or_purchase:
                print("====================================")
                print("====================================")
                print("FOUND A TRADE MADE BY A CEO")
                print("====================================")
                print("====================================")
                formated_data = extract_insider_trades_info_single(data)
                for d in formated_data:
                    d['link'] = link

                try:
                    print("====================================")
                    print("====================================")
                    print("SENDING NOTIFICATION TO USERS")
                    signal_notification(formated_data)

                except Exception as e:
                    print(
                        f"Failed To Send Notifcation For accessionNo: {accessionNo}")

                try:
                    print("====================================")
                    print("====================================")
                    print("INSERTING DATA INTO TRADE TABLE")
                    insert_data_into_table(formated_data)

                except Exception as e:
                    print(
                        f"Failed To Insert Data for : accessionNo: {accessionNo}")

            else:
                print("====================================")
                print("====================================")
                print(
                    f"THIS TRADE WAS MADE BY {officer_title} WITH CODING = {codings}, SO SKIPPING!")
                print("====================================")
                print("====================================")
        else:
            print("====================================")
            print("====================================")
            print(f"THIS TRADE WAS NOT MADE BY A OFFICER, SO SKIPPING!")
            print("====================================")
            print("====================================")

    else:
        print("====================================")
        print("====================================")
        print(
            f"GOT NO RESPONSE FOR THAT PARTICULAR ACCESSION-NUMBER = {accessionNo} WITH TICKER {ticker}")
        print("====================================")
        print("====================================")


async def on_filings(filing):
    try:
        form_type = filing.get('formType', "")
        ticker = filing.get('ticker', "")
        company_name = filing.get('companyName', "")
        accessionNo = filing.get('accessionNo', "")
        link = filing.get('linkToFilingDetails', "")

        if form_type in ["4", "4/A"] and accessionNo:
            print("====================================")
            print("====================================")
            print(
                f"JUST GOT A FORM TYPE 4 TRADE FOR TICKER {ticker} WITH NAME {company_name}")
            print("====================================")
            print("====================================")

            # Call the fetch_insider_trades coroutine
            await fetch_insider_trades(accessionNo, ticker, link)

    except Exception as e:
        print(f'Process Failed With the following Exception: {str(e)}')


async def send_ping(websocket):
    while True:
        try:
            pong_waiter = await websocket.ping()
            # Wait for a pong within 5 seconds
            await asyncio.wait_for(pong_waiter, timeout=5)
            await asyncio.sleep(30)  # Send ping every 30 seconds
        except Exception as e:
            print(f"An error occurred while sending ping: {e}")
            await websocket.close()  # Close the connection if an error occurs
            return


async def main():
    retry_counter = 0
    max_retries = 10
    ping_task = None

    while retry_counter < max_retries:
        try:
            async with websockets.connect(WS_ENDPOINT) as websocket:
                print("✅ Connected to:", SERVER_URL)
                retry_counter = 0
                # Start the ping/pong keep-alive routine
                ping_task = asyncio.create_task(send_ping(websocket))
                # Start the message-receiving loop
                while True:
                    message = await websocket.recv()
                    filings = json.loads(message)
                    for f in filings:
                        await on_filings(f)

        except Exception as e:
            retry_counter += 1

            print(f"Connection closed with message: {e}")
            print(
                f"Reconnecting in 5 sec... (Attempt {retry_counter}/{max_retries})")

            # Cancel the ping task
            if ping_task is not None:
                try:
                    ping_task.cancel()
                    await ping_task
                except Exception:
                    pass  # Ignore any exceptions
                ping_task = None

            await asyncio.sleep(5)  # Wait for 5 seconds before reconnecting

    print("Maximum reconnection attempts reached. Stopping client.")

# Run the main coroutine
asyncio.run(main())
