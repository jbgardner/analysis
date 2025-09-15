import json
import sys
import os
from app.internals.utils import extract_insider_trades_info_parallel, calculate_returns, extract_insider_trades_info_single
from app.app import insiderTradingApi, queryApi

args = sys.argv[1:]


def make_request(item, api_instance):
    query = {
        "query": {
            "query_string": {
                "query": f"ticker:{item['issuer']['tradingSymbol']} AND accessionNo:\"{item['accessionNo']}\""
            }
        },
        "from": "0",
        "size": "1",
        "sort": [{"filedAt": {"order": "desc"}}]
    }

    result = api_instance.get_filings(query)
    return item, result


if not os.path.isfile(f'json_data/purchase/{args[0]}.json'):
    # If it doesn't exist, create the file with an empty array
    with open(f'json_data/purchase/{args[0]}.json', 'w') as json_file:
        empty_array = []
        json.dump(empty_array, json_file)

with open('json_data/ticker_keys.json', 'r') as test:
    tickers = json.load(test)

api_instance = queryApi
offset = int(args[1])
size = 50
while True:
    query = {
        "query": {
            "query_string": {
                "query": f"reportingOwner.relationship.officerTitle:CEO* AND nonDerivativeTable.transactions.coding.code:P AND periodOfReport:[{args[0]}-01-01 TO {args[0]}-12-31]"
            },
        },
        "from": offset,
        "size": size,
        "sort": [{"filedAt": {"order": "desc"}}]
    }

    insider_trades = insiderTradingApi.get_data(query)
    final_list = []
    for item in insider_trades["transactions"]:
        if item['issuer']['tradingSymbol'] in tickers:
            try:
                print(item['issuer']['tradingSymbol'])
                item, result = make_request(item, api_instance)

                filings = result.get('filings', [])

                if len(filings):
                    item['link'] = filings[0]['linkToFilingDetails']
                    item['cik'] = filings[0]['cik']

                    if filings[0]['formType'] == "4":
                        item['form_type'] = filings[0]['formType']
                        print('Correct Data: ',
                              item['issuer']['tradingSymbol'])
                    else:
                        item['form_type'] = filings[0]['formType']
                        print('Correct Data: ',
                              item['issuer']['tradingSymbol'])

                final_list.append(item)

            except Exception as e:
                print(e)
                continue

    with open(f'json_data/purchase/{args[0]}.json', 'r') as test:
        x = json.load(test)
        x = x + final_list

    with open(f'json_data/purchase/{args[0]}.json', 'w') as json_file:
        json.dump(x, json_file, indent=4)

    total = insider_trades['total']
    offset += size

    if total['value'] <= size:
        break

    if total['value'] < offset:
        break

    print('offset = ', offset)
    print('size = ', size)
    print('total = ', total['value'])
