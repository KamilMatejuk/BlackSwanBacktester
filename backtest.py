import requests
import numpy as np
import pandas as pd
from schemas_request import StartRequest, URL


def parse_urls(data: StartRequest):
    for signal in data.signals:
        signal['url'].slug = signal['url'].slug.replace('{asset}', data.asset)
        signal['url'].slug = signal['url'].slug.replace('{interval}', data.interval)
        signal['url'].slug = signal['url'].slug.replace('{start_time}', f'{data.start_time}')
        signal['url'].slug = signal['url'].slug.replace('{end_time}', f'{data.end_time}')
    return data
        
    
def get_signals_for_timerange(signals: list[dict]):
    data = None
    for signal in signals:
        url : URL = signal['url']
        response = requests.get(f'http://{url.host}:{url.port}{url.slug}')
        assert response.status_code == 200, f'Failed status {response.status_code}'
        df = pd.DataFrame(response.json())
        if data is None: data = df
        else: data = pd.merge(data, df, on='timestamp')
    return data


def run_test(signals: pd.DataFrame, model: URL, starting: float):
    results = []
    value_account = starting
    value_assets = 0
    for _, row in signals.iterrows():
        data = row.to_dict()
        del data['timestamp']
        data['value_account'] = value_account
        data['value_assets'] = value_assets
        response = requests.post(f'http://{model.host}:{model.port}{model.slug}', json=data)
        assert response.status_code == 200, f'Failed status {response.status_code}'
        action = int(response.text)
        
        if action == 1:
            # buy
            value_assets = value_account / data['price']
            value_account = 0
        elif action == -1:
            # sell
            value_account = value_assets * data['price']
            value_assets = 0
        results.append({
            'timestamp': row['timestamp'],
            'action': action,
            'value_account': value_account,
            'value_assets': value_assets,
        })
    return pd.DataFrame(results)


def get_stats(signals: pd.DataFrame, results: pd.DataFrame, starting: float):
    data = pd.merge(signals, results, on='timestamp')
    transactions = []
    transaction = {}
    value = float(starting)
    for _, row in data.iterrows():
        if row['action'] == 1:
            transaction['buy_time'] = row['timestamp']
            transaction['buy_price'] = row['price']
        if row['action'] == -1:
            transaction['type'] = 'model'
            transaction['sell_time'] = row['timestamp']
            transaction['sell_price'] = row['price']
            transaction['profit'] = row['value_account'] - value
            value = row['value_account']
            transactions.append(transaction)
            transaction = {}
    if len(transaction) > 0:
        transaction['type'] = 'automatic closing of open positions at the end of test'
        transaction['sell_time'] = data.at[len(data)-1, 'timestamp']
        transaction['sell_price'] = data.at[len(data)-1, 'price']
        account_value = data.at[len(data)-1, 'value_assets'] * transaction['sell_price']
        transaction['profit'] = account_value - value
        transactions.append(transaction)
    return transactions
    