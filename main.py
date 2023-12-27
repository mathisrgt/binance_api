import requests
import sys
import sqlite3
import hashlib
import hmac
import time

base_url = 'https://api.binance.com'

def get_all_cryptos(api_key):
    session = requests.Session()
    session.headers.update({'X-MBX-APIKEY': api_key})

    response = session.get(f'{base_url}/api/v3/exchangeInfo')
    trading_pairs = response.json()['symbols']

    crypto_currencies = [pair['baseAsset'] for pair in trading_pairs]
    
    unique_crypto_currencies = set(crypto_currencies)

    print('Available cryptocurrencies')
    for crypto in unique_crypto_currencies:
        print(crypto)

def getDepth(api_key, direction='ask', pair='BTCUSD'):
    
    session = requests.Session()
    session.headers.update({'X-MBX-APIKEY': api_key})

    response = session.get(f'{base_url}/api/v3/depth', params={'symbol': pair})

    data = response.json()
    if direction == 'ask':
        return data['asks']
    elif direction == 'bid':
        return data['bids']
    
def getOrderBook(symbol, limit=10):
    base_url = 'https://api.binance.com'
    endpoint = '/api/v3/depth'

    params = {
        'symbol': symbol,
        'limit': limit
    }

    response = requests.get(f'{base_url}{endpoint}', params=params)
    order_book = response.json()
    return order_book

def refreshDataCandle(pair='BTCUSDT', duration='5m'):
    while True:
        try:
            candlestick_data = fetchCandlestickData(pair, duration)

            if candlestick_data:
                insertCandlestickData(candlestick_data)
                print('New candlestick data inserted.')

            time.sleep(300)
        except Exception as e:
            print(f'An error occurred: {e}')

def fetchCandlestickData(pair, duration):
    base_url = 'https://api.binance.com'
    endpoint = '/api/v3/klines'

    params = {
        'symbol': pair,
        'interval': duration
    }

    response = requests.get(f'{base_url}{endpoint}', params=params)

    candlestick_data = response.json()
    return candlestick_data

def insertCandlestickData(data):
    connection = sqlite3.connect('main.db')

    insert_data_query = '''
    INSERT INTO candlestick_data (date, high, low, open, close, volume)
    VALUES (?, ?, ?, ?, ?, ?)
    '''

    cursor = connection.cursor()
    
    for candle in data:
        cursor.execute(insert_data_query, (
            int(candle[0]),
            float(candle[2]),
            float(candle[3]),
            float(candle[1]),
            float(candle[4]),
            float(candle[5])
        ))

    connection.commit()
    connection.close()

def refreshData(pair='BTCUSDT'):
    while True:
        try:
            trade_data = fetchTradeData(pair)

            if trade_data:
                insertTradeData(trade_data)
                updateLastCheck(pair, trade_data[-1]['time'], trade_data[-1]['id'])
                print('New trade data inserted.')

            time.sleep(300)
        except Exception as e:
            print(f'An error occurred: {e}')

def fetchTradeData(pair):
    endpoint = '/api/v3/trades'

    params = {
        'symbol': pair
    }

    response = requests.get(f'{base_url}{endpoint}', params=params)
    trade_data = response.json()
    return trade_data

def insertTradeData(data):
    connection = sqlite3.connect('trade_data.db')

    create_table_query = '''
    CREATE TABLE IF NOT EXISTS trade_data (
        Id INTEGER PRIMARY KEY,
        uuid TEXT,
        traded_crypto REAL,
        price REAL,
        created_at_int INT,
        side TEXT
    )
    '''

    cursor = connection.cursor()
    cursor.execute(create_table_query)

    insert_data_query = '''
    INSERT INTO trade_data (uuid, traded_crypto, price, created_at_int, side)
    VALUES (?, ?, ?, ?, ?)
    '''

    for trade in data:
        cursor.execute(insert_data_query, (
            trade['id'],         
            trade['qty'],        
            trade['price'],      
            trade['time'],       
            trade['isBuyerMaker']
        ))

    connection.commit()
    connection.close()

def updateLastCheck(pair, last_check_timestamp, last_id):
    connection = sqlite3.connect('trade_data.db')

    update_query = '''
    INSERT OR REPLACE INTO last_checks (exchange, trading_pair, duration, table_name, last_check, startdate, last_id)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    '''

    cursor = connection.cursor()
    cursor.execute(update_query, (
        'Binance',         
        pair,              
        'N/A',             
        'trade_data',      
        last_check_timestamp,
        int(time.time()),  
        last_id            
    ))

    connection.commit()
    connection.close()

def createOrder(api_key, secret_key, direction, price, amount, pair='BTCUSDT', orderType='LIMIT'):
    endpoint = '/api/v3/order'

    params = {
        'symbol': pair,
        'side': direction.upper(),
        'type': orderType.upper(),
        'timeInForce': 'GTC',
        'quantity': amount,
        'price': price,
        'timestamp': int(time.time() * 1000)
    }

    query_string = '&'.join([f'{key}={params[key]}' for key in params])

    signature = hmac.new(secret_key.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
    
    headers = {'X-MBX-APIKEY': api_key}

    params['signature'] = signature

    response = requests.post(f'{base_url}{endpoint}', params=params, headers=headers)

    if response.status_code == 200:
        print('Order created successfully')
        order_info = response.json()
        return order_info
    else:
        print(f'Failed to create order. Status code: {response.status_code}')
        return None
    
def cancelOrder(api_key, secret_key, uuid):
    endpoint = '/api/v3/order'

    params = {
        'symbol': 'BTCUSDT',
        'orderId': uuid,
        'timestamp': int(time.time() * 1000)
    }

    query_string = '&'.join([f'{key}={params[key]}' for key in params])

    signature = hmac.new(secret_key.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

    headers = {'X-MBX-APIKEY': api_key}

    params['signature'] = signature

    response = requests.delete(f'{base_url}{endpoint}', params=params, headers=headers)

    if response.status_code == 200:
        print('Order canceled successfully')
    else:
        print(f'Failed to cancel order. Status code: {response.status_code}')

    
if __name__ == "__main__":
    api_key = sys.argv[1]
    secret_key = sys.argv[2]
    
    #REQ 1
    #get_all_cryptos(api_key)

    #REQ 2
    #direction = 'bid'
    #pair = 'BTCUSDT'
    #depth_data = getDepth(api_key, direction, pair)
    #if depth_data:
    #    print(f'{direction} price for {pair}: {depth_data[0]}')

    #REQ 3
    #symbol = 'BTCUSDT'
    #limit = 10

    #order_book_data = getOrderBook(symbol, limit)
    
    #print(f'Order book for {symbol}:')
    #print(f'Asks (sell orders): {order_book_data["asks"]}')
    #print(f'Bids (buy orders): {order_book_data["bids"]}')

    #REQ 4
    #pair = 'BTCUSDT'
    #duration = '5m'

    #candlestick_data = refreshDataCandle(pair, duration)
    
    #print(f'Candlestick data for {pair} ({duration}):')
    #for candle in candlestick_data:
    #    print(f'Open time: {candle[0]}, Open: {candle[1]}, High: {candle[2]}, Low: {candle[3]}, Close: {candle[4]}, Volume: {candle[5]}')

    #REQ 5
    #connection = sqlite3.connect('main.db')

    #create_table_query = '''
    #CREATE TABLE IF NOT EXISTS candlestick_data (
    #    Id INTEGER PRIMARY KEY,
    #    date INT,
    #    high REAL,
    #    low REAL,
    #    open REAL,
    #    close REAL,
    #    volume REAL
    #)
    #'''

    #cursor = connection.cursor()
    #cursor.execute(create_table_query)
    #connection.commit()

    #connection.close()

    #REQ 6
    #refreshDataCandle()

    #REQ 7
    connection = sqlite3.connect('trade_data.db')

    create_table_query = '''
    CREATE TABLE IF NOT EXISTS last_checks (
        Id INTEGER PRIMARY KEY,
        exchange TEXT,
        trading_pair TEXT,
        duration TEXT,
        table_name TEXT,
        last_check INT,
        startdate INT,
        last_id INT
    )
    '''

    cursor = connection.cursor()
    cursor.execute(create_table_query)
    connection.commit()
    connection.close()

    #refreshData()

    #POST ----
    #REQ 8
    direction = 'BUY'
    price = 1
    amount = 1

    order_info = createOrder(api_key, secret_key, direction, price, amount)
    if order_info:
        print('Order Info:', order_info)

    #REQ 9
    order_uuid = order_info['orderId']
    cancelOrder(api_key, secret_key, order_uuid)




