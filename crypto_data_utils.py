import requests
import pandas as pd
from datetime import datetime
import pytz

def get_single_ticker_data(symbol):
    """
    单个交易对的ticker数据获取
    """
    ticker_url = 'https://www.binance.com/dapi/v1/ticker/24hr?symbol={}'.format(symbol)
    try:
        res_obj = requests.get(ticker_url, timeout=15) #读取超过十五秒就跳过
    except Exception as e:
        print('error', e)
        return None

    if res_obj.status_code == 200:  #url出错，非api的问题
        json_obj = res_obj.json()
        #避免发生error
        if 'code' in json_obj:
            print('error_code')
        else:
            raw_df = pd.DataFrame(json_obj)
            raw_df['openTime'] = pd.to_datetime(beijing_time)
            raw_df['closeTime'] = pd.to_datetime(beijing_time)
            raw_df['symbol'] = symbol.replace('_', '/')
            ticker_df = raw_df
    else:
        print('error_code')

    return ticker_df

def get_tickers_data(symbols):
    '''
    解决多个交易对数据
    '''
    tickers_df = pd.DataFrame()
    for symbol in symbols:
        ticker_df = get_single_ticker_data(symbol)
        if ticker_df is None:
            continue
        tickers_df = tickers_df._append(ticker_df)
    return tickers_df

def get_single_kline_data(symbol, interval, limit): #交易对、k线类别、数据数目
    '''
    单个交易对的kline数据的获取及处理
    '''
    kline_url = 'https://www.binance.com/api/v3/klines?symbol={}&interval={}&limit={}'.format(symbol, interval, limit)
    try:
        res_obj = requests.get(kline_url, timeout=15)  # 读取超过十五秒就跳过
    except Exception as e:
        print('error', e)
        return None

    kline_df = None

    if res_obj.status_code == 200:  # url出错，非api的问题
        json_obj = res_obj.json()
        # 避免发生error
        if 'code' in json_obj:
            print('error_code')
        else:
            # 列意义
            # symbol(手动加) Open time	Open	High	Low	 Close	Volume	Close time
            # Quote asset volume	Number of trades	Taker buy base asset volume	 Taker buy quote asset volume	Ignore
            raw_df = pd.DataFrame(json_obj)
            kline_df = raw_df.copy()
            kline_df.columns = [
                'Open time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time',
                'Quote asset volume', 'Number of trades', 'Taker buy base asset volume',
                'Taker buy quote asset volume', 'Ignore'
            ]
            #两种转换时间的方式，前者是用交易所的数据，后者是用模块的上海时间
            kline_df['Open time'] = pd.to_datetime(kline_df['Open time'], unit='ms')
            #kline_df['Open time'] = pd.to_datetime(beijing_time)
            kline_df['Close time'] = pd.to_datetime(kline_df['Close time'], unit='ms')
            #在第一列加上交易对的名称
            symbol_data = [symbol] * len(kline_df)
            kline_df.insert(0, 'symbol', symbol_data)
            kline_df['symbol'] = symbol.replace('_', '/') #基于我们的输入交易对的形式是'BNBBTC'，这一行暂时没用
    else:
        print('error_code')

    return kline_df

def get_klines_data(symbols, interval = '1h', limit = 2000):
    '''
    多个交易对k线
    '''
    klines_df = pd.DataFrame()
    for symbol in symbols:
        kline_df = get_single_kline_data(symbol, interval, limit)
        if kline_df is None:
            continue
        klines_df = klines_df._append(kline_df)
    return klines_df
