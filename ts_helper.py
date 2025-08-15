import tushare as ts
import pandas as pd
import numpy as np
import dotenv
import os
from pyarrow import feather
import time
from tqdm import tqdm

dotenv.load_dotenv()

token = os.getenv('TUSHARE_TOKEN') # Ensure you have set this in your .env file
pro = ts.pro_api(token)


def simple_file_cache(file_path_or_func):
    _DEFAULT_CACHE_DIR = 'data/'
    if not os.path.exists(_DEFAULT_CACHE_DIR):
        os.makedirs(_DEFAULT_CACHE_DIR)

    def decorator(func):
        def wrapper(*args, **kwargs):
            if callable(file_path_or_func):
                file_path = file_path_or_func(*args, **kwargs)
            else:
                file_path = file_path_or_func
            if not os.path.exists(_DEFAULT_CACHE_DIR):
                os.makedirs(_DEFAULT_CACHE_DIR)
            full_path = os.path.join(_DEFAULT_CACHE_DIR, file_path)
            feather_path = full_path + '.feather'
            pkl_path = full_path + '.pkl'
            if os.path.exists(feather_path):
                return pd.read_feather(feather_path)
            elif os.path.exists(pkl_path):
                _result = pd.read_pickle(pkl_path)
                feather.write_feather(_result, feather_path, compression='zstd')
                return _result
            else:
                df = func(*args, **kwargs)
                feather.write_feather(df, feather_path, compression='zstd')
                return df
        return wrapper
    return decorator


@simple_file_cache(lambda exchange: 'future_basic_{}'.format(exchange))
def get_future_basic(exchange):
    return pro.fut_basic(exchange=exchange)


@simple_file_cache(lambda ts_code, start_date, end_date: 'index_daily_{}_{}_{}'.format(ts_code, start_date, end_date))
def _get_index_daily(ts_code, start_date=None, end_date=None):
    return pro.index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)


def get_index_daily(ts_code, start_date=None, end_date=None):
    _df = _get_index_daily(ts_code, start_date, end_date)
    _df.set_index(pd.to_datetime(_df['trade_date'], format='%Y%m%d'), inplace=True)
    _df.sort_index(inplace=True)
    return _df


@simple_file_cache(lambda ts_codes, start_date, end_date: 'fut_daily_{}_{}'.format(start_date, end_date))
def get_future_daily(ts_codes, start_date=None, end_date=None):
    result = list()
    for sym in tqdm(ts_codes):
        time.sleep(0.2)
        df = pro.fut_daily(ts_code=sym, start_date=start_date, end_date=end_date)
        result.append(df)
    return pd.concat(result, ignore_index=True)


@simple_file_cache(lambda trade_dates: 'margin_detail_{}'.format(max(trade_dates)))
def get_margin_detail(trade_dates):
    result = list()

    for d in tqdm(trade_dates):
        time.sleep(0.2)
        _df = pro.margin_detail(trade_date=d)
        result.append(_df)

    df_margin_detail = pd.concat(result, ignore_index=True)
    return df_margin_detail


@simple_file_cache('fund_basic')
def get_enhanced_index_fund_basic():
    return pro.fund_basic().query('fund_type == "股票型" and invest_type == "增强指数型"')


@simple_file_cache(lambda start_date, end_date: 'fund_nav_{}_{}'.format(start_date, end_date))
def get_enhanced_index_fund_nav(start_date, end_date):
    ts_codes = get_enhanced_index_fund_basic().ts_code.tolist()
    result = list()
    for i in tqdm(ts_codes):
        time.sleep(0.2)
        _df = pro.fund_nav(ts_code=i, start_date=start_date, end_date=end_date)
        result.append(_df)
    return pd.concat(result, ignore_index=True)


if __name__ == "__main__":
    print(get_future_basic('CFFEX'))
    