import json
import pandas as pd
import mplfinance as mpf
from datetime import datetime, timedelta, timezone

def load_klines(file_path):
    """
    从JSON文件加载K线数据。
    
    :param file_path: JSON文件路径
    :return: pandas DataFrame 包含K线数据
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            klines = json.load(f)
    except FileNotFoundError:
        print(f"文件 {file_path} 未找到。")
        return None
    except json.JSONDecodeError:
        print(f"文件 {file_path} 不是有效的JSON格式。")
        return None
    
    # 将K线数据转换为DataFrame
    df = pd.DataFrame(klines)
    
    # 转换时间为datetime对象并设置为索引
    df['open_time'] = pd.to_datetime(df['open_time'], format='%Y-%m-%d %H:%M:%S')
    df.set_index('open_time', inplace=True)
    
    # 转换价格和其他数值为浮点数
    for col in ['open_price', 'high_price', 'low_price', 'close_price', 'volume',
                'quote_asset_volume', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']:
        df[col] = df[col].astype(float)
    
    return df

def calculate_moving_averages(df, windows=[7, 25, 99]):
    """
    计算移动平均线。
    
    :param df: pandas DataFrame 包含K线数据
    :param windows: 移动平均窗口列表
    :return: DataFrame 包含移动平均线
    """
    for window in windows:
        df[f'MA_{window}'] = df['close_price'].rolling(window=window).mean()
    return df

def filter_recent_days(df, days=4):
    """
    筛选最近几天的数据。
    
    :param df: pandas DataFrame 包含K线数据
    :param days: 天数
    :return: 筛选后的DataFrame
    """
    end_time = df.index.max()
    start_time = end_time - timedelta(days=days)
    filtered_df = df.loc[start_time:end_time]
    return filtered_df

def plot_candlestick_with_ma(df, title='BTCUSDT - Last 4 Days K-Line with MA'):
    """
    绘制K线图并叠加移动平均线。
    
    :param df: pandas DataFrame 包含K线数据和MA
    :param title: 图表标题
    """
    # 选择需要绘制的列
    plot_df = df[['open_price', 'high_price', 'low_price', 'close_price', 'volume']].copy()
    plot_df.rename(columns={
        'open_price': 'Open',
        'high_price': 'High',
        'low_price': 'Low',
        'close_price': 'Close',
        'volume': 'Volume'
    }, inplace=True)
    
    # 准备移动平均线列表
    ma_list = []
    for col in df.columns:
        if col.startswith('MA_'):
            ma_list.append(col)
    
    # 定义移动平均线样式
    addplots = [mpf.make_addplot(df[ma], color=color)
               for ma, color in zip(ma_list, ['blue', 'orange', 'green'])]
    
    # 定义图表样式
    mc = mpf.make_marketcolors(
        up='g',
        down='r',
        inherit=True
    )
    s = mpf.make_mpf_style(marketcolors=mc)
    
    # 绘制图表
    mpf.plot(plot_df, type='candle', style=s, title=title, volume=True,
             addplot=addplots, mav=(7, 25, 99), show_nontrading=False)

def main():
    file_path = 'BTCUSDT_historical_klines.json'  # JSON文件路径
    df = load_klines(file_path)
    
    if df is None:
        return
    
    # 计算移动平均线
    df = calculate_moving_averages(df)
    
    # 筛选最近4天的数据
    recent_df = filter_recent_days(df, days=4)
    
    # 检查是否有足够的数据
    if recent_df.empty:
        print("没有足够的数据来绘制最近4天的K线图。")
        return
    
    # 绘制K线图和移动平均线
    plot_candlestick_with_ma(recent_df)

if __name__ == "__main__":
    main()
