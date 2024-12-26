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
    
    # 重命名列以符合mplfinance的要求
    df.rename(columns={
        'open_price': 'Open',
        'high_price': 'High',
        'low_price': 'Low',
        'close_price': 'Close',
        'volume': 'Volume'
    }, inplace=True)
    
    # 确保数值列为浮点数
    for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
        df[col] = df[col].astype(float)
    
    return df

def calculate_moving_averages(df, windows=[7, 25, 50, 99]):
    """
    计算移动平均线和Keltner Channels。
    
    :param df: pandas DataFrame 包含K线数据
    :param windows: 移动平均窗口列表
    :return: DataFrame 包含移动平均线和Keltner Channels
    """
    # 计算简单移动平均线（SMA）
    for window in windows:
        df[f'MA_{window}'] = df['Close'].rolling(window=window).mean()
    
    # 计算True Range (TR)
    df['Previous_Close'] = df['Close'].shift(1)
    df['TR'] = df[['High', 'Low', 'Previous_Close']].apply(
        lambda x: max(x['High'] - x['Low'], abs(x['High'] - x['Previous_Close']), abs(x['Low'] - x['Previous_Close'])),
        axis=1
    )
    
    # 计算ATR（Average True Range）
    df['ATR_50'] = df['TR'].rolling(window=50).mean()
    
    # 计算肯特那通道的上轨和下轨
    df['Upper_Channel'] = df['MA_50'] + (df['ATR_50'] * 2.75)
    df['Lower_Channel'] = df['MA_50'] - (df['ATR_50'] * 2.75)
    
    # 删除辅助列
    df.drop(['Previous_Close', 'TR'], axis=1, inplace=True)
    
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

def plot_candlestick_with_ma_kc(df, title='BTCUSDT - Last 4 Days K-Line with MA and Keltner Channels'):
    """
    绘制K线图并叠加移动平均线和Keltner Channels。
    
    :param df: pandas DataFrame 包含K线数据、移动平均线和Keltner Channels
    :param title: 图表标题
    """
    # 准备移动平均线和Keltner Channels的列表
    ma_list = [col for col in df.columns if col.startswith('MA_') and col != 'MA_50']
    kc_upper = df['Upper_Channel']
    kc_lower = df['Lower_Channel']
    
    # 重命名列以符合mplfinance的要求
    plot_df = df[['Open', 'High', 'Low', 'Close', 'Volume']].copy()
    
    # 定义移动平均线样式
    addplots = [
        mpf.make_addplot(df['MA_7'], color='blue', width=1.0, label='MA 7'),
        mpf.make_addplot(df['MA_25'], color='orange', width=1.0, label='MA 25'),
        mpf.make_addplot(df['MA_99'], color='green', width=1.0, label='MA 99'),
        mpf.make_addplot(kc_upper, color='purple', linestyle='--', width=1.0, label='Upper Keltner'),
        mpf.make_addplot(kc_lower, color='purple', linestyle='--', width=1.0, label='Lower Keltner')
    ]
    
    # 定义图表样式
    mc = mpf.make_marketcolors(
        up='g',
        down='r',
        inherit=True
    )
    s = mpf.make_mpf_style(marketcolors=mc)
    
    # 定义图例
    my_legends = ['MA 7', 'MA 25', 'MA 99', 'Upper Keltner', 'Lower Keltner']
    
    # 绘制图表
    mpf.plot(
        plot_df,
        type='candle',
        style=s,
        title=title,
        volume=True,
        addplot=addplots,
        ylabel='Price (USD)',
        ylabel_lower='Volume',
        figsize=(14, 8),
        datetime_format='%Y-%m-%d %H:%M',
        tight_layout=True,
        scale_width_adjustment=dict(volume=0.2)
    )
    
def main():
    file_path = 'BTCUSDT_historical_klines.json'  # JSON文件路径
    df = load_klines(file_path)
    
    if df is None:
        return
    
    # 计算移动平均线和Keltner Channels
    df = calculate_moving_averages(df)
    
    # 筛选最近4天的数据
    recent_df = filter_recent_days(df, days=6)
    
    # 检查是否有足够的数据来计算 MA 99 和 Keltner Channels
    required_length = 99  # MA 99需要至少99个数据点
    if len(recent_df) < required_length:
        print(f"警告：数据点不足以计算 MA 99 和 Keltner Channels（需要至少 {required_length} 个数据点）。")
    
    # 绘制K线图、移动平均线和Keltner Channels
    plot_candlestick_with_ma_kc(recent_df)

if __name__ == "__main__":
    main()
