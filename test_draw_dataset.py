import json
import pandas as pd
import mplfinance as mpf
from datetime import datetime, timedelta
import os

def load_train_data(file_path):
    """
    从JSON文件加载训练数据。

    :param file_path: JSON文件路径
    :return: pandas DataFrame 包含训练数据
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"成功读取输入文件: {file_path}")
    except FileNotFoundError:
        print(f"错误: 文件 {file_path} 未找到。")
        return None
    except json.JSONDecodeError as e:
        print(f"错误: 解析JSON文件时出错: {e}")
        return None

    # 将数据转换为DataFrame
    df = pd.DataFrame(data)
    print(f"总数据点数: {len(df)}")

    # 将时间字段转换为datetime对象并设置为索引
    try:
        df['open_time'] = pd.to_datetime(df['open_time'], format='%Y-%m-%d %H:%M:%S')
        df.set_index('open_time', inplace=True)
        print("成功将 'open_time' 转换为 datetime 对象并设置为索引")
    except Exception as e:
        print(f"错误: 转换时间字段时出错: {e}")
        return None

    # 确保价格和其他数值列为浮点数
    numeric_columns = [
        'open_price', 'high_price', 'low_price', 'close_price', 'volume',
        'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume',
        'MA7', 'MA25', 'MA99', 'KC_upper', 'KC_lower'
    ]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        else:
            print(f"警告: 列 '{col}' 不存在于数据中。")

    return df

def filter_last_n_days(df, days=6):
    """
    筛选最后N天的数据。

    :param df: pandas DataFrame 包含训练数据
    :param days: 天数
    :return: 筛选后的DataFrame
    """
    end_time = df.index.max()
    start_time = end_time - timedelta(days=days)
    filtered_df = df.loc[start_time:end_time].copy()
    print(f"绘制数据的时间范围: {start_time} 至 {end_time}")
    print(f"筛选后的数据点数 (最后{days}天): {len(filtered_df)}")
    return filtered_df

def plot_candlestick_with_indicators(df, title='BTCUSDT - Last 6 Days K-Line with MA and Keltner Channels'):
    """
    绘制K线图并叠加移动平均线和肯特那通道。

    :param df: pandas DataFrame 包含训练数据和指标
    :param title: 图表标题
    """
    if df.empty:
        print("警告: 数据为空，无法绘制图表。")
        return

    # 准备绘制的数据
    plot_df = df[['open_price', 'high_price', 'low_price', 'close_price', 'volume']].copy()
    plot_df.rename(columns={
        'open_price': 'Open',
        'high_price': 'High',
        'low_price': 'Low',
        'close_price': 'Close',
        'volume': 'Volume'
    }, inplace=True)

    # 准备移动平均线和肯特那通道的 addplot 列表
    addplots = []

    # 添加MA7, MA25, MA99
    ma_colors = {'MA7': 'blue', 'MA25': 'orange', 'MA99': 'green'}
    for ma, color in ma_colors.items():
        if ma in df.columns:
            addplots.append(mpf.make_addplot(df[ma], color=color, width=1))

    # 添加肯特那通道
    if 'KC_upper' in df.columns and 'KC_lower' in df.columns:
        addplots.append(mpf.make_addplot(df['KC_upper'], color='red', linestyle='--', width=1))
        addplots.append(mpf.make_addplot(df['KC_lower'], color='red', linestyle='--', width=1))

    # 定义图表样式
    mc = mpf.make_marketcolors(
        up='g',
        down='r',
        inherit=True
    )
    s = mpf.make_mpf_style(marketcolors=mc)

    # 定义移动平均线参数（如果使用mav）
    mav = tuple([7, 25, 99])  # 即使已添加 addplot，这里保留以确保兼容性

    # 绘制图表
    mpf.plot(
        plot_df,
        type='candle',
        style=s,
        title=title,
        volume=True,
        addplot=addplots,
        mav=mav,
        show_nontrading=False,
        datetime_format='%Y-%m-%d %H:%M',
        xrotation=15,
        tight_layout=True
    )

    # 添加图例
    # mplfinance 不直接支持为 addplot 添加标签，需手动创建图例
    import matplotlib.pyplot as plt

    # 获取当前图形
    ax = plt.gca()

    # 创建图例
    from matplotlib.lines import Line2D
    legend_elements = [
        Line2D([0], [0], color='blue', lw=1, label='MA7'),
        Line2D([0], [0], color='orange', lw=1, label='MA25'),
        Line2D([0], [0], color='green', lw=1, label='MA99'),
        Line2D([0], [0], color='red', lw=1, linestyle='--', label='KC Upper'),
        Line2D([0], [0], color='red', lw=1, linestyle='--', label='KC Lower')
    ]
    ax.legend(handles=legend_elements, loc='upper left')

    # 显示图表
    plt.show()

def main():
    # 定义输入文件路径
    input_file = os.path.join('dataset', 'BTCUSDT_historical_klines_train.json')

    # 加载数据
    df = load_train_data(input_file)

    if df is None:
        return

    # 筛选最后6天的数据
    last_6_days_df = filter_last_n_days(df, days=6)

    # 检查是否有足够的数据
    if last_6_days_df.empty:
        print("没有足够的数据来绘制最后6天的K线图。")
        return

    # 绘制K线图和指标
    plot_candlestick_with_indicators(last_6_days_df)

if __name__ == "__main__":
    main()
