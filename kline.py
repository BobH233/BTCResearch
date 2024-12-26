import json
import math
import time
from datetime import datetime, timedelta, timezone
from binance.client import Client
from tqdm import tqdm

# 初始化 Binance 客户端（无需 API 密钥即可获取公开数据）
client = Client()

def get_interval_timedelta(interval):
    """
    将币安的时间间隔字符串转换为 timedelta 对象。
    
    :param interval: 时间间隔字符串，例如 '1m', '1h', '1d', '1w', '1M'
    :return: 对应的 timedelta 对象
    """
    if interval.endswith('m'):
        minutes = int(interval[:-1])
        return timedelta(minutes=minutes)
    elif interval.endswith('h'):
        hours = int(interval[:-1])
        return timedelta(hours=hours)
    elif interval.endswith('d'):
        days = int(interval[:-1])
        return timedelta(days=days)
    elif interval.endswith('w'):
        weeks = int(interval[:-1])
        return timedelta(weeks=weeks)
    elif interval.endswith('M'):
        months = int(interval[:-1])
        return timedelta(days=30*months)  # 近似处理，每月30天
    else:
        raise ValueError("Unsupported interval format")

def get_all_historical_klines(symbol, interval, start_time, end_time):
    """
    获取指定交易对在指定时间范围内的所有 K 线数据。

    :param symbol: 交易对，例如 'BTCUSDT'
    :param interval: K 线时间间隔，例如 Client.KLINE_INTERVAL_1HOUR
    :param start_time: 开始时间（datetime 对象，时区感知）
    :param end_time: 结束时间（datetime 对象，时区感知）
    :return: 按时间排序的 K 线数据列表
    """
    klines = []
    limit = 1000  # 每次请求的最大数据条数
    start_str = start_time.strftime("%d %b %Y %H:%M:%S")
    end_str = end_time.strftime("%d %b %Y %H:%M:%S")
    
    # 计算每个请求能覆盖的时间范围
    delta_per_request = get_interval_timedelta(interval) * limit
    
    # 估算总请求次数（用于进度条）
    total_delta = end_time - start_time
    total_requests = math.ceil(total_delta / delta_per_request)
    
    with tqdm(total=total_requests, desc="Fetching Klines") as pbar:
        while True:
            temp_klines = client.get_historical_klines(
                symbol=symbol,
                interval=interval,
                start_str=start_str,
                end_str=end_str,
                limit=limit
            )
            
            if not temp_klines:
                break
            
            klines.extend(temp_klines)
            
            # 获取最后一条 K 线的收盘时间
            last_close_time = temp_klines[-1][6]  # 收盘时间（毫秒）
            
            # 设置下一个请求的 start_time 为最后一条 K 线的收盘时间 +1 毫秒
            start_time_new = last_close_time + 1
            if start_time_new >= end_time.timestamp() * 1000:
                break
            start_str = datetime.fromtimestamp(start_time_new / 1000, tz=timezone.utc).strftime("%d %b %Y %H:%M:%S")
            
            pbar.update(1)
            
            # 为了避免请求过于频繁，建议添加短暂的延时
            time.sleep(0.2)
            
            # 如果返回的数据少于 limit，说明已获取完毕
            if len(temp_klines) < limit:
                break
                
    return klines

def transform_klines(klines):
    """
    将原始 K 线数据转换为包含明确字段的字典列表。

    :param klines: 原始 K 线数据列表
    :return: 转换后的 K 线数据列表
    """
    transformed = []
    for k in klines:
        k_dict = {
            "open_time": datetime.fromtimestamp(k[0]/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            "open_price": float(k[1]),
            "high_price": float(k[2]),
            "low_price": float(k[3]),
            "close_price": float(k[4]),
            "volume": float(k[5]),
            "close_time": datetime.fromtimestamp(k[6]/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            "quote_asset_volume": float(k[7]),
            "number_of_trades": int(k[8]),
            "taker_buy_base_asset_volume": float(k[9]),
            "taker_buy_quote_asset_volume": float(k[10]),
            # "ignore": k[11]  # 忽略的参数
        }
        transformed.append(k_dict)
    return transformed

def main():
    symbol = 'DOGEUSDT'  # 交易对
    interval = Client.KLINE_INTERVAL_1HOUR  # 1小时
    end_datetime = datetime.now(timezone.utc)
    start_datetime = end_datetime - timedelta(days=365 * 4)
    
    print("开始获取数据...")
    klines = get_all_historical_klines(symbol, interval, start_datetime, end_datetime)
    print(f"共获取到 {len(klines)} 条 K 线数据。")
    
    print("转换数据结构...")
    transformed_klines = transform_klines(klines)
    
    # 按时间排序（从早到晚）
    transformed_klines.sort(key=lambda x: x['open_time'])
    
    # 保存为 JSON 文件
    output_file = f'{symbol}_historical_klines.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(transformed_klines, f, ensure_ascii=False, indent=4)
    
    print(f"数据已保存到 {output_file}。")

if __name__ == "__main__":
    main()
