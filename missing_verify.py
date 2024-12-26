import json
from datetime import datetime, timezone
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceRequestException
from pytz import UTC

def convert_to_timestamp(time_str):
    dt = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
    dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp() * 1000)

def check_kline_exists(client, symbol, interval, time_str):
    try:
        start_time = convert_to_timestamp(time_str)
        end_time = start_time + 3600000  # 加1小时，单位为毫秒
        
        klines = client.get_historical_klines(
            symbol=symbol,
            interval=interval,
            start_str=start_time,
            end_str=end_time,
            limit=1
        )
        
        if not klines:
            return False, None
        
        kline = klines[0]
        kline_open_time = kline[0]
        
        if kline_open_time == start_time:
            kline_data = {
                "open_time": datetime.fromtimestamp(kline[0]/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                "open_price": float(kline[1]),
                "high_price": float(kline[2]),
                "low_price": float(kline[3]),
                "close_price": float(kline[4]),
                "volume": float(kline[5]),
                "close_time": datetime.fromtimestamp(kline[6]/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                "quote_asset_volume": float(kline[7]),
                "number_of_trades": int(kline[8]),
                "taker_buy_base_asset_volume": float(kline[9]),
                "taker_buy_quote_asset_volume": float(kline[10]),
                # "ignore": kline[11]
            }
            return True, kline_data
        else:
            return False, None
        
    except BinanceAPIException as e:
        print(f"Binance API 异常: {e.message}")
        return False, None
    except BinanceRequestException as e:
        print(f"Binance 请求异常: {e.message}")
        return False, None
    except Exception as e:
        print(f"其他异常: {e}")
        return False, None

def load_missing_times(file_path):
    """
    从文件中加载缺失的时间点。
    
    :param file_path: 文件路径，每行一个时间点，格式为 'YYYY-MM-DD HH:MM:SS'
    :return: 时间点列表
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            times = [line.strip() for line in f if line.strip()]
        return times
    except FileNotFoundError:
        print(f"文件 {file_path} 未找到。")
        return []
    except Exception as e:
        print(f"读取文件时发生错误: {e}")
        return []

def main():
    client = Client()
    
    symbol = 'BTCUSDT'
    interval = Client.KLINE_INTERVAL_1HOUR
    
    missing_file = 'missing_times.txt'  # 包含缺失时间点的文件
    time_points = load_missing_times(missing_file)
    
    if not time_points:
        print("没有需要验证的时间点。")
        return
    
    print(f"开始验证 {symbol} 在 {len(time_points)} 个缺失时间点的 K 线数据是否存在...")
    
    for time_str in time_points:
        exists, data = check_kline_exists(client, symbol, interval, time_str)
        if exists:
            print(f"[存在] 时间点 {time_str} 的 K 线数据:")
            print(json.dumps(data, ensure_ascii=False, indent=4))
        else:
            print(f"[缺失] 时间点 {time_str} 的 K 线数据不存在。")
    
if __name__ == "__main__":
    main()
