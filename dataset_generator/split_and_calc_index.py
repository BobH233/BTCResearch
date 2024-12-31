import pandas as pd
import json
from datetime import datetime, timedelta
import os
from ta import trend, momentum, volatility, volume
import logging

def process_crypto_data(input_file, output_dir_separate, output_file_combined):
    # 设置日志配置
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # 创建输出目录（用于保存单独的 JSON 文件）
    os.makedirs(output_dir_separate, exist_ok=True)
    
    # 从输入文件加载 JSON 数据
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # 将字典列表转换为 pandas DataFrame
    df = pd.DataFrame(data)
    
    # 将 'open_time' 和 'close_time' 转换为 datetime 对象
    df['open_time'] = pd.to_datetime(df['open_time'], format='%Y-%m-%d %H:%M:%S')
    df['close_time'] = pd.to_datetime(df['close_time'], format='%Y-%m-%d %H:%M:%S')
    
    # 按 'open_time' 排序以确保时间顺序
    df.sort_values('open_time', inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    # 初始化用于分割连续时间段的变量
    segments = []
    current_segment = [df.iloc[0]]
    
    # 遍历 DataFrame，将数据分割成连续的每小时段
    for i in range(1, len(df)):
        current_time = df.iloc[i]['open_time']
        previous_time = df.iloc[i-1]['open_time']
        if current_time - previous_time == timedelta(hours=1):
            current_segment.append(df.iloc[i])
        else:
            segments.append(pd.DataFrame(current_segment))
            current_segment = [df.iloc[i]]
    # 添加最后一个段
    segments.append(pd.DataFrame(current_segment))
    
    logging.info(f"Total segments found: {len(segments)}")
    
    # 列表用于存储合并输出的数据段
    combined_processed_segments = []
    
    # 处理每个时间段
    for idx, segment in enumerate(segments):
        if len(segment) >= 99:
            seg = segment.copy()
            seg.reset_index(drop=True, inplace=True)
            
            # 检查并填补缺失值
            if seg.isnull().values.any():
                logging.warning(f"Segment {idx+1} contains missing values. Filling forward.")
                seg.fillna(method='ffill', inplace=True)
                seg.fillna(method='bfill', inplace=True)
            
            # 计算技术指标
            
            ## 1. 移动平均线 (MA7, MA25, MA99)
            seg['MA7'] = seg['close_price'].rolling(window=7).mean()
            seg['MA25'] = seg['close_price'].rolling(window=25).mean()
            seg['MA99'] = seg['close_price'].rolling(window=99).mean()
            
            ## 2. 肯特纳通道 (Keltner Channels)
            # 计算 ATR50
            atr50 = volatility.AverageTrueRange(high=seg['high_price'], low=seg['low_price'], close=seg['close_price'], window=50)
            seg['ATR50'] = atr50.average_true_range()
            # 计算 MA50
            seg['MA50'] = seg['close_price'].rolling(window=50).mean()
            # 计算肯特纳通道的上轨和下轨
            multiplier = 2.75
            seg['Keltner_Upper'] = seg['MA50'] + multiplier * seg['ATR50']
            seg['Keltner_Lower'] = seg['MA50'] - multiplier * seg['ATR50']
            
            ## 3. 相对强弱指数 (RSI)
            rsi = momentum.RSIIndicator(close=seg['close_price'], window=14)
            seg['RSI'] = rsi.rsi()
            
            ## 4. 移动平均收敛散度 (MACD)
            macd = trend.MACD(close=seg['close_price'])
            seg['MACD'] = macd.macd()
            seg['MACD_Signal'] = macd.macd_signal()
            seg['MACD_Hist'] = macd.macd_diff()
            
            ## 5. 布林带 (Bollinger Bands)
            bollinger = volatility.BollingerBands(close=seg['close_price'], window=20, window_dev=2)
            seg['Bollinger_High'] = bollinger.bollinger_hband()
            seg['Bollinger_Low'] = bollinger.bollinger_lband()
            seg['Bollinger_Middle'] = bollinger.bollinger_mavg()
            
            ## 6. 随机指标 (Stochastic Oscillator)
            stochastic = momentum.StochasticOscillator(high=seg['high_price'], low=seg['low_price'], close=seg['close_price'], window=14, smooth_window=3)
            seg['Stochastic_%K'] = stochastic.stoch()  # 修正方法调用
            seg['Stochastic_%D'] = stochastic.stoch_signal()  # 修正方法调用
            
            ## 7. 平均方向性指数 (ADX)
            adx = trend.ADXIndicator(high=seg['high_price'], low=seg['low_price'], close=seg['close_price'], window=14)
            seg['ADX'] = adx.adx()
            seg['ADX_PDI'] = adx.adx_pos()
            seg['ADX_MDI'] = adx.adx_neg()
            
            ## 8. 威廉指标 (Williams %R)
            williams = momentum.WilliamsRIndicator(high=seg['high_price'], low=seg['low_price'], close=seg['close_price'], lbp=14)  # 修正参数名
            seg['Williams_%R'] = williams.williams_r()
            
            ## 9. 资金流量指标 (Chaikin Money Flow, CMF)
            cmf = volume.ChaikinMoneyFlowIndicator(high=seg['high_price'], low=seg['low_price'], close=seg['close_price'], volume=seg['volume'], window=20)
            seg['CMF'] = cmf.chaikin_money_flow()
            
            ## 10. 资金流向指标 (Money Flow Index, MFI)
            mfi = volume.MFIIndicator(high=seg['high_price'], low=seg['low_price'], close=seg['close_price'], volume=seg['volume'], window=14)
            seg['MFI'] = mfi.money_flow_index()
            
            ## 11. 指数移动平均线 (EMA12, EMA26)
            ema12 = trend.EMAIndicator(close=seg['close_price'], window=12)
            seg['EMA12'] = ema12.ema_indicator()
            ema26 = trend.EMAIndicator(close=seg['close_price'], window=26)
            seg['EMA26'] = ema26.ema_indicator()
            
            # 丢弃前99个数据点，因为这些点无法计算 MA99
            seg = seg.iloc[99:].reset_index(drop=True)
            
            # 将 datetime 列转换为字符串，以确保 JSON 序列化
            seg['open_time'] = seg['open_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
            seg['close_time'] = seg['close_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # 将处理后的段转换为字典列表
            processed_data = seg.to_dict(orient='records')
            
            # 保存为单独的 JSON 文件
            segment_filename = os.path.join(output_dir_separate, f'segment_{idx+1}.json')
            with open(segment_filename, 'w', encoding='utf-8') as f_out:
                json.dump(processed_data, f_out, ensure_ascii=False, indent=4)
            logging.info(f"Segment {idx+1} processed and saved to '{segment_filename}'. Records: {len(processed_data)}")
            
            # 为合并输出，包含每个段的信息
            combined_processed_segments.append({
                "segment_id": idx+1,
                "start_time": seg['open_time'].iloc[0],
                "end_time": seg['close_time'].iloc[-1],
                "records": processed_data
            })
        else:
            # 段长度不足，丢弃
            logging.warning(f"Segment {idx+1} discarded due to insufficient length ({len(segment)} records).")
    
    # 保存合并后的 JSON 文件
    with open(output_file_combined, 'w', encoding='utf-8') as f_combined:
        json.dump(combined_processed_segments, f_combined, ensure_ascii=False, indent=4)
    
    logging.info(f"Processing complete. Separate segments saved to '{output_dir_separate}' and combined output saved to '{output_file_combined}'.")

if __name__ == "__main__":
    all_file = [
        '../tmp/BNBUSDT_historical_10y_klines.json',
        '../tmp/BTCUSDT_historical_10y_klines.json',
        '../tmp/DOGEUSDT_historical_10y_klines.json',
        '../tmp/ETHUSDT_historical_10y_klines.json',
        '../tmp/FIROUSDT_historical_10y_klines.json',
        '../tmp/PEPEUSDT_historical_10y_klines.json',
        '../tmp/PHAUSDT_historical_10y_klines.json',
        '../tmp/SOLUSDT_historical_10y_klines.json',
        '../tmp/XRPUSDT_historical_10y_klines.json'
    ]

    for file in all_file:
        filename = os.path.basename(file)
        prefix = filename.split("_")[0]
        output_dir = "tmp_segments"
        output_combined = f'{prefix}_output.json'
        process_crypto_data(file, output_dir, output_combined)