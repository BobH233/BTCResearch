import json
import pandas as pd
import os

def calculate_indicators(input_file):
    try:
        # 读取JSON文件
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"成功读取输入文件: {input_file}")
    except FileNotFoundError:
        print(f"错误: 找不到文件 {input_file}")
        return
    except json.JSONDecodeError as e:
        print(f"错误: 解析JSON文件时出错: {e}")
        return

    # 将数据转换为DataFrame
    df = pd.DataFrame(data)
    total_points = len(df)
    print(f"总数据点数: {total_points}")

    # 将时间字段转换为datetime对象
    try:
        df['open_time'] = pd.to_datetime(df['open_time'])
        df['close_time'] = pd.to_datetime(df['close_time'])
        print("成功将时间字段转换为 datetime 对象")
    except Exception as e:
        print(f"错误: 转换时间字段时出错: {e}")
        return

    # 确保数据按时间排序
    df = df.sort_values('open_time').reset_index(drop=True)
    print(f"排序后的数据点数: {len(df)}")

    # 计算移动平均线
    df['MA7'] = df['close_price'].rolling(window=7).mean()
    df['MA25'] = df['close_price'].rolling(window=25).mean()
    df['MA99'] = df['close_price'].rolling(window=99).mean()
    print("成功计算 MA7, MA25, MA99")

    # 计算前一个收盘价
    df['previous_close'] = df['close_price'].shift(1)

    # 计算True Range (TR)
    df['tr1'] = df['high_price'] - df['low_price']
    df['tr2'] = (df['high_price'] - df['previous_close']).abs()
    df['tr3'] = (df['low_price'] - df['previous_close']).abs()
    df['TR'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)

    # 计算ATR(50)
    df['ATR50'] = df['TR'].rolling(window=50).mean()

    # 计算MA(50)
    df['MA50'] = df['close_price'].rolling(window=50).mean()

    # 计算肯特那通道
    multiplier = 2.75
    df['KC_upper'] = df['MA50'] + multiplier * df['ATR50']
    df['KC_lower'] = df['MA50'] - multiplier * df['ATR50']
    print("成功计算肯特那通道 (KC_upper, KC_lower)")

    # 计算前后数据点数
    points_after_calculation = len(df)
    print(f"计算所有指标后的数据点数: {points_after_calculation}")

    # 舍弃无法计算所有指标的行
    required_columns = ['MA7', 'MA25', 'MA99', 'KC_upper', 'KC_lower']
    before_drop = len(df)
    df_final = df.dropna(subset=required_columns)
    after_drop = len(df_final)
    discarded_points = before_drop - after_drop
    print(f"舍弃的数据点数: {discarded_points}")
    print(f"保留的数据点数: {after_drop}")

    # 选择需要输出的列（包括原始数据和新计算的指标）
    output_columns = [
        'open_time', 'open_price', 'high_price', 'low_price', 'close_price',
        'volume', 'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume',
        'MA7', 'MA25', 'MA99', 'KC_upper', 'KC_lower'
    ]

    # 检查所有输出列是否存在
    missing_columns = set(output_columns) - set(df_final.columns)
    if missing_columns:
        print(f"错误: 缺少必要的列: {missing_columns}")
        return

    df_output = df_final[output_columns].copy()

    # 将时间字段转换回字符串格式
    df_output['open_time'] = df_output['open_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_output['close_time'] = df_output['close_time'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # 将DataFrame转换为字典列表
    output_data = df_output.to_dict(orient='records')

    # 创建输出目录（如果不存在）
    output_dir = 'dataset'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"创建输出目录: {output_dir}")

    # 获取源文件名（不含扩展名）
    base_name = os.path.splitext(os.path.basename(input_file))[0]

    # 定义输出文件路径
    output_file = os.path.join(output_dir, f"{base_name}_train.json")

    # 将结果写入新的JSON文件
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=4)
        print(f"成功写入输出文件: {output_file}")
    except Exception as e:
        print(f"错误: 写入JSON文件时出错: {e}")
        return

    # 输出详细信息
    print("\n处理详细信息:")
    print(f"原始数据点数: {total_points}")
    print(f"排序后的数据点数: {points_after_calculation}")
    print(f"舍弃的数据点数: {discarded_points}")
    print(f"保留的数据点数: {after_drop}")
    print(f"输出文件路径: {output_file}")

if __name__ == "__main__":
    input_filename = 'DOGEUSDT_historical_klines.json'
    calculate_indicators(input_filename)
