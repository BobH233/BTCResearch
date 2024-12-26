import json
from datetime import datetime, timedelta, timezone
from tqdm import tqdm

def load_klines(file_path):
    """
    从JSON文件加载K线数据。

    :param file_path: JSON文件路径
    :return: K线数据列表
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            klines = json.load(f)
        return klines
    except FileNotFoundError:
        print(f"文件 {file_path} 未找到。")
        return []
    except json.JSONDecodeError:
        print(f"文件 {file_path} 不是有效的JSON格式。")
        return []

def sort_klines(klines):
    """
    按照open_time升序排序K线数据。

    :param klines: 原始K线数据列表
    :return: 排序后的K线数据列表
    """
    return sorted(klines, key=lambda x: x['open_time'])

def validate_klines(klines):
    """
    验证K线数据的时间序列是否严格每小时递增，且无重复或缺失。

    :param klines: 排序后的K线数据列表
    :return: 验证结果及详细信息
    """
    if not klines:
        return False, "K线数据为空。"

    errors = []
    duplicates = []
    missing = []

    # 将字符串时间转换为datetime对象列表
    try:
        times = [datetime.strptime(k['open_time'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc) for k in klines]
    except ValueError as e:
        return False, f"时间格式错误: {e}"

    # 遍历时间列表，检查递增和缺失
    for i in tqdm(range(1, len(times)), desc="Validating Klines"):
        expected_time = times[i-1] + timedelta(hours=1)
        actual_time = times[i]
        if actual_time == times[i-1]:
            duplicates.append(actual_time)
        elif actual_time != expected_time:
            # 计算缺失的时间点数量
            delta = actual_time - expected_time
            missing_hours = delta // timedelta(hours=1)
            for j in range(1, missing_hours + 1):
                missing_time = expected_time + timedelta(hours=j-1)
                missing.append(missing_time.strftime('%Y-%m-%d %H:%M:%S'))

    if duplicates or missing:
        if duplicates:
            errors.append(f"发现重复的时间点: {', '.join([dt.strftime('%Y-%m-%d %H:%M:%S') for dt in duplicates])}")
        if missing:
            errors.append(f"发现缺失的时间点，共 {len(missing)} 个: {', '.join(missing[:10])} {'...' if len(missing) > 20 else ''}")
        return False, '\n'.join(errors)
    else:
        return True, "所有K线数据的open_time严格按照每小时递增，且无重复或缺失。"

def main():
    file_path = 'DOGEUSDT_historical_klines.json'  # JSON文件路径
    print(f"正在加载文件: {file_path}...")
    klines = load_klines(file_path)

    if not klines:
        return

    print("正在排序K线数据...")
    sorted_klines = sort_klines(klines)

    print("正在验证K线数据的时间序列...")
    is_valid, message = validate_klines(sorted_klines)

    if is_valid:
        print("验证通过：所有K线数据的open_time严格按照每小时递增，且无重复或缺失。")
    else:
        print("验证失败：")
        print(message)

if __name__ == "__main__":
    main()
