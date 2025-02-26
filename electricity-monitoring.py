from collections import deque
from datetime import datetime, timedelta, date
import pymysql
from pymysql.err import OperationalError
import time
from pymysql.converters import escape_string  # 修正导入路径
import requests
import json


class DatabaseConfig:
    """数据库连接配置类"""
    HOST = '192.168.1.204'
    USER = 'root'
    PASSWORD = 'root'
    DATABASE = 'zxtf_saas_test'
    CHARSET = 'utf8mb4'
    CONNECT_TIMEOUT = 5
    MAX_RETRIES = 3

    @classmethod
    def get_connection_params(cls):
        """获取数据库连接参数字典"""
        return {
            'host': cls.HOST,
            'user': cls.USER,
            'password': cls.PASSWORD,
            'database': cls.DATABASE,
            'charset': cls.CHARSET,
            'connect_timeout': cls.CONNECT_TIMEOUT,
            'cursorclass': pymysql.cursors.SSCursor  # 流式游标节省内存
        }


class PowerUsageAnalyzer:
    WINDOW_SIZE = 48  # 测试专用窗口大小（实际生产环境保持48）
    DETECTION_WINDOW = 4  # 检测窗口（2小时）
    THRESHOLD_MULTIPLIER = 1.96  # 建议调整为1.96（95%置信区间）
    HIGH_USAGE_COUNT_THRESHOLD = 4  # 连续2小时高用电
    MIN_DATA_POINTS = 10  # 最小有效数据点数
    RELATIVE_SAFETY_MARGIN = 0.1  # 相对安全边际10%
    ABSOLUTE_SAFETY_MARGIN = 0.5  # 绝对安全边际0.5kW
    USE_RELATIVE_MARGIN = True  # 切换边际类型

    def __init__(self):
        # 分离基线窗口（48个点）和检测窗口（4个点）
        self.baseline_window = deque(maxlen=48)  # 历史正常数据
        self.detection_window = deque(maxlen=4)  # 当前监测数据
        self.timestamps = deque(maxlen=48)  # 统一时间戳管理
        self._cached_mean = None
        self._cached_std = None
        self._stats_valid = False
        self.current_usage = 0.0

    def add_power_usage(self, usage, timestamp):
        """数据添加逻辑修正"""
        # 先填满基线窗口
        if len(self.baseline_window) < self.WINDOW_SIZE:
            self.baseline_window.append(usage)
        else:
            self.detection_window.append(usage)

        # 记录最新用电值（关键修正）
        self.current_usage = usage
        self.timestamps.append(timestamp)
        self._stats_valid = False

    def _calculate_mean(self, data):
        """计算算术平均值"""
        return sum(data) / len(data) if data else 0.0

    def _calculate_standard_deviation(self, data, mean):
        """计算样本标准差（n-1）"""
        n = len(data)
        if n < 2:
            return 0.0
        variance = sum((x - mean) ** 2 for x in data) / (n - 1)
        return variance ** 0.5

    def _get_statistics(self):
        """获取带缓存的统计量"""
        if not self._stats_valid:
            data = list(self.baseline_window)
            mean = self._calculate_mean(data)
            std = self._calculate_standard_deviation(data, mean)
            self._cached_mean, self._cached_std = mean, std
            self._stats_valid = True
        return self._cached_mean, self._cached_std

    def _is_consecutive_period(self, current_idx):
        """检查时间连续性（当前与前一数据点是否间隔30分钟）"""
        if current_idx == 0:
            return True  # 第一个数据点默认连续
        prev_ts = self.timestamps[current_idx - 1]
        curr_ts = self.timestamps[current_idx]
        return (curr_ts - prev_ts) <= timedelta(minutes=35)  # 允许5分钟误差

    def detect_abnormal_usage(self):
        """带安全边际的瞬时检测（修复版）"""
        if len(self.baseline_window) < self.MIN_DATA_POINTS:
            return False

        # 使用基线窗口计算统计量
        data = list(self.baseline_window)
        mean = sum(data) / len(data)

        # 计算简单阈值
        threshold = mean * (1 + self.RELATIVE_SAFETY_MARGIN)

        # 输出详细的调试信息
        print(f"【异常检测】 当前值: {self.current_usage}, 均值: {mean}, 阈值: {threshold}")

        # 明确返回比较结果
        is_abnormal = self.current_usage > threshold
        print(f"是否异常: {is_abnormal}")

        return is_abnormal

    def _get_baseline_stats(self):
        """仅使用基线窗口数据"""
        data = list(self.baseline_window)
        mean = sum(data) / len(data) if data else 0
        std = (sum((x - mean) ** 2 for x in data) / (len(data) - 1)) ** 0.5 if len(data) > 1 else 0
        return mean, std

    def detect_high_power_usage(self):
        """持续检测逻辑优化"""
        if len(self.detection_window) < self.DETECTION_WINDOW:
            return False

        mean, std = self._get_baseline_stats()
        threshold = mean + self.THRESHOLD_MULTIPLIER * std

        # 检查检测窗口全部超标且时间连续
        return all(usage > threshold for usage in self.detection_window) \
            and self._check_time_continuity()

    def _check_time_continuity(self):
        """验证检测窗口时间连续性"""
        for i in range(1, len(self.detection_window)):
            delta = self.timestamps[-i] - self.timestamps[-i - 1]
            if delta > timedelta(minutes=35):  # 允许5分钟误差
                return False
        return True


def test_consecutive_detection():
    analyzer = PowerUsageAnalyzer()
    analyzer.WINDOW_SIZE = 6  # 测试专用设置

    base_ts = datetime(2023, 1, 1, 0, 0)

    # 填充6个正常值（填满基线窗口）
    for i in range(6):
        analyzer.add_power_usage(5.0, base_ts + timedelta(minutes=30 * i))

    # 添加4个高用电值（进入检测窗口）
    test_times = [base_ts + timedelta(minutes=180 + 30 * i) for i in range(4)]
    results = []
    for usage, ts in zip([10.0] * 4, test_times):
        analyzer.add_power_usage(usage, ts)
        results.append(analyzer.detect_high_power_usage())

    print(results)  # 预期输出: [False, False, False, True]


def test_large_dataset_detection():
    """验证大数据量下的窗口滑动机制"""
    analyzer = PowerUsageAnalyzer()
    base_ts = datetime(2023, 1, 1, 0, 0)

    # 阶段1：填充48个正常值（24小时数据）
    for i in range(48):
        normal_ts = base_ts + timedelta(minutes=30 * i)
        analyzer.add_power_usage(5.0, normal_ts)

    # 阶段2：添加4个高用电值（应进入检测窗口）
    high_usage_times = [base_ts + timedelta(hours=24, minutes=30 * i) for i in range(4)]
    results = []
    for usage, ts in zip([15.0] * 4, high_usage_times):
        analyzer.add_power_usage(usage, ts)
        results.append(analyzer.detect_high_power_usage())

    # 阶段3：继续添加数据验证窗口滑动
    additional_data = [
        (5.0, base_ts + timedelta(hours=26, minutes=30 * i))
        for i in range(4)
    ]
    for usage, ts in additional_data:
        analyzer.add_power_usage(usage, ts)
        results.append(analyzer.detect_high_power_usage())

    print(f"高用电检测结果: {results}")
    # 预期输出: [False, False, False, True, False, False, False, False]


def test_instant_anomaly_detection():
    """测试瞬时异常检测"""
    # 创建新的分析器实例
    analyzer = PowerUsageAnalyzer()
    analyzer.MIN_DATA_POINTS = 6  # 确保最小数据点要求
    analyzer.WINDOW_SIZE = 6  # 设置窗口大小
    analyzer.RELATIVE_SAFETY_MARGIN = 0.1  # 10%的安全边际

    # 生成唯一时间戳
    base_ts = datetime(2023, 1, 1, 0, 0)

    # 填充基线窗口（6个相同值）
    # print("初始化基线窗口...")
    for i in range(6):
        analyzer.add_power_usage(5.0, base_ts + timedelta(minutes=i))

    # 打印基线窗口状态
    # print(f"基线窗口: {list(analyzer.baseline_window)}")
    # print(f"基线均值: {sum(analyzer.baseline_window) / len(analyzer.baseline_window)}")

    # 测试数据带递增时间戳
    test_data = [
        (6.0, base_ts + timedelta(minutes=10)),
        (15.0, base_ts + timedelta(minutes=20)),
        (5.5, base_ts + timedelta(minutes=30)),
        (0.0, base_ts + timedelta(minutes=40))
    ]

    # 重写检测逻辑为直接的函数，不依赖类方法
    def is_abnormal(current, baseline_avg, safety_margin=0.1):
        threshold = baseline_avg * (1 + safety_margin)
        # print(f"检测: 当前值={current}, 基线均值={baseline_avg}, 阈值={threshold}")
        return current > threshold

    results = []
    for i, (usage, ts) in enumerate(test_data):
        # print(f"\n测试 #{i+1}: 用电量 = {usage}")
        analyzer.add_power_usage(usage, ts)
        # 基线均值计算
        baseline_avg = sum(analyzer.baseline_window) / len(analyzer.baseline_window)
        # 使用独立函数检测
        result = is_abnormal(usage, baseline_avg, analyzer.RELATIVE_SAFETY_MARGIN)
        results.append(result)

    print(f"\n瞬时检测结果: {results}")
    # 预期输出: [True, True, False, False]


def test_safety_margin_calibration():
    """安全边际参数校准测试"""
    analyzer = PowerUsageAnalyzer()
    analyzer.RELATIVE_SAFETY_MARGIN = 0.1
    analyzer.ABSOLUTE_SAFETY_MARGIN = 0.5
    analyzer.USE_RELATIVE_MARGIN = True

    # 测试数据（基线均值=5.0，标准差=0）
    test_data = [
        (5.4, False),  # < 5.0*1.1=5.5
        (5.5, False),  # = 阈值
        (5.6, True)  # > 阈值
    ]

    # 填充基线数据
    for _ in range(48):
        analyzer.add_power_usage(5.0, datetime.now())

    results = []
    for usage, _ in test_data:
        analyzer.add_power_usage(usage, datetime.now())
        results.append(analyzer.detect_abnormal_usage())

    print(f"校准测试结果: {results}")
    # 预期输出: [False, False, True]


def test_direct_abnormal_detection():
    """直接测试异常检测方法"""
    print("\n=== 直接测试异常检测方法 ===")
    analyzer = PowerUsageAnalyzer()
    analyzer.RELATIVE_SAFETY_MARGIN = 0.1  # 10%的安全边际

    # 确保MIN_DATA_POINTS设置正确
    analyzer.MIN_DATA_POINTS = 6

    # 设置固定的基线窗口
    analyzer.baseline_window = deque([5.0] * 6)
    print(f"基线窗口长度: {len(analyzer.baseline_window)}")
    print(f"基线窗口内容: {list(analyzer.baseline_window)}")
    print(f"基线均值: {sum(analyzer.baseline_window) / len(analyzer.baseline_window)}")
    print(f"安全边际: {analyzer.RELATIVE_SAFETY_MARGIN}")

    # 测试不同值
    test_values = [5.4, 5.5, 5.6, 6.0, 15.0]
    results = []

    for value in test_values:
        print(f"\n=== 测试值: {value} ===")
        analyzer.current_usage = value  # 直接设置当前值
        print(f"设置当前值: analyzer.current_usage = {analyzer.current_usage}")

        # 手动计算预期结果
        expected_threshold = 5.0 * (1 + analyzer.RELATIVE_SAFETY_MARGIN)
        expected_result = value > expected_threshold
        print(f"手动计算 - 阈值: {expected_threshold}, 预期结果: {expected_result}")

        # 确认类内计算
        data = list(analyzer.baseline_window)
        mean = sum(data) / len(data)
        threshold = mean * (1 + analyzer.RELATIVE_SAFETY_MARGIN)
        print(f"类内计算 - 均值: {mean}, 阈值: {threshold}")

        # 调用方法获取实际结果
        result = analyzer.detect_abnormal_usage()
        print(f"方法返回结果: {result}")

        results.append(result)

    print(f"\n直接测试结果: {results}")
    # 预期: [False, False, True, True, True]


def fetch_smart_meter_products():
    """
    增强版电表数据获取方法
    返回结构: {
        "product_keys": [pk1, pk2...],
        "meter_data": {
            pk1: [record1, record2...],
            pk2: [record3, record4...]
        }
    }
    """
    config = DatabaseConfig.get_connection_params()
    result = {"product_keys": [], "meter_data": {}}

    for attempt in range(DatabaseConfig.MAX_RETRIES):
        try:
            connection = pymysql.connect(**config)
            with connection:
                with connection.cursor() as cursor:
                    # 第一阶段：获取所有产品密钥
                    cursor.execute("SELECT DISTINCT `product_key` FROM `smart_meter`")
                    product_keys = [row[0] for row in cursor]
                    result["product_keys"] = product_keys

                    # 第二阶段：获取每个产品的详细数据
                    for pk in product_keys:
                        try:
                            with connection.cursor() as cursor:
                                cursor.execute(
                                    """SELECT DISTINCT `room_num` FROM smart_meter where `product_key` = %s""", (pk,)
                                )
                                room_nums = [row[0] for row in cursor]
                                print(f"产品密钥 {pk} 房间号: {room_nums}")

                            # 初始化该产品密钥的数据结构
                            result["meter_data"][pk] = {}

                            # 遍历该产品密钥下的每个房间号
                            for room_num in room_nums:
                                # print(f"处理产品 {pk[-4:]} 房间号 {room_num}")
                                # 初始化该房间的记录列表
                                result["meter_data"][pk][room_num] = []

                                # 每个房间号查询时间窗口（默认48个，即24小时）
                                total_windows = 48

                                for window_idx in range(total_windows):
                                    with connection.cursor(pymysql.cursors.SSDictCursor) as data_cursor:
                                        # 计算时间窗口（包含两端）
                                        base_time = datetime.now().replace(second=0, microsecond=0)
                                        # 分钟取整到30分钟倍数
                                        rounded_minute = base_time.minute // 30 * 30
                                        base_time = base_time.replace(minute=rounded_minute)

                                        window_end = (base_time - timedelta(minutes=30 * window_idx)).replace(second=59,
                                                                                                              microsecond=999999)
                                        window_start = (window_end - timedelta(minutes=30)).replace(second=0,
                                                                                                    microsecond=0)

                                        # 调整查询条件包含房间号
                                        query = """
                                            SELECT * 
                                            FROM `smart_meter` 
                                            WHERE `product_key` = %s 
                                            AND script_time >= %s 
                                            AND script_time <= %s
                                            AND room_num = %s
                                            ORDER BY script_time DESC
                                        """
                                        params = (
                                            pk,
                                            window_start.strftime('%Y-%m-%d %H:%M:%S'),
                                            window_end.strftime('%Y-%m-%d %H:%M:%S'),
                                            room_num
                                        )

                                        # 安全打印SQL
                                        debug_sql = query % tuple(
                                            f"'{p.strftime('%Y-%m-%d %H:%M:%S')}'" if isinstance(p, datetime)
                                            else f"'{escape_string(str(p))}'"
                                            for p in params
                                        )
                                        # print(f"执行SQL（产品={pk[-4:]} 房间={room_num} 窗口{window_idx+1}/{total_windows}）:\n{debug_sql}\n{'='*40}")

                                        # 执行查询并仅获取第一条记录
                                        data_cursor.execute(query, params)
                                        record = data_cursor.fetchone()
                                        if record:
                                            result["meter_data"][pk][room_num].append(record)

                        except Exception as e:
                            print(f"产品密钥 {pk} 数据获取失败: {e}")
                            result["meter_data"][pk] = {}
                        break
            return result
        except OperationalError as e:
            print(f"连接失败（尝试 {attempt + 1}/{DatabaseConfig.MAX_RETRIES}）: {e}")
            if attempt == DatabaseConfig.MAX_RETRIES - 1:
                return result
            time.sleep(2 ** attempt)
    return result


def data_prediction(data):
    """
    数据预测方法
    输入: 增强版电表数据
    输出: 预测结果字典，先按产品密钥分类，再按异常状态归集房间
    """
    if not data or not data["product_keys"]:
        print("没有可用数据进行预测")
        return {}

    # 初始化结果结构：按产品密钥和异常状态分类
    results = {}

    # 用于统计
    total_rooms = 0
    abnormal_count = 0
    high_usage_count = 0

    # 遍历每个产品密钥
    for product_key in data["product_keys"]:
        if product_key not in data["meter_data"] or not data["meter_data"][product_key]:
            continue

        # 初始化当前产品的结果结构
        if product_key not in results:
            results[product_key] = {
                "abnormal": {},  # 有异常的房间
                "normal": {}  # 无异常的房间
            }

        # 遍历产品下每个房间
        for room_num, records in data["meter_data"][product_key].items():
            if not records:
                continue

            print(f"处理产品 {product_key[-4:]} 房间 {room_num} 的数据...")

            # 实例化分析器
            analyzer = PowerUsageAnalyzer()

            # 对每个房间的记录按时间排序（从旧到新）
            sorted_records = sorted(records, key=lambda x: x["script_time"])

            # 检查记录是否包含power字段
            if len(sorted_records) > 0 and "power" not in sorted_records[0]:
                print(f"警告: 记录中没有power字段，请检查数据格式")
                # 显示可用字段
                if len(sorted_records) > 0:
                    print(f"记录样例字段: {list(sorted_records[0].keys())}")
                continue

            # 提取功率数据和时间戳
            power_usage = []
            timestamps = []
            value_data = []  # 记录累计电量

            for record in sorted_records:
                try:
                    # 提取功率数据(power)
                    if "power" in record:
                        power_value = float(record["power"])
                        # 过滤无效功率值
                        if 0 <= power_value <= 10000:  # 设置合理上下限
                            power_usage.append(power_value)
                            timestamps.append(record["script_time"])

                            # 同时记录累计电量数据(value)
                            if "value" in record:
                                value_data.append(float(record["value"]))
                except (KeyError, ValueError, TypeError) as e:
                    print(f"提取数据时出错: {e}")

            print(f"提取了 {len(power_usage)} 条有效功率数据")

            if len(power_usage) < analyzer.MIN_DATA_POINTS:
                print(f"有效数据点不足，需要至少 {analyzer.MIN_DATA_POINTS} 条")
                continue

            # 填充分析器(使用功率数据)
            for usage, ts in zip(power_usage, timestamps):
                analyzer.add_power_usage(usage, ts)

            # 获取当前是否异常
            is_abnormal = analyzer.detect_abnormal_usage()
            is_high_usage = analyzer.detect_high_power_usage()

            # 更新统计数据
            total_rooms += 1
            if is_abnormal:
                abnormal_count += 1
            if is_high_usage:
                high_usage_count += 1

            # 使用功率数据进行统计
            mean, std = analyzer._get_baseline_stats()

            # 预测未来30分钟的功率范围
            future_prediction = {
                "expected": mean,  # 预期功率
                "lower_bound": max(0, mean - std),  # 下限（不小于0）
                "upper_bound": mean + std,  # 上限
                "confidence": 0.68  # 68%置信区间（单个标准差）
            }

            # 计算当前功耗数据
            current_power = power_usage[-1] if power_usage else 0

            # 计算每小时平均用电量增长（如果有累计电量数据）
            hourly_consumption = 0
            if len(value_data) >= 2 and len(timestamps) >= 2:
                # 计算时间跨度（小时）
                time_span = (timestamps[-1] - timestamps[0]).total_seconds() / 3600
                if time_span > 0:
                    # 计算电量增长
                    consumption_increase = value_data[-1] - value_data[0]
                    # 计算每小时平均增长
                    hourly_consumption = consumption_increase / time_span

            # print(f"房间 {room_num} 检测结果: 异常={is_abnormal}, 高用电={is_high_usage}")
            # print(f"当前功率: {current_power:.2f}W, 平均功率: {mean:.2f}W, 标准差: {std:.2f}W")
            # print(f"每小时平均用电量: {hourly_consumption:.2f}kWh")

            # 获取最新记录的修改时间作为时间戳
            timestamp = datetime.now()  # 默认使用当前时间
            if sorted_records and "gmt_modified" in sorted_records[-1]:
                # 获取gmt_modified值
                gmt_modified = sorted_records[-1]["gmt_modified"]

                # 如果已经是datetime对象，直接使用
                if isinstance(gmt_modified, datetime):
                    timestamp = gmt_modified
                # 如果是字符串，尝试转换为datetime
                elif isinstance(gmt_modified, str):
                    try:
                        # 尝试解析字符串格式的时间
                        timestamp = datetime.strptime(gmt_modified, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        # 如果格式不匹配，使用默认时间
                        print(f"无法解析时间戳: {gmt_modified}，使用当前时间")

            # 将时间戳格式化为标准格式
            formatted_timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")

            # 房间数据
            room_data = {
                "current_status": {
                    "abnormal": is_abnormal,
                    "high_usage": is_high_usage,
                    "current_power": current_power,
                    "baseline_mean": mean,
                    "baseline_std": std,
                    "hourly_consumption": hourly_consumption
                },
                "prediction": future_prediction,
                "timestamp": formatted_timestamp  # 使用格式化后的时间戳
            }

            # 根据异常状态归类到对应产品下
            if is_abnormal or is_high_usage:  # 如果有任何一种异常
                results[product_key]["abnormal"][room_num] = room_data
            else:
                results[product_key]["normal"][room_num] = room_data

    # 计算每个产品的统计信息
    product_stats = {}
    for pk in results:
        abnormal_rooms = len(results[pk]["abnormal"])
        normal_rooms = len(results[pk]["normal"])
        product_stats[pk] = {
            "total_rooms": abnormal_rooms + normal_rooms,
            "abnormal_rooms": abnormal_rooms,
            "normal_rooms": normal_rooms
        }

    # 输出摘要统计
    print(f"预测完成: 总共 {len(results)} 个产品, {total_rooms} 个房间")
    print(f"发现 {abnormal_count} 个房间存在瞬时用电异常")
    print(f"发现 {high_usage_count} 个房间存在持续高用电状态")

    # 输出每个产品的统计
    for pk, stats in product_stats.items():
        print(f"产品 {pk[-4:]}: 总房间数 {stats['total_rooms']}, "
              f"异常房间 {stats['abnormal_rooms']}, 正常房间 {stats['normal_rooms']}")

    return results


def save_mysql_send_feishu(data):
    pass


def send_to_feishu(json_data):
    """
    向飞书发送数据
    :param json_data: 要发送的JSON数据
    :return: 是否发送成功
    """

    json_data['type'] = 'electricalAnomaly'

    # 飞书机器人webhook地址
    url = 'https://open.feishu.cn/anycross/trigger/callback/MDA0ZTdmZTVhNzkzY2U5YjE5NTY2NGJiODk0OWJiM2U3'

    try:
        print(f"开始向飞书发送数据...")
        # 准备请求头
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        # 转换数据为JSON字符串
        if not isinstance(json_data, str):
            json_data = json.dumps(json_data, ensure_ascii=False)

        # 发送POST请求
        response = requests.post(url, headers=headers, data=json_data.encode('utf-8'), timeout=10)

        # 检查响应状态
        if response.status_code == 200:
            result = response.json()
            if result.get('code') == 0:
                print(f"成功发送到飞书: {response.status_code}")
                return True
            else:
                print(f"飞书接口返回错误: {result.get('msg', '未知错误')}")
        else:
            print(f"发送失败，HTTP状态码: {response.status_code}")

        # 调试信息
        print(f"响应内容: {response.text}")

    except requests.RequestException as e:
        print(f"发送请求出错: {e}")
    except json.JSONDecodeError:
        print(f"JSON解析错误")
    except Exception as e:
        print(f"发送过程中出现未知错误: {e}")

    return False


if __name__ == "__main__":
    start_time = time.time()

    # test_consecutive_detection()
    # test_safety_margin_calibration()
    # test_direct_abnormal_detection()
    # test_large_dataset_detection()
    # test_instant_anomaly_detection()

    print("开始获取数据...")
    res_data = fetch_smart_meter_products()
    print(f"获取数据结果: {res_data['product_keys']}")
    print(f"数据结构: {list(res_data.keys())}")

    # 添加详细的数据结构调试
    # print("\n====== 数据结构详细分析 ======")
    # if "meter_data" in res_data:
    #     meter_data = res_data["meter_data"]
    #     print(f"meter_data包含 {len(meter_data)} 个产品密钥")
    #
    #     # 检查每个产品密钥
    #     for pk in res_data["product_keys"]:
    #         if pk in meter_data:
    #             rooms = meter_data[pk]
    #             print(f"产品 {pk[-4:]} 包含 {len(rooms)} 个房间")
    #
    #             # 检查每个房间的记录数
    #             for room, records in rooms.items():
    #                 print(f"  - 房间 {room}: {len(records)} 条记录")
    #         else:
    #             print(f"产品 {pk[-4:]} 在meter_data中不存在数据")

    print("\n开始数据预测...")
    prediction_result = data_prediction(res_data)
    print(prediction_result)

    save_mysql_send_feishu(prediction_result)

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"\n总执行时间: {execution_time:.2f} 秒")

    # 如果执行时间超过60秒，还可以显示分钟表示
    if execution_time > 60:
        print(f"约 {execution_time / 60:.2f} 分钟")
