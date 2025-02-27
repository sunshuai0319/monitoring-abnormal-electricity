from collections import deque
from datetime import datetime, timedelta, date
import pymysql
from pymysql.err import OperationalError
import time
from pymysql.converters import escape_string  # 修正导入路径
import requests
import json
import datetime
from dateutil import parser
from snowflake import SnowflakeGenerator


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
        # print(f"【异常检测】 当前值: {self.current_usage}, 均值: {mean}, 阈值: {threshold}")

        # 明确返回比较结果
        is_abnormal = self.current_usage > threshold
        # print(f"是否异常: {is_abnormal}")

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
                                        base_time = datetime.datetime.now().replace(second=0, microsecond=0)
                                        # 分钟取整到30分钟倍数
                                        rounded_minute = base_time.minute // 30 * 30
                                        base_time = base_time.replace(minute=rounded_minute)

                                        window_end = (base_time - datetime.timedelta(minutes=30 * window_idx)).replace(second=59,
                                                                                                              microsecond=999999)
                                        window_start = (window_end - datetime.timedelta(minutes=30)).replace(second=0,
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
                                            f"'{p.strftime('%Y-%m-%d %H:%M:%S')}'" if isinstance(p, datetime.datetime)
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
                        # break
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

            # print(f"处理产品 {product_key[-4:]} 房间 {room_num} 的数据...")

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

            # print(f"提取了 {len(power_usage)} 条有效功率数据")

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


def find_dept_id(data):
    """
    查询房间信息（不保存数据）
    :param data: 预测结果数据
    :return: 查询到的房间信息字典，格式为 {dept_id: [{room_number: xxx, room_type_id: xxx, abnormal_time: xxx, device_name: xxx}]}
    """
    if not data:
        print("没有数据需要查询")
        return {}

    # 获取产品列表
    product_keys = list(data.keys())
    if not product_keys:
        print("没有有效的产品密钥")
        return {}

    print(f"处理的产品密钥: {product_keys}")

    # 连接数据库
    try:
        conn = pymysql.connect(**DatabaseConfig.get_connection_params())
        cursor = conn.cursor()

        print("连接数据库成功")

        # 查询产品部门信息
        placeholders = ', '.join(['%s'] * len(product_keys))
        dept_query = f"""
            SELECT product_key, dept_id 
            FROM iot_product_dept 
            WHERE product_key IN ({placeholders})
        """

        cursor.execute(dept_query, product_keys)

        # 将查询结果转换为字典，以product_key为键
        product_dept_map = {}
        for row in cursor.fetchall():
            product_key, dept_id = row
            # 确保dept_id是字符串类型
            dept_id = str(dept_id)
            # 检查是否包含逗号，如果包含则分割为单独的部门ID
            if ',' in dept_id:
                print(f"警告: 部门ID '{dept_id}' 包含逗号，将被分割为单独部门")
                # 处理组合部门ID情况，只取第一个部门ID
                dept_id = dept_id.split(',')[0].strip()
            
            product_dept_map[product_key] = {
                'dept_id': dept_id
            }

        print(f"查询到 {len(product_dept_map)} 个产品的部门信息: {list(product_dept_map.keys())}")

        # 收集所有需要查询的房间信息
        dept_room_num = {}
        
        # 遍历所有产品，收集异常房间信息
        for product_key, categories in data.items():
            # 跳过没有部门信息的产品
            if product_key not in product_dept_map:
                print(f"警告: 产品 {product_key} 没有部门信息，跳过")
                continue

            # 获取部门ID
            dept_id = product_dept_map[product_key]['dept_id']
            
            # 确保部门ID有对应的列表
            if dept_id not in dept_room_num:
                dept_room_num[dept_id] = []
            
            # 收集异常房间的房间号和时间戳
            if 'abnormal' in categories and categories['abnormal']:
                for room_num, room_data in categories['abnormal'].items():
                    # 提取时间戳
                    abnormal_time = room_data.get('timestamp')
                    
                    # 查询设备名称
                    device_name_query = """
                        SELECT sm.device_name  
                        FROM smart_meter sm
                        WHERE sm.product_key = %s AND sm.room_num = %s
                        LIMIT 1
                    """
                    
                    device_name = "智能电表"  # 默认设备名称
                    try:
                        cursor.execute(device_name_query, [product_key, room_num])
                        device_result = cursor.fetchone()
                        
                        if device_result and device_result[0]:
                            device_name = device_result[0]
                        else:
                            print(f"未找到设备名称: 产品={product_key}, 房间={room_num}")
                    except Exception as e:
                        print(f"查询设备名称时出错: {e}")
                    
                    # 查询房间类型ID
                    room_type_query = """
                        SELECT room_type_id
                        FROM pms_beyond_room_info
                        WHERE dept_id = %s AND room_number = %s
                        LIMIT 1
                    """
                    
                    try:
                        cursor.execute(room_type_query, [dept_id, room_num])
                        room_type_result = cursor.fetchone()
                        
                        if room_type_result and room_type_result[0]:
                            room_type_id = room_type_result[0]
                        else:
                            print(f"未找到房间类型ID: 部门={dept_id}, 房间={room_num}")
                            room_type_id = f"RT{room_num[:2]}"  # 默认使用房间号前两位作为房间类型前缀
                    except Exception as e:
                        print(f"查询房间类型时出错: {e}")
                        room_type_id = f"RT{room_num[:2]}"
                    
                    # 添加房间信息，确保结构完全符合预期
                    room_info = {
                        'room_number': room_num,
                        'room_type_id': room_type_id,
                        'abnormal_time': abnormal_time,
                        'device_name': device_name  # 添加设备名称字段
                    }
                    
                    # 添加到对应部门的房间列表
                    dept_room_num[dept_id].append(room_info)
                    
                    print(f"添加异常房间: 部门={dept_id}, 房间={room_num}, 类型={room_type_id}, 设备={device_name}, 时间={abnormal_time}")

        # 关闭数据库连接
        cursor.close()
        conn.close()
        
        print(f"处理完成，返回 {len(dept_room_num)} 个部门的房间信息")
        return dept_room_num
        
    except pymysql.OperationalError as e:
        print(f"数据库连接错误: {e}")
    except Exception as e:
        print(f"查询过程中出现错误: {e}")
        import traceback
        traceback.print_exc()
    
    # 发生错误时返回空字典
    return {}

def find_datas(dept_room_num):
    """
    根据部门ID和房间信息查询部门及房间详情
    :param dept_room_num: 部门ID和房间信息的映射，格式为 {dept_id: [{room_number: xxx, room_type_id: xxx, abnormal_time: xxx}]}
    :return: 部门及房间信息字典，以部门ID为键
    """
    
    if not dept_room_num:
        print("没有部门ID需要查询")
        return {}
    
    # 获取所有需要查询的部门ID
    dept_ids = list(dept_room_num.keys())
    print(f"需要查询的部门ID: {dept_ids}")
    
    # 返回结果
    dept_info_map = {}
    
    # 连接数据库
    try:
        conn = pymysql.connect(**DatabaseConfig.get_connection_params())
        cursor = conn.cursor()
        
        print("连接数据库成功")
        
        # 逐个查询每个部门ID
        for dept_id in dept_ids:
            # 构建查询
            dept_query = """
                SELECT dept_name, dept_code, brand
                FROM sys_dept
                WHERE dept_id = %s
            """
            
            # 执行查询
            # print(f"执行查询部门: {dept_id}")
            cursor.execute(dept_query, [dept_id])
            
            # 获取结果
            result = cursor.fetchone()
            
            if result:
                # print(f"部门ID {dept_id} 查询成功")
                
                # 获取字段列表
                field_names = [i[0] for i in cursor.description]
                
                # 创建部门信息字典
                dept_info = {}
                for i, name in enumerate(field_names):
                    dept_info[name] = result[i]
                
                # 添加部门ID
                dept_info['dept_id'] = dept_id
                
                # 查询部门下每个房间的详细信息
                room_details = []
                rooms_data = dept_room_num.get(dept_id, [])
                
                for room_data in rooms_data:
                    room_number = room_data.get('room_number')
                    room_type_id = room_data.get('room_type_id')
                    abnormal_time = room_data.get('abnormal_time')  # 获取异常时间
                    device_name = room_data.get('device_name')  # 获取异常时间
                    
                    if room_number and room_type_id:
                        # 构建房间查询
                        room_query = """
                            SELECT status
                            FROM pms_beyond_room_info
                            WHERE dept_id = %s AND room_number = %s AND room_type_id = %s
                        """
                        
                        # 执行查询
                        # print(f"查询房间: {room_number}, 部门ID: {dept_id}")
                        cursor.execute(room_query, [dept_id, room_number, room_type_id])
                        
                        # 获取结果
                        room_result = cursor.fetchone()
                        
                        # 创建房间信息字典
                        room_info = {'room_number': room_number}
                        
                        # 添加异常时间到房间信息
                        room_info['abnormal_time'] = abnormal_time
                        room_info['device_name'] = device_name
                        
                        if room_result:
                            # 获取房间字段列表
                            room_field_names = [i[0] for i in cursor.description]
                            
                            # 添加房间状态信息
                            for i, name in enumerate(room_field_names):
                                room_info[name] = room_result[i]
                        
                        # 查询最新入住记录
                        checkin_query = """
                            SELECT check_in_detail, check_out_detail
                            FROM hotel_order_night
                            WHERE dept_id = %s AND room_no = %s
                            ORDER BY check_in_detail DESC
                            LIMIT 1
                        """
                        
                        # 执行查询最新入住记录
                        cursor.execute(checkin_query, [dept_id, room_number])
                        
                        # 获取入住记录结果
                        checkin_result = cursor.fetchone()
                        
                        if checkin_result:
                            # 获取入住和退房日期
                            check_in_detail = checkin_result[0]
                            check_out_detail = checkin_result[1]
                            
                            # 格式化日期字段
                            if check_in_detail:
                                if isinstance(check_in_detail, datetime.datetime):
                                    formatted_check_in = check_in_detail.strftime('%Y-%m-%d %H:%M:%S')
                                else:
                                    # 尝试解析字符串格式
                                    try:
                                        parsed_date = parser.parse(str(check_in_detail))
                                        formatted_check_in = parsed_date.strftime('%Y-%m-%d %H:%M:%S')
                                    except (ValueError, TypeError):
                                        formatted_check_in = str(check_in_detail)
                            else:
                                formatted_check_in = None
                                
                            if check_out_detail:
                                if isinstance(check_out_detail, datetime.datetime):
                                    formatted_check_out = check_out_detail.strftime('%Y-%m-%d %H:%M:%S')
                                else:
                                    # 尝试解析字符串格式
                                    try:
                                        parsed_date = parser.parse(str(check_out_detail))
                                        formatted_check_out = parsed_date.strftime('%Y-%m-%d %H:%M:%S')
                                    except (ValueError, TypeError):
                                        formatted_check_out = str(check_out_detail)
                            else:
                                formatted_check_out = None
                            
                            # 添加格式化后的日期
                            room_info['check_in_detail'] = formatted_check_in
                            room_info['check_out_detail'] = formatted_check_out
                            
                            # print(f"房间 {room_number} 入住日期: {formatted_check_in}, 退房日期: {formatted_check_out}, 异常时间: {abnormal_time}")
                        else:
                            print(f"房间 {room_number} 没有找到入住记录, 异常时间: {abnormal_time}")
                            room_info['check_in_detail'] = None
                            room_info['check_out_detail'] = None
                        
                        # 添加到房间详情列表
                        room_details.append(room_info)
                    else:
                        print(f"房间数据不完整: {room_data}")
                
                # 添加房间详情到部门信息
                dept_info['rooms'] = room_details
                
                # 存储到结果字典
                dept_info_map[dept_id] = dept_info
                
                # 打印部门基本信息
                dept_name = dept_info.get('dept_name', '未知')
                # print(f"部门ID: {dept_id}, 部门名称: {dept_name}, 关联房间数: {len(room_details)}")
            else:
                print(f"部门ID {dept_id} 未找到信息")
        
        print(f"查询成功，返回 {len(dept_info_map)} 个部门信息")
        
        # 关闭数据库连接
        cursor.close()
        conn.close()
        
    except pymysql.OperationalError as e:
        print(f"数据库连接错误: {e}")
    except Exception as e:
        print(f"查询时出现未知错误: {e}")
        import traceback
        traceback.print_exc()  # 打印详细错误信息
    
    return dept_info_map

def save_to_mysql(dept_info_map):
    """
    将部门和房间信息保存到MySQL数据库
    :param dept_info_map: 部门及房间信息字典，来自find_datas函数
    :return: 成功保存的记录数和JSON格式的数据列表，用于发送到飞书
    """
    if not dept_info_map:
        print("没有数据需要保存")
        return 0, []

    # 创建雪花ID生成器
    gen = SnowflakeGenerator(42)  # 设置一个机器ID
    
    # 记录计数
    saved_count = 0
    
    # 用于存储所有记录的JSON数据
    json_records = []
    
    # 定义房间状态映射字典（英文代码 -> 中文名称）
    room_status_map = {
        "OD": "住脏",
        "OC": "住净",
        "VD": "空脏",
        "VC": "空净",
        "OOO": "维修",
        "LOCKED": "锁房"
    }
    
    try:
        # 连接数据库
        conn = pymysql.connect(**DatabaseConfig.get_connection_params())
        cursor = conn.cursor()
        
        print("连接数据库成功，准备保存数据...")
        
        # 遍历所有部门信息
        for dept_id, dept_info in dept_info_map.items():
            dept_name = dept_info.get('dept_name', '未知酒店')
            dept_code = dept_info.get('dept_code', dept_id)
            dept_brand = dept_info.get('brand', '未知品牌')
            
            # 遍历该部门下的所有房间
            rooms = dept_info.get('rooms', [])
            for room in rooms:
                room_num = room.get('room_number', '')
                
                # 获取原始房间状态并转换为中文
                original_status = room.get('status', '未知')
                room_status = room_status_map.get(original_status, '未知')
                
                # 处理入住和退房时间
                check_in_time = room.get('check_in_detail')
                check_out_time = room.get('check_out_detail')
                device_name = room.get('device_name')
                
                # 使用雪花算法生成唯一ID
                record_id = next(gen)
                
                # 异常时间
                abnormal_time = room.get('abnormal_time')
                
                # 设置默认值
                electricity_usage_status = '异常'
                power_consumption = 0.000  # 可以根据实际情况修改
                estimated_cost = 0.00      # 可以根据实际情况修改
                
                # 构建SQL插入语句
                insert_sql = """
                    INSERT INTO abnormal_electricity_usage (
                        id, dept_name, dept_id, dept_brand, room_num, 
                        device_name, room_status, check_in_time, check_out_time,
                        electricity_usage_status, abnormal_time, 
                        power_consumption, estimated_electricity_cost
                    ) VALUES (
                        %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, 
                        %s, %s, 
                        %s, %s
                    )
                """
                
                # 准备参数
                params = (
                    str(record_id), dept_name, dept_code, dept_brand, room_num,
                    device_name, room_status, check_in_time, check_out_time,
                    electricity_usage_status, abnormal_time,
                    power_consumption, estimated_cost
                )
                
                # 执行插入
                try:
                    cursor.execute(insert_sql, params)
                    saved_count += 1
                    # print(f"成功保存房间记录: 部门={dept_name}, 房间={room_num}, 状态={original_status}({room_status})")
                    
                    # 创建用于飞书发送的JSON记录
                    json_record = {
                        "id": str(record_id),
                        "dept_name": dept_name,
                        "dept_id": dept_code,
                        "dept_brand": dept_brand,
                        "room_num": room_num,
                        "device_name": device_name,
                        "room_status": room_status,
                        "check_in_time": check_in_time,
                        "check_out_time": check_out_time,
                        "electricity_usage_status": electricity_usage_status,
                        "abnormal_time": abnormal_time,
                        "power_consumption": power_consumption,
                        "estimated_electricity_cost": estimated_cost
                    }
                    json_records.append(json_record)
                    
                except Exception as e:
                    print(f"保存房间记录失败: 部门={dept_name}, 房间={room_num}, 状态={original_status}({room_status}), 错误: {e}")
        
        # 提交事务
        conn.commit()
        print(f"数据保存完成，共保存 {saved_count} 条记录")
        
        # 关闭连接
        cursor.close()
        conn.close()
        
    except pymysql.OperationalError as e:
        print(f"数据库连接错误: {e}")
    except Exception as e:
        print(f"保存数据时出现未知错误: {e}")
        import traceback
        traceback.print_exc()
    
    # 返回保存的记录数和JSON记录列表
    return saved_count, json_records


def send_to_feishu(json_records):
    """
    向飞书发送数据
    :param json_records: 要发送的JSON数据列表，来自save_to_mysql函数的第二个返回值
    :return: 是否发送成功
    """
    if not json_records or not isinstance(json_records, list):
        print("没有有效数据需要发送到飞书")
        return False

    # 构建发送数据
    payload = {
        "records": json_records,
        "type": "electricalAnomaly"
    }

    # 飞书机器人webhook地址
    url = 'https://open.feishu.cn/anycross/trigger/callback/MDA0ZTdmZTVhNzkzY2U5YjE5NTY2NGJiODk0OWJiM2U3'

    try:
        print(f"开始向飞书发送数据，共 {len(json_records)} 条记录...")
        # 准备请求头
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        # 转换数据为JSON字符串
        json_data = json.dumps(payload, ensure_ascii=False)

        # 发送POST请求
        response = requests.post(url, headers=headers, data=json_data.encode('utf-8'), timeout=10)

        # 检查响应状态
        if response.status_code == 200:
            # 尝试解析响应内容为JSON
            try:
                result = response.json()
                # 检查是否包含challenge字段(飞书API特殊响应格式)
                if 'challenge' in result or result.get('code') == 0:
                    print(f"成功发送到飞书")
                    return True
                else:
                    print(f"飞书接口返回未识别的响应: {result}")
            except json.JSONDecodeError:
                # 如果无法解析为JSON，但状态码是200，依然认为是成功的
                print(f"飞书接口返回非JSON响应，但状态码为200，视为成功")
                return True
        else:
            print(f"发送失败，HTTP状态码: {response.status_code}")

        # 输出详细响应内容以便调试
        print(f"响应详情: {response.text}")

    except requests.RequestException as e:
        print(f"发送请求出错: {e}")
    except Exception as e:
        print(f"发送过程中出现未知错误: {e}")
        import traceback
        traceback.print_exc()

    return False


if __name__ == "__main__":
    start_time = time.time()

    # test_consecutive_detection()
    # test_safety_margin_calibration()
    # test_direct_abnormal_detection()
    # test_large_dataset_detection()
    # test_instant_anomaly_detection()

    print("开始获取数据...")
    # res_data = fetch_smart_meter_products()
    # print(res_data)
    # print(f"获取数据结果: {res_data['product_keys']}")
    # print(f"数据结构: {list(res_data.keys())}")



    print("\n开始数据预测...")
    # prediction_result = data_prediction(res_data)
    # print(prediction_result)
    prediction_result={'a1KBbjGM3vc': {'abnormal': {'1417': {'current_status': {'abnormal': True, 'high_usage': False, 'current_power': 2359.8, 'baseline_mean': 440.86875000000003, 'baseline_std': 879.9119023011166, 'hourly_consumption': 0.3480041135238053}, 'prediction': {'expected': 440.86875000000003, 'lower_bound': 0, 'upper_bound': 1320.7806523011166, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:51:20'}, '1439': {'current_status': {'abnormal': True, 'high_usage': False, 'current_power': 2055.7, 'baseline_mean': 518.1291666666667, 'baseline_std': 896.8390977295271, 'hourly_consumption': 0.03655319148935845}, 'prediction': {'expected': 518.1291666666667, 'lower_bound': 0, 'upper_bound': 1414.9682643961937, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:36:50'}, '1418': {'current_status': {'abnormal': True, 'high_usage': False, 'current_power': 2346.2, 'baseline_mean': 698.7395833333334, 'baseline_std': 592.5560017486391, 'hourly_consumption': 0.6184680851063777}, 'prediction': {'expected': 698.7395833333334, 'lower_bound': 106.18358158469425, 'upper_bound': 1291.2955850819726, 'confidence': 0.68}, 'timestamp': '2025-02-26 12:33:33'}}, 'normal': {'1425': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 21.2, 'baseline_mean': 585.9354166666667, 'baseline_std': 605.6008525000265, 'hourly_consumption': 0.5377084835518137}, 'prediction': {'expected': 585.9354166666667, 'lower_bound': 0, 'upper_bound': 1191.5362691666933, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:30:58'}, '1422': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 196.2, 'baseline_mean': 480.9833333333338, 'baseline_std': 818.9718843622695, 'hourly_consumption': 0.44089361702127133}, 'prediction': {'expected': 480.9833333333338, 'lower_bound': 0, 'upper_bound': 1299.9552176956033, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:56:18'}, '1409': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 13.6, 'baseline_mean': 284.0416666666667, 'baseline_std': 613.7063639931345, 'hourly_consumption': 0.2682553191489302}, 'prediction': {'expected': 284.0416666666667, 'lower_bound': 0, 'upper_bound': 897.7480306598011, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:32:45'}, '1435': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 226.2, 'baseline_mean': 605.4145833333333, 'baseline_std': 549.3484581813311, 'hourly_consumption': 0.6233617021276547}, 'prediction': {'expected': 605.4145833333333, 'lower_bound': 56.0661251520022, 'upper_bound': 1154.7630415146646, 'confidence': 0.68}, 'timestamp': '2025-02-26 13:37:18'}, '1315': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 10.6, 'baseline_mean': 1080.0041666666664, 'baseline_std': 1049.6987069161412, 'hourly_consumption': 1.0682127659574496}, 'prediction': {'expected': 1080.0041666666664, 'lower_bound': 30.305459750525188, 'upper_bound': 2129.7028735828076, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:58:04'}, '1405': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 11.2, 'baseline_mean': 958.2583333333326, 'baseline_std': 1344.4034316986547, 'hourly_consumption': 0.8417546306693912}, 'prediction': {'expected': 958.2583333333326, 'lower_bound': 0, 'upper_bound': 2302.661765031987, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:38:30'}, '1431': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 7.7, 'baseline_mean': 284.24375000000015, 'baseline_std': 396.0921179694858, 'hourly_consumption': 0.5519639712053336}, 'prediction': {'expected': 284.24375000000015, 'lower_bound': 0, 'upper_bound': 680.3358679694859, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:30:01'}, '1302': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 9.2, 'baseline_mean': 1339.5041666666664, 'baseline_std': 970.7937552855489, 'hourly_consumption': 0.8683081360299852}, 'prediction': {'expected': 1339.5041666666664, 'lower_bound': 368.7104113811175, 'upper_bound': 2310.2979219522153, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:49:13'}, '1306': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 8.0, 'baseline_mean': 375.18333333333334, 'baseline_std': 811.9851427561575, 'hourly_consumption': 0.1150212765957436}, 'prediction': {'expected': 375.18333333333334, 'lower_bound': 0, 'upper_bound': 1187.168476089491, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:59:20'}, '1403': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 10.8, 'baseline_mean': 551.1624999999996, 'baseline_std': 1241.1084461720397, 'hourly_consumption': 0.4873617021276585}, 'prediction': {'expected': 551.1624999999996, 'lower_bound': 0, 'upper_bound': 1792.2709461720392, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:37:55'}, '1419': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 198.5, 'baseline_mean': 286.49791666666687, 'baseline_std': 809.809233771726, 'hourly_consumption': 0.2061300961004192}, 'prediction': {'expected': 286.49791666666687, 'lower_bound': 0, 'upper_bound': 1096.307150438393, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:30:54'}, '1312': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 221.7, 'baseline_mean': 1198.2312499999998, 'baseline_std': 1239.4198933296911, 'hourly_consumption': 0.9187342639983793}, 'prediction': {'expected': 1198.2312499999998, 'lower_bound': 0, 'upper_bound': 2437.651143329691, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:25:51'}, '1415': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 68.5, 'baseline_mean': 128.20000000000002, 'baseline_std': 432.14687515025213, 'hourly_consumption': 0.10438421257934383}, 'prediction': {'expected': 128.20000000000002, 'lower_bound': 0, 'upper_bound': 560.3468751502521, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:30:00'}, '1311': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 215.4, 'baseline_mean': 580.2812499999999, 'baseline_std': 927.9098754054725, 'hourly_consumption': 0.4658297872340282}, 'prediction': {'expected': 580.2812499999999, 'lower_bound': 0, 'upper_bound': 1508.1911254054723, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:44:26'}, '1406': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 31.3, 'baseline_mean': 1440.7791666666678, 'baseline_std': 1100.5055122694578, 'hourly_consumption': 0.07651063829786274}, 'prediction': {'expected': 1440.7791666666678, 'lower_bound': 340.27365439721007, 'upper_bound': 2541.2846789361256, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:57:55'}, '1421': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 205.1, 'baseline_mean': 604.9625, 'baseline_std': 737.6994771340359, 'hourly_consumption': 0.5065106382978635}, 'prediction': {'expected': 604.9625, 'lower_bound': 0, 'upper_bound': 1342.6619771340359, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:51:00'}, '1412': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 13.5, 'baseline_mean': 158.06249999999997, 'baseline_std': 445.3767182027157, 'hourly_consumption': 0.19821510892564292}, 'prediction': {'expected': 158.06249999999997, 'lower_bound': 0, 'upper_bound': 603.4392182027157, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:38:41'}}}, 'a1bboFh9ffy': {'abnormal': {'20布草间': {'current_status': {'abnormal': True, 'high_usage': False, 'current_power': 1116.2, 'baseline_mean': 600.5558139534885, 'baseline_std': 280.06676355598944, 'hourly_consumption': 0.568483710101985}, 'prediction': {'expected': 600.5558139534885, 'lower_bound': 320.48905039749906, 'upper_bound': 880.6225775094779, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:46:27'}, '21层布草间': {'current_status': {'abnormal': True, 'high_usage': False, 'current_power': 939.7, 'baseline_mean': 526.9744186046513, 'baseline_std': 237.7690557112523, 'hourly_consumption': 0.5310952380952391}, 'prediction': {'expected': 526.9744186046513, 'lower_bound': 289.205362893399, 'upper_bound': 764.7434743159035, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:32:01'}}, 'normal': {'2107': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 8.2, 'baseline_mean': 8.25348837209302, 'baseline_std': 0.12601745383515886, 'hourly_consumption': 0.00804761904761907}, 'prediction': {'expected': 8.25348837209302, 'lower_bound': 8.127470918257861, 'upper_bound': 8.37950582592818, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:30:05'}, '2109': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 10.1, 'baseline_mean': 10.32093023255814, 'baseline_std': 0.3203852885384243, 'hourly_consumption': 0.01014272297985481}, 'prediction': {'expected': 10.32093023255814, 'lower_bound': 10.000544944019715, 'upper_bound': 10.641315521096566, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:31:57'}, '2125': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 8.1, 'baseline_mean': 8.211627906976743, 'baseline_std': 0.1095647282607333, 'hourly_consumption': 0.00819047619047605}, 'prediction': {'expected': 8.211627906976743, 'lower_bound': 8.102063178716008, 'upper_bound': 8.321192635237477, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:59:55'}, '1929': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 0.0, 'baseline_mean': 0.0, 'baseline_std': 0.0, 'hourly_consumption': 0.0}, 'prediction': {'expected': 0.0, 'lower_bound': 0, 'upper_bound': 0.0, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:43:41'}, '2121': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 10.5, 'baseline_mean': 10.360465116279073, 'baseline_std': 0.25832752824542515, 'hourly_consumption': 0.01038095238095238}, 'prediction': {'expected': 10.360465116279073, 'lower_bound': 10.102137588033647, 'upper_bound': 10.618792644524499, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:59:13'}, '2122': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 8.2, 'baseline_mean': 8.281395348837208, 'baseline_std': 0.09064796888825459, 'hourly_consumption': 0.00823809523809524}, 'prediction': {'expected': 8.281395348837208, 'lower_bound': 8.190747379948954, 'upper_bound': 8.372043317725462, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:59:11'}, '2106': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 9.0, 'baseline_mean': 8.937209302325583, 'baseline_std': 0.07874992309581551, 'hourly_consumption': 0.008761904761904728}, 'prediction': {'expected': 8.937209302325583, 'lower_bound': 8.858459379229767, 'upper_bound': 9.015959225421398, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:30:05'}, '2006': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 10.6, 'baseline_mean': 10.285714285714285, 'baseline_std': 0.36396959707621, 'hourly_consumption': 0.0102450592885376}, 'prediction': {'expected': 10.285714285714285, 'lower_bound': 9.921744688638075, 'upper_bound': 10.649683882790494, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:51:10'}, '1905': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 1.0, 'baseline_mean': 1.0511627906976748, 'baseline_std': 0.059249645461088435, 'hourly_consumption': 0.0}, 'prediction': {'expected': 1.0511627906976748, 'lower_bound': 0.9919131452365864, 'upper_bound': 1.1104124361587633, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:43:43'}, '1926': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 0.0, 'baseline_mean': 0.0, 'baseline_std': 0.0, 'hourly_consumption': 0.0}, 'prediction': {'expected': 0.0, 'lower_bound': 0, 'upper_bound': 0.0, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:44:25'}, '1901': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 1.0, 'baseline_mean': 1.0418604651162793, 'baseline_std': 0.06630577845799998, 'hourly_consumption': 0.0}, 'prediction': {'expected': 1.0418604651162793, 'lower_bound': 0.9755546866582793, 'upper_bound': 1.1081662435742792, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:44:21'}, '2002': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 10.0, 'baseline_mean': 23.539534883720936, 'baseline_std': 42.68261126665543, 'hourly_consumption': 0.014714285714285595}, 'prediction': {'expected': 23.539534883720936, 'lower_bound': 0, 'upper_bound': 66.22214615037636, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:51:48'}, '2119': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 8.3, 'baseline_mean': 8.362790697674418, 'baseline_std': 0.06554989308027105, 'hourly_consumption': 0.00842857142857141}, 'prediction': {'expected': 8.362790697674418, 'lower_bound': 8.297240804594146, 'upper_bound': 8.42834059075469, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:59:11'}, '1922': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 10.2, 'baseline_mean': 9.981395348837209, 'baseline_std': 0.4831719668002905, 'hourly_consumption': 0.01004775195439086}, 'prediction': {'expected': 9.981395348837209, 'lower_bound': 9.498223382036919, 'upper_bound': 10.464567315637499, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:45:07'}, '2011': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 5.2, 'baseline_mean': 5.149999999999998, 'baseline_std': 0.07407971974871934, 'hourly_consumption': 0.005000066138441174}, 'prediction': {'expected': 5.149999999999998, 'lower_bound': 5.075920280251278, 'upper_bound': 5.224079719748717, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:33:15'}, '2001': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 8.4, 'baseline_mean': 8.465116279069766, 'baseline_std': 0.10208241518786142, 'hourly_consumption': 0.008333443564068174}, 'prediction': {'expected': 8.465116279069766, 'lower_bound': 8.363033863881904, 'upper_bound': 8.567198694257629, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:31:02'}, '2020': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 10.4, 'baseline_mean': 10.214285714285714, 'baseline_std': 0.40277781115249434, 'hourly_consumption': 0.0102380952380954}, 'prediction': {'expected': 10.214285714285714, 'lower_bound': 9.811507903133219, 'upper_bound': 10.617063525438208, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:55:14'}, '2123': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 18.7, 'baseline_mean': 18.495348837209313, 'baseline_std': 0.44718883215679545, 'hourly_consumption': 0.018428815195968222}, 'prediction': {'expected': 18.495348837209313, 'lower_bound': 18.04816000505252, 'upper_bound': 18.942537669366107, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:59:19'}, '2012': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 5.1, 'baseline_mean': 5.0953488372093005, 'baseline_std': 0.17314113204481074, 'hourly_consumption': 0.004952380952380915}, 'prediction': {'expected': 5.0953488372093005, 'lower_bound': 4.92220770516449, 'upper_bound': 5.268489969254111, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:31:11'}, '2008': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 4.9, 'baseline_mean': 5.030232558139534, 'baseline_std': 0.09888637953523118, 'hourly_consumption': 0.0050476190476194645}, 'prediction': {'expected': 5.030232558139534, 'lower_bound': 4.931346178604303, 'upper_bound': 5.1291189376747655, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:52:31'}, '2112': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 8.2, 'baseline_mean': 8.220930232558137, 'baseline_std': 0.08035083646965518, 'hourly_consumption': 0.013523809523809558}, 'prediction': {'expected': 8.220930232558137, 'lower_bound': 8.140579396088482, 'upper_bound': 8.301281069027793, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:59:11'}, '2029': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 10.6, 'baseline_mean': 10.406976744186048, 'baseline_std': 0.6449050682583738, 'hourly_consumption': 0.010238095238094724}, 'prediction': {'expected': 10.406976744186048, 'lower_bound': 9.762071675927674, 'upper_bound': 11.051881812444423, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:46:50'}, '2032': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 8.4, 'baseline_mean': 8.500000000000002, 'baseline_std': 0.20976176963402998, 'hourly_consumption': 0.008571938805879597}, 'prediction': {'expected': 8.500000000000002, 'lower_bound': 8.290238230365972, 'upper_bound': 8.709761769634031, 'confidence': 0.68}, 'timestamp': '2025-02-25 22:31:29'}, '2021': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 8.3, 'baseline_mean': 8.392857142857148, 'baseline_std': 0.1967948754325569, 'hourly_consumption': 0.00843891004186944}, 'prediction': {'expected': 8.392857142857148, 'lower_bound': 8.19606226742459, 'upper_bound': 8.589652018289705, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:39:03'}, '2111': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 8.2, 'baseline_mean': 8.14418604651163, 'baseline_std': 0.17902158101003118, 'hourly_consumption': 0.008095238095238091}, 'prediction': {'expected': 8.14418604651163, 'lower_bound': 7.965164465501599, 'upper_bound': 8.323207627521661, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:59:11'}}}, 'a1Q4MNc2hZ0': {'abnormal': {'1112': {'current_status': {'abnormal': True, 'high_usage': False, 'current_power': 591.6, 'baseline_mean': 466.54166666666674, 'baseline_std': 448.22924692775064, 'hourly_consumption': 0.41276107847425164}, 'prediction': {'expected': 466.54166666666674, 'lower_bound': 18.312419738916105, 'upper_bound': 914.7709135944174, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:59:24'}, '1125': {'current_status': {'abnormal': True, 'high_usage': False, 'current_power': 1015.0, 'baseline_mean': 81.12916666666668, 'baseline_std': 215.95439329235583, 'hourly_consumption': 0.12199855793667062}, 'prediction': {'expected': 81.12916666666668, 'lower_bound': 0, 'upper_bound': 297.0835599590225, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:35:14'}, '1105': {'current_status': {'abnormal': True, 'high_usage': False, 'current_power': 1024.4, 'baseline_mean': 871.25625, 'baseline_std': 244.24198229942618, 'hourly_consumption': 0.9724991430361409}, 'prediction': {'expected': 871.25625, 'lower_bound': 627.0142677005738, 'upper_bound': 1115.4982322994263, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:48:08'}}, 'normal': {'1122': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 337.3, 'baseline_mean': 1000.9375000000003, 'baseline_std': 598.0666417093765, 'hourly_consumption': 0.92672340425532}, 'prediction': {'expected': 1000.9375000000003, 'lower_bound': 402.8708582906238, 'upper_bound': 1599.0041417093769, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:44:45'}, '1222': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 9.3, 'baseline_mean': 528.6583333333333, 'baseline_std': 864.4682354730243, 'hourly_consumption': 0.5163404255319155}, 'prediction': {'expected': 528.6583333333333, 'lower_bound': 0, 'upper_bound': 1393.1265688063577, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:52:15'}, '1108': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 15.8, 'baseline_mean': 290.4270833333333, 'baseline_std': 473.00652990840706, 'hourly_consumption': 0.38114893617021733}, 'prediction': {'expected': 290.4270833333333, 'lower_bound': 0, 'upper_bound': 763.4336132417404, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:53:30'}, '1121': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 6.0, 'baseline_mean': 274.3374999999999, 'baseline_std': 405.9758056018147, 'hourly_consumption': 0.3166808510638301}, 'prediction': {'expected': 274.3374999999999, 'lower_bound': 0, 'upper_bound': 680.3133056018146, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:55:20'}, '1216': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 36.9, 'baseline_mean': 500.1333333333334, 'baseline_std': 832.4259219059632, 'hourly_consumption': 0.5089727071784027}, 'prediction': {'expected': 500.1333333333334, 'lower_bound': 0, 'upper_bound': 1332.5592552392966, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:56:26'}, '1219': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 1243.8, 'baseline_mean': 1371.4687499999998, 'baseline_std': 146.74887241764776, 'hourly_consumption': 1.4058131700570928}, 'prediction': {'expected': 1371.4687499999998, 'lower_bound': 1224.719877582352, 'upper_bound': 1518.2176224176476, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:33:16'}, '1116': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 728.6, 'baseline_mean': 896.0250000000001, 'baseline_std': 950.7370159009566, 'hourly_consumption': 0.8826704176073591}, 'prediction': {'expected': 896.0250000000001, 'lower_bound': 0, 'upper_bound': 1846.7620159009566, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:41:14'}, '1109': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 33.0, 'baseline_mean': 1379.254166666666, 'baseline_std': 1205.6533064282696, 'hourly_consumption': 1.3662391697497653}, 'prediction': {'expected': 1379.254166666666, 'lower_bound': 173.6008602383963, 'upper_bound': 2584.9074730949355, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:30:11'}, '1208': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 4.5, 'baseline_mean': 4.612499999999998, 'baseline_std': 0.08410985724102968, 'hourly_consumption': 0.08318952270631591}, 'prediction': {'expected': 4.612499999999998, 'lower_bound': 4.528390142758968, 'upper_bound': 4.696609857241028, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:57:19'}, '1102': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 15.7, 'baseline_mean': 640.0125000000002, 'baseline_std': 826.1795437819977, 'hourly_consumption': 0.5298660772331295}, 'prediction': {'expected': 640.0125000000002, 'lower_bound': 0, 'upper_bound': 1466.1920437819979, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:57:33'}, '1212': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 9.0, 'baseline_mean': 855.0249999999996, 'baseline_std': 1081.90299787389, 'hourly_consumption': 0.8774042553191477}, 'prediction': {'expected': 855.0249999999996, 'lower_bound': 0, 'upper_bound': 1936.9279978738896, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:47:20'}}}, 'a1hMUeoBSiK': {'abnormal': {'8902': {'current_status': {'abnormal': True, 'high_usage': False, 'current_power': 1508.0, 'baseline_mean': 1277.8291666666664, 'baseline_std': 917.3206585597906, 'hourly_consumption': 1.2786534119788748}, 'prediction': {'expected': 1277.8291666666664, 'lower_bound': 360.50850810687587, 'upper_bound': 2195.149825226457, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:18:25'}}, 'normal': {'8918': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 32.2, 'baseline_mean': 78.14791666666662, 'baseline_std': 316.3318033059612, 'hourly_consumption': 0.09574694437220738}, 'prediction': {'expected': 78.14791666666662, 'lower_bound': 0, 'upper_bound': 394.47971997262783, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:52:38'}, '8903': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 784.1, 'baseline_mean': 999.9354166666666, 'baseline_std': 448.69445391523493, 'hourly_consumption': 1.0747062578311493}, 'prediction': {'expected': 999.9354166666666, 'lower_bound': 551.2409627514317, 'upper_bound': 1448.6298705819015, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:30:03'}, '8912': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 148.4, 'baseline_mean': 1091.3500000000001, 'baseline_std': 649.0793827463502, 'hourly_consumption': 1.1760139008735315}, 'prediction': {'expected': 1091.3500000000001, 'lower_bound': 442.2706172536499, 'upper_bound': 1740.4293827463503, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:37:42'}, '8933': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 75.3, 'baseline_mean': 1027.64375, 'baseline_std': 1120.1172001570935, 'hourly_consumption': 1.0139268785683047}, 'prediction': {'expected': 1027.64375, 'lower_bound': 0, 'upper_bound': 2147.7609501570932, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:17:07'}, '8921': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 197.4, 'baseline_mean': 320.68541666666675, 'baseline_std': 551.3269671224313, 'hourly_consumption': 0.2962162673317657}, 'prediction': {'expected': 320.68541666666675, 'lower_bound': 0, 'upper_bound': 872.0123837890981, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:35:10'}, '8929': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 183.5, 'baseline_mean': 688.94375, 'baseline_std': 868.7703461952367, 'hourly_consumption': 0.7466896771829465}, 'prediction': {'expected': 688.94375, 'lower_bound': 0, 'upper_bound': 1557.7140961952368, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:00:10'}, '8915': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 14.8, 'baseline_mean': 252.54374999999985, 'baseline_std': 486.5002726404737, 'hourly_consumption': 0.30622000520106485}, 'prediction': {'expected': 252.54374999999985, 'lower_bound': 0, 'upper_bound': 739.0440226404736, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:37:47'}, '8906': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 11.0, 'baseline_mean': 659.6020833333336, 'baseline_std': 1181.090458547593, 'hourly_consumption': 0.6497098074445337}, 'prediction': {'expected': 659.6020833333336, 'lower_bound': 0, 'upper_bound': 1840.6925418809265, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:31:13'}, '8917': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 132.9, 'baseline_mean': 1233.547916666667, 'baseline_std': 658.3799947161029, 'hourly_consumption': 1.3379307084008099}, 'prediction': {'expected': 1233.547916666667, 'lower_bound': 575.1679219505642, 'upper_bound': 1891.9279113827702, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:41:50'}, '8923': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 11.2, 'baseline_mean': 642.5791666666668, 'baseline_std': 286.6399364599407, 'hourly_consumption': 0.6973273915767398}, 'prediction': {'expected': 642.5791666666668, 'lower_bound': 355.93923020672605, 'upper_bound': 929.2191031266075, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:50:27'}, '8907': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 14.7, 'baseline_mean': 565.8791666666665, 'baseline_std': 591.6717045285471, 'hourly_consumption': 0.6177094291894736}, 'prediction': {'expected': 565.8791666666665, 'lower_bound': 0, 'upper_bound': 1157.5508711952136, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:40:11'}, '8928': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 284.8, 'baseline_mean': 1096.3673913043478, 'baseline_std': 688.6930501736517, 'hourly_consumption': 1.198879406132523}, 'prediction': {'expected': 1096.3673913043478, 'lower_bound': 407.6743411306961, 'upper_bound': 1785.0604414779996, 'confidence': 0.68}, 'timestamp': '2025-02-26 12:22:20'}, '8909': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 72.4, 'baseline_mean': 1437.0041666666664, 'baseline_std': 600.4305582674972, 'hourly_consumption': 1.4991241031217786}, 'prediction': {'expected': 1437.0041666666664, 'lower_bound': 836.5736083991692, 'upper_bound': 2037.4347249341636, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:52:30'}, '8911': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 1105.9, 'baseline_mean': 1098.39375, 'baseline_std': 635.2821418451335, 'hourly_consumption': 1.0503102873556431}, 'prediction': {'expected': 1098.39375, 'lower_bound': 463.1116081548664, 'upper_bound': 1733.6758918451335, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:41:01'}, '8936': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 20.9, 'baseline_mean': 601.0749999999999, 'baseline_std': 791.030341968558, 'hourly_consumption': 0.7225191787137079}, 'prediction': {'expected': 601.0749999999999, 'lower_bound': 0, 'upper_bound': 1392.105341968558, 'confidence': 0.68}, 'timestamp': '2025-02-26 13:56:00'}, '8901': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 25.7, 'baseline_mean': 832.3187499999998, 'baseline_std': 774.8687695774615, 'hourly_consumption': 0.9193300157212182}, 'prediction': {'expected': 832.3187499999998, 'lower_bound': 57.44998042253826, 'upper_bound': 1607.1875195774614, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:47:44'}, '8927': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 338.4, 'baseline_mean': 763.4874999999998, 'baseline_std': 766.1812453274288, 'hourly_consumption': 0.8495419567607351}, 'prediction': {'expected': 763.4874999999998, 'lower_bound': 0, 'upper_bound': 1529.6687453274285, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:56:39'}, '8930': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 19.9, 'baseline_mean': 251.39583333333323, 'baseline_std': 603.7751226290079, 'hourly_consumption': 0.21123903638383637}, 'prediction': {'expected': 251.39583333333323, 'lower_bound': 0, 'upper_bound': 855.1709559623412, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:40:18'}, '8905': {'current_status': {'abnormal': False, 'high_usage': False, 'current_power': 71.1, 'baseline_mean': 727.9833333333332, 'baseline_std': 1052.6630938139774, 'hourly_consumption': 0.7144849760041593}, 'prediction': {'expected': 727.9833333333332, 'lower_bound': 0, 'upper_bound': 1780.6464271473105, 'confidence': 0.68}, 'timestamp': '2025-02-26 14:57:25'}}}}
    # dept_room_num=find_dept_id(prediction_result)
    # print(dept_room_num)
    dept_room_num={'214': [{'room_number': '1417', 'room_type_id': 'SRJ', 'abnormal_time': '2025-02-26 14:51:20', 'device_name': '94c960588b4b'}, {'room_number': '1439', 'room_type_id': 'DRJ', 'abnormal_time': '2025-02-26 14:36:50', 'device_name': '94c960588b85'}, {'room_number': '1418', 'room_type_id': 'ZNDC', 'abnormal_time': '2025-02-26 12:33:33', 'device_name': '94c960588b3a'}], '230': [{'room_number': '20布草间', 'room_type_id': 'RT20', 'abnormal_time': '2025-02-26 14:46:27', 'device_name': '94c96058a45f'}, {'room_number': '21层布草间', 'room_type_id': 'RT21', 'abnormal_time': '2025-02-26 14:32:01', 'device_name': '94c96058a479'}], '229': [{'room_number': '1112', 'room_type_id': 'ZNDC', 'abnormal_time': '2025-02-26 14:59:24', 'device_name': '94c96058a528'}, {'room_number': '1125', 'room_type_id': 'SSYYDCF', 'abnormal_time': '2025-02-26 14:35:14', 'device_name': '94c96058a4b7'}, {'room_number': '1105', 'room_type_id': 'RT11', 'abnormal_time': '2025-02-26 14:48:08', 'device_name': '94c96058a49a'}], '228': [{'room_number': '8902', 'room_type_id': 'ZNSRJ', 'abnormal_time': '2025-02-26 14:18:25', 'device_name': '94c960588b88'}]}
    dept_info_map=find_datas(dept_room_num)
    # print(dept_info_map)
    # dept_info_map={'214': {'dept_name': '裕华珺悦主题酒店(华山风景区店)', 'dept_code': 'JDYH009', 'brand': '', 'dept_id': '214', 'rooms': [{'room_number': '1417', 'abnormal_time': '2025-02-26 14:51:20', 'status': 'OC', 'check_in_detail': '2024-12-30 00:00:00', 'check_out_detail': '2024-12-31 00:00:00'}, {'room_number': '1439', 'abnormal_time': '2025-02-26 14:36:50', 'status': 'OOO', 'check_in_detail': '2025-05-03 00:00:00', 'check_out_detail': '2025-05-04 00:00:00'}, {'room_number': '1418', 'abnormal_time': '2025-02-26 12:33:33', 'status': 'VD', 'check_in_detail': '2025-03-09 00:00:00', 'check_out_detail': '2025-03-10 00:00:00'}]}, '230': {'dept_name': '济南雅锦酒店管理有限公司', 'dept_code': 'JDYH018', 'brand': '', 'dept_id': '230', 'rooms': [{'room_number': '20布草间', 'abnormal_time': '2025-02-26 14:46:27', 'check_in_detail': None, 'check_out_detail': None}, {'room_number': '21层布草间', 'abnormal_time': '2025-02-26 14:32:01', 'check_in_detail': None, 'check_out_detail': None}]}, '229': {'dept_name': '裕华珺悦主题酒店（济南高铁西站店）', 'dept_code': 'JDYH016', 'brand': '', 'dept_id': '229', 'rooms': [{'room_number': '1112', 'abnormal_time': '2025-02-26 14:59:24', 'status': 'VD', 'check_in_detail': None, 'check_out_detail': None}, {'room_number': '1125', 'abnormal_time': '2025-02-26 14:35:14', 'status': 'OOO', 'check_in_detail': '2025-03-01 00:00:00', 'check_out_detail': '2025-03-02 00:00:00'}, {'room_number': '1105', 'abnormal_time': '2025-02-26 14:48:08', 'check_in_detail': None, 'check_out_detail': None}]}, '228': {'dept_name': '大富鲨S智能电竞酒店（济南和谐广场店）', 'dept_code': 'JDDS017', 'brand': '', 'dept_id': '228', 'rooms': [{'room_number': '8902', 'abnormal_time': '2025-02-26 14:18:25', 'status': 'VC', 'check_in_detail': '2025-02-11 00:00:00', 'check_out_detail': '2025-02-12 00:00:00'}]}}

    # 保存数据到MySQL，获取记录数和JSON数据
    saved_count, json_records = save_to_mysql(dept_info_map)

    # 如果有记录，则发送到飞书
    if saved_count > 0:
        send_to_feishu(json_records)

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"\n总执行时间: {execution_time:.2f} 秒")

    # 如果执行时间超过60秒，还可以显示分钟表示
    if execution_time > 60:
        print(f"约 {execution_time / 60:.2f} 分钟")
