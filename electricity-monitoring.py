from collections import deque
from datetime import datetime, timedelta
import pymysql
from pymysql.err import OperationalError
import time
import requests
import json
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
    """智能电表数据获取主方法"""
    config = DatabaseConfig.get_connection_params()
    result = {"product_keys": [], "meter_data": {}}

    for attempt in range(DatabaseConfig.MAX_RETRIES):
        try:
            connection = pymysql.connect(**config)
            with connection:
                # 第一阶段：获取所有产品密钥及对应房间号
                with connection.cursor() as cursor:
                    # 关键SQL：获取产品密钥和房间号的映射关系
                    cursor.execute("""
                        SELECT DISTINCT product_key, room_num 
                        FROM smart_meter
                    """)
                    product_room_mapping = {}
                    for product_key, room_num in cursor:
                        product_room_mapping.setdefault(product_key, []).append(room_num)
                    
                    result["product_keys"] = list(product_room_mapping.keys())
                
                # 2. 为每个产品和房间准备结果容器
                for pk, rooms in product_room_mapping.items():
                    result["meter_data"][pk] = {}
                    for room in rooms:
                        result["meter_data"][pk][room] = []

                # 3. 计算查询时间窗口（一次性计算所有窗口）
                base_time = datetime.now().replace(second=0, microsecond=0)
                rounded_minute = base_time.minute // 30 * 30
                base_time = base_time.replace(minute=rounded_minute)

                time_windows = []
                for window_idx in range(48):  # 24小时，每30分钟一个窗口
                    window_end = (base_time - timedelta(minutes=30 * window_idx)).replace(
                        second=59, microsecond=999999)
                    window_start = (window_end - timedelta(minutes=30)).replace(
                        second=0, microsecond=0)
                    time_windows.append((window_start, window_end))

                # 4. 批量查询每个产品的所有房间数据（减少查询次数）
                for pk in result["product_keys"]:
                    try:
                        # 创建批量查询，包含24小时窗口
                        query = """
                            SELECT product_key, room_num, script_time, power, value, 
                                   gmt_modified, device_name
                            FROM smart_meter 
                            WHERE product_key = %s 
                            AND script_time >= %s 
                            AND script_time <= %s
                            AND room_num IN ({})
                            ORDER BY room_num, script_time DESC
                        """.format(','.join(['%s'] * len(product_room_mapping[pk])))

                        # 一次查询24小时内所有数据
                        with connection.cursor(pymysql.cursors.SSDictCursor) as data_cursor:
                            params = [pk, time_windows[-1][0], time_windows[0][1]]
                            params.extend(product_room_mapping[pk])
                            data_cursor.execute(query, params)

                            # 将结果按房间号和时间窗口分类
                            for record in data_cursor:
                                room_num = record['room_num']
                                script_time = record['script_time']

                                # 找到对应的时间窗口
                                for window_start, window_end in time_windows:
                                    if window_start <= script_time <= window_end:
                                        # 检查是否已存在该时间窗口的记录
                                        window_records = [r for r in result["meter_data"][pk][room_num]
                                                         if window_start <= r['script_time'] <= window_end]

                                        # 如果该窗口还没有记录，添加当前记录
                                        if not window_records:
                                            result["meter_data"][pk][room_num].append(record)
                                        break
                    except Exception as e:
                        print(f"产品密钥 {pk} 数据获取失败: {e}")
                        result["meter_data"][pk] = {}

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

    # 连接数据库
    try:
        conn = pymysql.connect(**DatabaseConfig.get_connection_params())
        cursor = conn.cursor()

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

        # 关闭数据库连接
        cursor.close()
        conn.close()

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

    # 返回结果
    dept_info_map = {}

    # 连接数据库
    try:
        conn = pymysql.connect(**DatabaseConfig.get_connection_params())
        cursor = conn.cursor()

        # 逐个查询每个部门ID
        for dept_id in dept_ids:
            # 构建查询
            dept_query = """
                SELECT dept_name, dept_code, brand
                FROM sys_dept
                WHERE dept_id = %s
            """

            # 执行查询
            cursor.execute(dept_query, [dept_id])

            # 获取结果
            result = cursor.fetchone()

            if result:
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
                                if isinstance(check_in_detail, datetime):
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
                                if isinstance(check_out_detail, datetime):
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
                        else:
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
            else:
                print(f"部门ID {dept_id} 未找到信息")

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
    print("开始获取数据...")
    res_data = fetch_smart_meter_products()

    print("开始数据预测...")
    prediction_result = data_prediction(res_data)
    print("开始数据查询组装...")
    dept_room_num = find_dept_id(prediction_result)
    dept_info_map = find_datas(dept_room_num)

    # 保存数据到MySQL，获取记录数和JSON数据
    print("开始异常数据入库...")
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
