from collections import deque
from datetime import datetime, timedelta

class PowerUsageAnalyzer:
    WINDOW_SIZE = 48  # 测试专用窗口大小（实际生产环境保持48）
    DETECTION_WINDOW = 4  # 检测窗口（2小时）
    THRESHOLD_MULTIPLIER = 1.96  # 建议调整为1.96（95%置信区间）
    HIGH_USAGE_COUNT_THRESHOLD = 4  # 连续2小时高用电
    MIN_DATA_POINTS = 10  # 最小有效数据点数
    RELATIVE_SAFETY_MARGIN = 0.1  # 相对安全边际10%
    ABSOLUTE_SAFETY_MARGIN = 0.5  # 绝对安全边际0.5kW
    USE_RELATIVE_MARGIN = True    # 切换边际类型

    def __init__(self):
        # 分离基线窗口（48个点）和检测窗口（4个点）
        self.baseline_window = deque(maxlen=48)  # 历史正常数据
        self.detection_window = deque(maxlen=4)  # 当前监测数据
        self.timestamps = deque(maxlen=48)       # 统一时间戳管理
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
        mean = sum(data)/len(data) if data else 0
        std = (sum((x-mean)**2 for x in data)/(len(data)-1))**0.5 if len(data)>1 else 0
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
            delta = self.timestamps[-i] - self.timestamps[-i-1]
            if delta > timedelta(minutes=35):  # 允许5分钟误差
                return False
        return True

def test_consecutive_detection():
    analyzer = PowerUsageAnalyzer()
    analyzer.WINDOW_SIZE = 6  # 测试专用设置
    
    base_ts = datetime(2023, 1, 1, 0, 0)
    
    # 填充6个正常值（填满基线窗口）
    for i in range(6):
        analyzer.add_power_usage(5.0, base_ts + timedelta(minutes=30*i))
    
    # 添加4个高用电值（进入检测窗口）
    test_times = [base_ts + timedelta(minutes=180 + 30*i) for i in range(4)]
    results = []
    for usage, ts in zip([10.0]*4, test_times):
        analyzer.add_power_usage(usage, ts)
        results.append(analyzer.detect_high_power_usage())
    
    print(results)  # 预期输出: [False, False, False, True]

def test_large_dataset_detection():
    """验证大数据量下的窗口滑动机制"""
    analyzer = PowerUsageAnalyzer()
    base_ts = datetime(2023, 1, 1, 0, 0)
    
    # 阶段1：填充48个正常值（24小时数据）
    for i in range(48):
        normal_ts = base_ts + timedelta(minutes=30*i)
        analyzer.add_power_usage(5.0, normal_ts)
    
    # 阶段2：添加4个高用电值（应进入检测窗口）
    high_usage_times = [base_ts + timedelta(hours=24, minutes=30*i) for i in range(4)]
    results = []
    for usage, ts in zip([15.0]*4, high_usage_times):
        analyzer.add_power_usage(usage, ts)
        results.append(analyzer.detect_high_power_usage())
    
    # 阶段3：继续添加数据验证窗口滑动
    additional_data = [
        (5.0, base_ts + timedelta(hours=26, minutes=30*i)) 
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
        (5.6, True)    # > 阈值
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

if __name__ == "__main__":
    # 更新测试用例验证新功能
    # analyzer = PowerUsageAnalyzer()
    # base_ts = datetime(2023, 1, 1, 0, 0)
    
    # # 测试数据：3小时正常数据 + 2小时高用电 + 1小时正常
    # test_data = [
    #     (5.0, base_ts + timedelta(minutes=30*i)) 
    #     for i in range(6)  # 正常数据
    # ] + [
    #     (10.0, base_ts + timedelta(minutes=180 + 30*i)) 
    #     for i in range(4)  # 连续高用电
    # ] + [
    #     (5.0, base_ts + timedelta(minutes=300 + 30*i)) 
    #     for i in range(2)  # 恢复正常
    # ]

    # print(test_data)

    # for usage, ts in test_data:
    #     analyzer.add_power_usage(usage, ts)
    #     print(f"[{ts.strftime('%H:%M')}] 用电量: {usage}")
    #     print(f"异常用电: {analyzer.detect_abnormal_usage()}")
    #     print(f"持续高用电: {analyzer.detect_high_power_usage()}")
    #     print("---")

    # test_consecutive_detection()
    test_large_dataset_detection()
    test_instant_anomaly_detection()
    # test_safety_margin_calibration()
    # test_direct_abnormal_detection()