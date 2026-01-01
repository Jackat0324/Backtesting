import pandas as pd
from abc import ABC, abstractmethod

class BaseStrategy(ABC):
    @abstractmethod
    def calculate_signals(self, df: pd.DataFrame) -> pd.Series:
        """
        Calculate signals for the given DataFrame.
        Returns a boolean Series of the same length as df.
        """
        pass

# --- Daily Strategies ---

class FlatMAStrategy(BaseStrategy):
    def __init__(self, ma1: int, ma2: int):
        self.ma1 = f'MA{ma1}'
        self.ma2 = f'MA{ma2}'

    def calculate_signals(self, df: pd.DataFrame) -> pd.Series:
        ma1_pct = df[self.ma1].pct_change(fill_method=None).abs()
        ma2_pct = df[self.ma2].pct_change(fill_method=None).abs()
        
        # 前兩天 (T-1, T-2) 平整
        flat_t1 = (ma1_pct.shift(1) == 0) & (ma2_pct.shift(1) == 0)
        flat_t2 = (ma1_pct.shift(2) == 0) & (ma2_pct.shift(2) == 0)
        
        # 第三天 (T) MA5 與 MA10 向上
        trend = (df['MA5'] > df['MA5'].shift(1)) & (df['MA10'] > df['MA10'].shift(1))
        
        return flat_t1 & flat_t2 & trend

class EqMA2DaysStrategy(BaseStrategy):
    def __init__(self, ma1: int, ma2: int):
        self.ma1 = f'MA{ma1}'
        self.ma2 = f'MA{ma2}'

    def calculate_signals(self, df: pd.DataFrame) -> pd.Series:
        diff_pct = (df[self.ma1] - df[self.ma2]).abs() / df[self.ma2]
        
        # 前兩天 (T-1, T-2) 相等
        eq_t1 = diff_pct.shift(1) == 0
        eq_t2 = diff_pct.shift(2) == 0
        
        # 第三天 (T) MA5 與 MA10 向上
        trend = (df['MA5'] > df['MA5'].shift(1)) & (df['MA10'] > df['MA10'].shift(1))
        
        return eq_t1 & eq_t2 & trend

class CrossMAStrategy(BaseStrategy):
    def __init__(self, short_ma: int, long_ma: int):
        self.short_ma = f'MA{short_ma}'
        self.long_ma = f'MA{long_ma}'

    def calculate_signals(self, df: pd.DataFrame) -> pd.Series:
        cross_today = df[self.short_ma] > df[self.long_ma]
        cross_prev = df[self.short_ma].shift(1) <= df[self.long_ma].shift(1)
        return cross_today & cross_prev

# --- Weekly Strategies ---

class WeeklySequenceStrategy(BaseStrategy):
    def __init__(self, prev_seq: list, curr_seq: list):
        self.prev_seq = [f'MA{p}' for p in prev_seq]
        self.curr_seq = [f'MA{c}' for c in curr_seq]

    def _check_sequence(self, df, seq, shift=0):
        conditions = []
        for i in range(len(seq) - 1):
            if shift > 0:
                conditions.append(df[seq[i]].shift(shift) > df[seq[i+1]].shift(shift))
            else:
                conditions.append(df[seq[i]] > df[seq[i+1]])
        
        res = conditions[0]
        for c in conditions[1:]:
            res = res & c
        return res

    def calculate_signals(self, df: pd.DataFrame) -> pd.Series:
        c_prev = self._check_sequence(df, self.prev_seq, shift=1)
        c_curr = self._check_sequence(df, self.curr_seq, shift=0)
        
        # 新增條件：T 的 5MA > T-1 的 5MA 且 T 的 10MA > T-1 的 10MA
        trend_ma5 = df['MA5'] > df['MA5'].shift(1)
        trend_ma10 = df['MA10'] > df['MA10'].shift(1)
        
        return c_prev & c_curr & trend_ma5 & trend_ma10

class ThreeWeekSequenceStrategy(BaseStrategy):
    def __init__(self, t2_seq: list, t1_seq: list, t0_seq: list):
        self.t2_seq = [f'MA{p}' for p in t2_seq]
        self.t1_seq = [f'MA{p}' for p in t1_seq]
        self.t0_seq = [f'MA{c}' for c in t0_seq]

    def _check_sequence(self, df, seq, shift=0):
        conditions = []
        for i in range(len(seq) - 1):
            conditions.append(df[seq[i]].shift(shift) > df[seq[i+1]].shift(shift))
        
        res = conditions[0]
        for c in conditions[1:]:
            res = res & c
        return res

    def calculate_signals(self, df: pd.DataFrame) -> pd.Series:
        c_t2 = self._check_sequence(df, self.t2_seq, shift=2)
        c_t1 = self._check_sequence(df, self.t1_seq, shift=1)
        c_t0 = self._check_sequence(df, self.t0_seq, shift=0)
        
        # 條件：T 的 5MA > T-1 的 5MA 且 T 的 10MA > T-1 的 10MA
        trend_ma5 = df['MA5'] > df['MA5'].shift(1)
        trend_ma10 = df['MA10'] > df['MA10'].shift(1)
        
        return c_t2 & c_t1 & c_t0 & trend_ma5 & trend_ma10

# --- Strategy Lists for GUI ---

DAILY_STRATEGIES = [
    'MA5_MA10_Flat',
    'MA10_MA20_Flat',
    'MA5_MA20_Flat',
    'MA5_Eq_MA10_2Days',
    'MA10_Eq_MA20_2Days',
    'MA5_Eq_MA20_2Days',
    'MA5_cross_MA10'
]

# --- Weekly Strategy Code Mapping (MA Sequence Shortcuts) ---
# 1:   60_20_10_5
# 1-1: 10_5_20_60
# 2:   60_20_5_10
# 2-1: 20_10_5_60
# 2-2: 20_60_5_10
# 3:   60_5_20_10
# 3-1: 60_10_5_20
# 7-1: 20_5_10_60
# 7-3: 5_60_20_10
# 7-5: 60_10_20_5
# 7-7: 60_5_10_20
# 7-8: 5_60_10_20
# 9:   5_10_60_20
# 10:  5_10_20_60
# A+1: 10_20_5_60

WEEKLY_STRATEGIES = [
    '3_to_7-7',
    '7-7_to_7-8',
    '3_to_7-3',
    '2_to_3',
    '2-2_to_7-3',
    '2-1_to_7-1',
    '3-1_to_7-7',
    '3-1_to_7-8',
    '3-1_to_9',
    '2_to_7-3',
    '3_to_7-8',
    '1-1_to_10',
    '1-1_to_A+1',
    '10_to_1-1_to_10',
    '3-1_to_7-5_to_3',
    '1_to_7-5_to_7-7',
    '1_to_7-5_to_1',
    '1_to_7-5_to_3',
    '7-5_to_7-7_to_3-1'
]

# --- Strategy Factory / Registry ---

STRATEGY_MAP = {
    # Daily
    'MA5_MA10_Flat': FlatMAStrategy(5, 10),
    'MA10_MA20_Flat': FlatMAStrategy(10, 20),
    'MA5_MA20_Flat': FlatMAStrategy(5, 20),
    'MA5_Eq_MA10_2Days': EqMA2DaysStrategy(5, 10),
    'MA10_Eq_MA20_2Days': EqMA2DaysStrategy(10, 20),
    'MA5_Eq_MA20_2Days': EqMA2DaysStrategy(5, 20),
    'MA5_cross_MA10': CrossMAStrategy(5, 10),
    
    # Weekly
    '3_to_7-7': WeeklySequenceStrategy([60, 5, 20, 10], [60, 5, 10, 20]),
    '7-7_to_7-8': WeeklySequenceStrategy([60, 5, 10, 20], [5, 60, 10, 20]),
    '3_to_7-3': WeeklySequenceStrategy([60, 5, 20, 10], [5, 60, 20, 10]),
    '2_to_3': WeeklySequenceStrategy([60, 20, 5, 10], [60, 5, 20, 10]),
    '2-2_to_7-3': WeeklySequenceStrategy([20, 60, 5, 10], [5, 60, 20, 10]),
    '2-1_to_7-1': WeeklySequenceStrategy([20, 10, 5, 60], [20, 5, 10, 60]),
    '3-1_to_7-7': WeeklySequenceStrategy([60, 10, 5, 20], [60, 5, 10, 20]),
    '3-1_to_7-8': WeeklySequenceStrategy([60, 10, 5, 20], [5, 60, 10, 20]),
    '3-1_to_9': WeeklySequenceStrategy([60, 10, 5, 20], [5, 10, 60, 20]),
    '2_to_7-3': WeeklySequenceStrategy([60, 20, 5, 10], [5, 60, 20, 10]),
    '3_to_7-8': WeeklySequenceStrategy([60, 5, 20, 10], [5, 60, 10, 20]),
    '1-1_to_10': WeeklySequenceStrategy([10, 5, 20, 60], [5, 10, 20, 60]),
    '1-1_to_A+1': WeeklySequenceStrategy([10, 5, 20, 60], [10, 20, 5, 60]),
    '10_to_1-1_to_10': ThreeWeekSequenceStrategy([5, 10, 20, 60], [10, 5, 20, 60], [5, 10, 20, 60]),
    '3-1_to_7-5_to_3': ThreeWeekSequenceStrategy([60, 10, 5, 20], [60, 10, 20, 5], [60, 5, 20, 10]),
    '1_to_7-5_to_7-7': ThreeWeekSequenceStrategy([60, 20, 10, 5], [60, 10, 20, 5], [60, 5, 10, 20]),
    '1_to_7-5_to_1': ThreeWeekSequenceStrategy([60, 20, 10, 5], [60, 10, 20, 5], [60, 20, 10, 5]),
    '1_to_7-5_to_3': ThreeWeekSequenceStrategy([60, 20, 10, 5], [60, 10, 20, 5], [60, 5, 20, 10]),
    '7-5_to_7-7_to_3-1': ThreeWeekSequenceStrategy([60, 10, 20, 5], [60, 5, 10, 20], [60, 10, 5, 20]),
}

def get_strategy(name: str) -> BaseStrategy:
    return STRATEGY_MAP.get(name)
