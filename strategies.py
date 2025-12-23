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
        flat_today = (ma1_pct == 0) & (ma2_pct == 0)
        flat_prev = (ma1_pct.shift(1) == 0) & (ma2_pct.shift(1) == 0)
        return flat_today & flat_prev

class EqMA2DaysStrategy(BaseStrategy):
    def __init__(self, ma1: int, ma2: int):
        self.ma1 = f'MA{ma1}'
        self.ma2 = f'MA{ma2}'

    def calculate_signals(self, df: pd.DataFrame) -> pd.Series:
        diff_pct = (df[self.ma1] - df[self.ma2]).abs() / df[self.ma2]
        eq_today = diff_pct == 0
        eq_prev = diff_pct.shift(1) == 0
        return eq_today & eq_prev

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

WEEKLY_STRATEGIES = [
    '60_5_20_10_to_60_5_10_20',
    '60_5_10_20_to_5_60_10_20',
    '60_5_20_10_to_5_60_20_10',
    '60_20_5_10_to_60_5_20_10',
    '20_60_5_10_to_5_60_20_10',
    '20_10_5_60_to_20_5_10_60'
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
    '60_5_20_10_to_60_5_10_20': WeeklySequenceStrategy([60, 5, 20, 10], [60, 5, 10, 20]),
    '60_5_10_20_to_5_60_10_20': WeeklySequenceStrategy([60, 5, 10, 20], [5, 60, 10, 20]),
    '60_5_20_10_to_5_60_20_10': WeeklySequenceStrategy([60, 5, 20, 10], [5, 60, 20, 10]),
    '60_20_5_10_to_60_5_20_10': WeeklySequenceStrategy([60, 20, 5, 10], [60, 5, 20, 10]),
    '20_60_5_10_to_5_60_20_10': WeeklySequenceStrategy([20, 60, 5, 10], [5, 60, 20, 10]),
    '20_10_5_60_to_20_5_10_60': WeeklySequenceStrategy([20, 10, 5, 60], [20, 5, 10, 60]),
}

def get_strategy(name: str) -> BaseStrategy:
    return STRATEGY_MAP.get(name)
