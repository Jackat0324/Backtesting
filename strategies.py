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

# --- Weekly Strategy Shortcuts ---
SEQUENCE_SHORTCUTS = {
    '1':   [60, 20, 10, 5],
    '1-1': [10, 5, 20, 60],
    '2':   [60, 20, 5, 10],
    '2-1': [20, 10, 5, 60],
    '2-2': [20, 60, 5, 10],
    '3':   [60, 5, 20, 10],
    '3-1': [60, 10, 5, 20],
    '7-1': [20, 5, 10, 60],
    '7-2': [20, 5, 60, 10],
    '7-3': [5, 60, 20, 10],
    '7-5': [60, 10, 20, 5],
    '7-7': [60, 5, 10, 20],
    '7-8': [5, 60, 10, 20],
    '9':   [5, 10, 60, 20],
    '10':  [5, 10, 20, 60],
    'A':   [20, 60, 10, 5],
    'A+1': [10, 20, 5, 60]
}

# --- Weekly Strategies ---

class MultiWeekSequenceStrategy(BaseStrategy):
    def __init__(self, sequences: list):
        """
        sequences: A list of sequences. Each element can be a list of integers 
                   or a shortcut string from SEQUENCE_SHORTCUTS.
                   sequences[0] is T, sequences[1] is T-1, etc.
        """
        resolved_sequences = []
        for seq in sequences:
            if isinstance(seq, str):
                resolved_sequences.append(SEQUENCE_SHORTCUTS.get(seq, []))
            else:
                resolved_sequences.append(seq)
        
        self.sequences = [[f'MA{p}' for p in seq] for seq in resolved_sequences]

    def _check_sequence(self, df, seq, shift=0):
        conditions = []
        for i in range(len(seq) - 1):
            conditions.append(df[seq[i]].shift(shift) > df[seq[i+1]].shift(shift))
        
        res = conditions[0]
        for c in conditions[1:]:
            res = res & c
        return res

    def calculate_signals(self, df: pd.DataFrame) -> pd.Series:
        # Check sequences for each week
        combined_condition = None
        for i, seq in enumerate(self.sequences):
            condition = self._check_sequence(df, seq, shift=i)
            if combined_condition is None:
                combined_condition = condition
            else:
                combined_condition = combined_condition & condition
        
        # 共同條件：T 的 5MA > T-1 的 5MA 且 T 的 10MA > T-1 的 10MA
        trend_ma5 = df['MA5'] > df['MA5'].shift(1)
        trend_ma10 = df['MA10'] > df['MA10'].shift(1)
        
        return combined_condition & trend_ma5 & trend_ma10

class WeeklySequenceStrategy(MultiWeekSequenceStrategy):
    def __init__(self, prev_seq: list, curr_seq: list):
        # WeeklySequenceStrategy was (prev (T-1), curr (T0))
        # MultiWeekSequenceStrategy expects (T0, T-1, ...)
        super().__init__([curr_seq, prev_seq])

class ThreeWeekSequenceStrategy(MultiWeekSequenceStrategy):
    def __init__(self, t2_seq: list, t1_seq: list, t0_seq: list):
        # ThreeWeekSequenceStrategy was (T-2, T-1, T0)
        # MultiWeekSequenceStrategy expects (T0, T-1, T-2)
        super().__init__([t0_seq, t1_seq, t2_seq])

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
    '3to7-7',
    '7-7to7-8',
    '3to7-3',
    '2to3',
    '2-2to7-3',
    '2-1to7-1',
    '3-1to7-7',
    '3-1to7-8',
    '3-1to9',
    '2to7-3',
    '3to7-8',
    '1-1to10',
    '1-1toA+1',
    '10to1-1to10',
    '3-1to7-5to3',
    '1to7-5to7-7',
    '1to7-5to1',
    '1to7-5to3',
    '7-5to7-7to3-1',
    '2-2to2-2to2-2to7-3',
    'Ato2-2to7-2to7-2',
    '2-2to2-2to7-3to7-3'
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
    '3to7-7': MultiWeekSequenceStrategy(['7-7', '3']),
    '7-7to7-8': MultiWeekSequenceStrategy(['7-8', '7-7']),
    '3to7-3': MultiWeekSequenceStrategy(['7-3', '3']),
    '2to3': MultiWeekSequenceStrategy(['3', '2']),
    '2-2to7-3': MultiWeekSequenceStrategy(['7-3', '2-2']),
    '2-1to7-1': MultiWeekSequenceStrategy(['7-1', '2-1']),
    '3-1to7-7': MultiWeekSequenceStrategy(['7-7', '3-1']),
    '3-1to7-8': MultiWeekSequenceStrategy(['7-8', '3-1']),
    '3-1to9': MultiWeekSequenceStrategy(['9', '3-1']),
    '2to7-3': MultiWeekSequenceStrategy(['7-3', '2']),
    '3to7-8': MultiWeekSequenceStrategy(['7-8', '3']),
    '1-1to10': MultiWeekSequenceStrategy(['10', '1-1']),
    '1-1toA+1': MultiWeekSequenceStrategy(['A+1', '1-1']),
    '10to1-1to10': MultiWeekSequenceStrategy(['10', '1-1', '10']),
    '3-1to7-5to3': MultiWeekSequenceStrategy(['3', '7-5', '3-1']),
    '1to7-5to7-7': MultiWeekSequenceStrategy(['7-7', '7-5', '1']),
    '1to7-5to1': MultiWeekSequenceStrategy(['1', '7-5', '1']),
    '1to7-5to3': MultiWeekSequenceStrategy(['3', '7-5', '1']),
    '7-5to7-7to3-1': MultiWeekSequenceStrategy(['3-1', '7-7', '7-5']),
    '2-2to2-2to2-2to7-3': MultiWeekSequenceStrategy(['7-3', '2-2', '2-2', '2-2']),
    'Ato2-2to7-2to7-2': MultiWeekSequenceStrategy(['7-2', '7-2', '2-2', 'A']),
    '2-2to2-2to7-3to7-3': MultiWeekSequenceStrategy(['7-3', '7-3', '2-2', '2-2']),
}

def get_strategy(name: str) -> BaseStrategy:
    return STRATEGY_MAP.get(name)
