import pandas as pd
from abc import ABC, abstractmethod
import re

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
    '2-3': [20, 10, 60, 5],
    '3':   [60, 5, 20, 10],
    '3-1': [60, 10, 5, 20],
    '7-1': [20, 5, 10, 60],
    '7-2': [20, 5, 60, 10],
    '7-3': [5, 60, 20, 10],
    '7-4': [5, 20, 60, 10],
    '7-5': [60, 10, 20, 5],
    '7-7': [60, 5, 10, 20],
    '7-8': [5, 60, 10, 20],
    '7-9': [5, 20, 10, 60],
    '9':   [5, 10, 60, 20],
    '10':  [5, 10, 20, 60],
    'A':   [20, 60, 10, 5],
    'B':   [10, 60, 20, 5],
    'B1':  [10, 5, 60, 20],
    'B3':  [10, 60, 5, 20],
    'B4':  [10, 20, 60, 5],
    'A+1': [10, 20, 5, 60]
}

# --- Weekly & Daily Sequence Strategies ---
# (Commonly used with SEQUENCE_SHORTCUTS)

class MultiSequenceStrategy(BaseStrategy):
    def __init__(self, sequences: list):
        """
        sequences: A list of sequences. Each element can be a list of integers 
                   or a shortcut string from SEQUENCE_SHORTCUTS.
                   sequences[0] is T, sequences[1] is T-1, etc.
        """
        resolved_sequences = []
        for seq in sequences:
            if isinstance(seq, str):
                shortcut = SEQUENCE_SHORTCUTS.get(seq)
                if shortcut:
                    resolved_sequences.append([f'MA{p}' for p in shortcut])
                else:
                    # It's a custom expression like 'MA5=MA10'
                    resolved_sequences.append(seq)
            else:
                resolved_sequences.append([f'MA{p}' for p in seq])
        
        self.sequences = resolved_sequences

    def _check_sequence(self, df, seq, shift=0):
        if isinstance(seq, str):
            # Handle expression strings (e.g., 'MA5=MA10')
            # Replace '=' with '==' but avoid changing '>=', '<=', '!=', '=='
            expr = re.sub(r'(?<![<>!=])=(?![=])', '==', seq)
            try:
                # Calculate the condition then shift the result
                return df.eval(expr).shift(shift).fillna(False)
            except Exception as e:
                return pd.Series(False, index=df.index)
        else:
            # Handle standard list of MAs (e.g., ['MA60', 'MA20', 'MA10', 'MA5'])
            if not seq:
                return pd.Series(True, index=df.index)
                
            conditions = []
            for i in range(len(seq) - 1):
                conditions.append(df[seq[i]].shift(shift) > df[seq[i+1]].shift(shift))
            
            res = conditions[0]
            for c in conditions[1:]:
                res = res & c
            return res.fillna(False)

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
        # trend_ma5 = df['MA5'] > df['MA5'].shift(1)
        # trend_ma10 = df['MA10'] > df['MA10'].shift(1)
        
        return combined_condition # & trend_ma5 & trend_ma10

class WeeklySequenceStrategy(MultiSequenceStrategy):
    def __init__(self, prev_seq: list, curr_seq: list):
        # WeeklySequenceStrategy was (prev (T-1), curr (T0))
        # MultiSequenceStrategy expects (T0, T-1, ...)
        super().__init__([curr_seq, prev_seq])

class ThreeWeekSequenceStrategy(MultiSequenceStrategy):
    def __init__(self, t2_seq: list, t1_seq: list, t0_seq: list):
        # ThreeWeekSequenceStrategy was (T-2, T-1, T0)
        # MultiSequenceStrategy expects (T0, T-1, T-2)
        super().__init__([t0_seq, t1_seq, t2_seq])

class MultiSequenceRanksStrategy(MultiSequenceStrategy):
    def __init__(self, sequences: list, *ma_ranks: list):
        """
        sequences: List of shortcuts in Old-to-New order (e.g., [T-3, T-2, T-1, T])
        ma_ranks: Variable args of ['MA_NAME', rank_t_minus_3, rank_t_minus_2, rank_t_minus_1, rank_today]
        """
        # MultiSequenceStrategy expects T, T-1, ... (New-to-Old)
        super().__init__(sequences[::-1])
        
        self.rank_specs = []
        for spec in ma_ranks:
            ma_name = spec[0]
            # Reverse ranks to match the shift index (shift 0 = today, shift 1 = T-1, etc.)
            ranks = spec[1:][::-1]
            self.rank_specs.append((ma_name, ranks))

    def calculate_signals(self, df: pd.DataFrame) -> pd.Series:
        # Get signals from base sequence logic (T, T-1, ...)
        combined_condition = super().calculate_signals(df)
        
        # Check rank conditions for each spec (Temporal/Vertical Ranks)
        for ma_name, target_ranks in self.rank_specs:
            num_steps = len(target_ranks)
            if num_steps == 0: continue
            
            # Create a temporary DF with columns representing each shift
            temp_cols = {shift: df[ma_name].shift(shift) for shift in range(num_steps)}
            temp_df = pd.DataFrame(temp_cols, index=df.index)
            
            # Rank values across time steps for this specific MA (Vertical)
            # method='max' ensures larger values get higher ranks
            temporal_ranks = temp_df.rank(axis=1, method='max')
            
            ma_condition = pd.Series(True, index=df.index)
            for shift, target_rank in enumerate(target_ranks):
                if target_rank is not None:
                    ma_condition = ma_condition & (temporal_ranks[shift] == target_rank)
            
            combined_condition = combined_condition & ma_condition.fillna(False)
        
        return combined_condition

# --- Strategy Lists for GUI ---

DAILY_STRATEGIES = [

    '10toMA5=10to10_Daily',
    '10toMA5=10to1-1_Daily',
    '1toMA5=10to2_Daily',
    '1toMA5=10to3_Daily',
    '1toMA5=10to7-7_Daily',
    '2toMA5=20to3_Daily',
    '1-1toMA5=20to10_Daily',
    '2-1toMA5=20to7-9_Daily',
    '3-1toMA5=20to7-5_Daily',
    '7-5toMA5=20to7-5_Daily',
    '7-1toMA5=20to10_Daily',
    '9toMA20=60to10_Daily',
    '3toMA10=20to7-7_Daily',
    'A+1toMA10=20to10_Daily',
    '1toMA10=20to7-8_Daily'
]

WEEKLY_STRATEGIES = [
    '2-2to2-2to2-2to7-3',
    'Ato2-2to7-2to7-2',
    '2-2to2-2to7-3to7-3',
    '7-5to7-5to7-5to3-1',
    '7-7to7-7to7-8to9',
    '3to3to3to7-3',
    'Bto2-3to2-3to7-2',
    '1to2to3to7-8',
    '2to3to3to7-8',
    'A+1toA+1to2-1to2-1',
    '10to10to10to1-1',
    '10to10to9toB',
    'B1toB3toBtoB1',
    'B1toB3toBto1-1',
    '7-5to7-5to7-5to3',
    '1to1to1to2',
    '1to1to1to3',
    '2to2to3to7-3',
    '2to2to7-5to7-5',
    '2-1to2-1to2-1to7-1',
    '2-3to2-3toAtoA',
    '2-3to2-3to2-3to7-1',
    '3-1to3-1to3-1to7-7',
    'Ato2-3to7-9to7-9',
    'AtoAto1to2',
    'Ato2-3to7-1to7-9',
    'A+1toA+1to2-1to7-1',
    'B4to2-3to2-3to7-2',
    '10to10to1-1to10'
]

WEEKLY_STRATEGIES_RANKS = [
    '1to1to2to3',
    '1to1to2to7-3',
    '1to1to2to7-3-v2',
    '1to2to3to7-3',
    '1to2to3to7-3-v2',
    '1to1to3to7-7',
    '1to2to2to3',
    '2to3to3to7-8',
    '2to2to2to3',
    '2to3to3to7-7',
    '2to2to3to7-7',
    '2to2to7-7to7-8',
    '2to3to3to3-1',
    '3to3to3to7-8',
    '1-1to1-1toA+1toA+1'
]




# --- Strategy Factory / Registry ---

STRATEGY_MAP = {
    # Daily

    '10toMA5=10to10_Daily': MultiSequenceStrategy(['10', 'MA5=MA10', '10']), 
    '10toMA5=10to1-1_Daily': MultiSequenceStrategy(['1-1', 'MA5=MA10', '10']), 
    '1toMA5=10to2_Daily': MultiSequenceStrategy(['2', 'MA5=MA10', '1']), 
    '1toMA5=10to3_Daily': MultiSequenceStrategy(['3', 'MA5=MA10', '1']), 
    '1toMA5=10to7-7_Daily': MultiSequenceStrategy(['7-7', 'MA5=MA10', '1']), 
    '2toMA5=20to3_Daily': MultiSequenceStrategy(['3', 'MA5=MA20', '2']), 
    '1-1toMA5=20to10_Daily': MultiSequenceStrategy(['10', 'MA5=MA20', '1-1']),
    '2-1toMA5=20to7-9_Daily': MultiSequenceStrategy(['7-9', 'MA5=MA20', '2-1']),
    '3-1toMA5=20to7-5_Daily': MultiSequenceStrategy(['7-5', 'MA5=MA20', '3-1']),
    '7-5toMA5=20to7-5_Daily': MultiSequenceStrategy(['7-5', 'MA5=MA20', '7-5']),
    '7-1toMA5=20to10_Daily': MultiSequenceStrategy(['10', 'MA5=MA20', '7-1']),
    '9toMA20=60to10_Daily': MultiSequenceStrategy(['10', 'MA20=MA60', '9']),
    '3toMA10=20to7-7_Daily': MultiSequenceStrategy(['7-7', 'MA10=MA20', '3']),
    'A+1toMA10=20to10_Daily': MultiSequenceStrategy(['10', 'MA10=MA20', 'A+1']),
    '1toMA10=20to7-8_Daily': MultiSequenceStrategy(['7-8', 'MA10=MA20', '1']),
    
    # Weekly
    '2-2to2-2to2-2to7-3': MultiSequenceStrategy(['7-3', '2-2', '2-2', '2-2']),
    'Ato2-2to7-2to7-2': MultiSequenceStrategy(['7-2', '7-2', '2-2', 'A']),
    '2-2to2-2to7-3to7-3': MultiSequenceStrategy(['7-3', '7-3', '2-2', '2-2']),
    '7-5to7-5to7-5to3-1': MultiSequenceStrategy(['3-1', '7-5', '7-5', '7-5']),
    '7-7to7-7to7-8to9': MultiSequenceStrategy(['9', '7-8', '7-7', '7-7']),
    '3to3to3to7-3': MultiSequenceStrategy(['7-3', '3', '3', '3']),
    'Bto2-3to2-3to7-2': MultiSequenceStrategy(['7-2', '2-3', '2-3', 'B']),
    '1to2to3to7-8': MultiSequenceStrategy(['7-8', '3', '2', '1']),
    '2to3to3to7-8': MultiSequenceStrategy(['7-8', '3', '3', '2']),
    'A+1toA+1to2-1to2-1': MultiSequenceStrategy(['2-1', '2-1', 'A+1', 'A+1']),
    '10to10to10to1-1': MultiSequenceStrategy(['1-1', '10', '10', '10']),
    '10to10to9toB': MultiSequenceStrategy(['B', '9', '10', '10']),
    'B1toB3toBtoB1': MultiSequenceStrategy(['B1', 'B', 'B3', 'B1']),
    'B1toB3toBto1-1': MultiSequenceStrategy(['1-1', 'B', 'B3', 'B1']),
    '7-5to7-5to7-5to3': MultiSequenceStrategy(['3', '7-5', '7-5', '7-5']),
    '1to1to1to2': MultiSequenceStrategy(['2', '1', '1', '1']),
    '1to1to1to3': MultiSequenceStrategy(['3', '1', '1', '1']),
    '2to2to3to7-3': MultiSequenceStrategy(['7-3', '3', '2', '2']),
    '2to2to7-5to7-5': MultiSequenceStrategy(['7-5', '7-5', '2', '2']),
    '2-1to2-1to2-1to7-1': MultiSequenceStrategy(['7-1', '2-1', '2-1', '2-1']),
    '2-3to2-3toAtoA': MultiSequenceStrategy(['A', 'A', '2-3', '2-3']),
    '2-3to2-3to2-3to7-1': MultiSequenceStrategy(['7-1', '2-3', '2-3', '2-3']),
    '3-1to3-1to3-1to7-7': MultiSequenceStrategy(['7-7', '3-1', '3-1', '3-1']),
    'Ato2-3to7-9to7-9': MultiSequenceStrategy(['7-9', '7-9', '2-3', 'A']),
    'AtoAto1to2': MultiSequenceStrategy(['2', '1', 'A', 'A']),
    'Ato2-3to7-1to7-9': MultiSequenceStrategy(['7-9', '7-1', '2-3', 'A']),
    'A+1toA+1to2-1to7-1': MultiSequenceStrategy(['7-1', '2-1', 'A+1', 'A+1']),
    'B4to2-3to2-3to7-2': MultiSequenceStrategy(['7-2', '2-3', '2-3', 'B4']),
    '10to10to1-1to10': MultiSequenceStrategy(['10', '1-1', '10', '10']),

    # Weekly Ranks
    '1to1to2to3': MultiSequenceRanksStrategy(
        ['1', '1', '2', '3'], 
        ['MA5', 1, 2, 3, 4], 
        ['MA10', 4, 3, 2, 1], 
        ['MA20', 4, 3, 2, 1], 
        ['MA60', 4, 3, 2, 1]
    ),
    '1to1to2to7-3': MultiSequenceRanksStrategy(
        ['1', '1', '2', '7-3'], 
        ['MA5', 1, 2, 3, 4], 
        ['MA10', 4, 3, 2, 1], 
        ['MA20', 4, 3, 2, 1], 
        ['MA60', 4, 3, 2, 1]
    ),
    '1to1to2to7-3-v2': MultiSequenceRanksStrategy(
        ['1', '1', '2', '7-3'], 
        ['MA5', 1, 2, 3, 4], 
        ['MA10', 4, 3, 2, 1], 
        ['MA20', 4, 3, 2, 1], 
        ['MA60', 2, 2, 3, 4]
    ),
    '1to2to3to7-3': MultiSequenceRanksStrategy(
        ['1', '2', '3', '7-3'], 
        ['MA5', 1, 2, 3, 4], 
        ['MA10', 3, 1, 2, 4], 
        ['MA20', 1, 2, 3, 4], 
        ['MA60', 4, 1, 2, 3]
    ),
    '1to2to3to7-3-v2': MultiSequenceRanksStrategy(
        ['1', '2', '3', '7-3'], 
        ['MA5', 1, 2, 3, 4], 
        ['MA10', 1, 2, 3, 4], 
        ['MA20', 1, 2, 3, 4], 
        ['MA60', 1, 2, 3, 4]
    ),
    '1to1to3to7-7': MultiSequenceRanksStrategy(
        ['1', '1', '3', '7-7'], 
        ['MA5', 1, 2, 3, 4], 
        ['MA10', 1, 2, 3, 4], 
        ['MA20', 1, 2, 3, 4], 
        ['MA60', 3, 4, 3, 3]
    ),
    '1to2to2to3': MultiSequenceRanksStrategy(
        ['1', '2', '2', '3'], 
        ['MA5', 1, 2, 3, 4], 
        ['MA10', 3, 2, 1, 4], 
        ['MA20', 3, 2, 3, 4], 
        ['MA60', 4, 1, 2, 3]
    ),
    '2to3to3to7-8': MultiSequenceRanksStrategy(
        ['2', '3', '3', '7-8'], 
        ['MA5', 1, 2, 3, 4], 
        ['MA10', 1, 2, 3, 4], 
        ['MA20', 1, 2, 3, 4], 
        ['MA60', 1, 2, 3, 4]
    ),
    '2to2to2to3': MultiSequenceRanksStrategy(
        ['2', '2', '2', '3'], 
        ['MA5', 1, 2, 3, 4], 
        ['MA10', 2, 1, 3, 4], 
        ['MA20', 4, 2, 1, 3], 
        ['MA60', 2, 1, 3, 4]
    ),
    '2to3to3to7-7': MultiSequenceRanksStrategy(
        ['2', '3', '3', '7-7'], 
        ['MA5', 1, 2, 3, 4], 
        ['MA10', 1, 2, 3, 4], 
        ['MA20', 1, 2, 3, 4], 
        ['MA60', 4, 3, 2, 1]
    ),
    '2to2to3to7-7': MultiSequenceRanksStrategy(
        ['2', '2', '3', '7-7'], 
        ['MA5', 1, 2, 3, 4], 
        ['MA10', 1, 2, 3, 4], 
        ['MA20', 4, 2, 1, 3], 
        ['MA60', 4, 3, 2, 1]
    ),
    '2to2to7-7to7-8': MultiSequenceRanksStrategy(
        ['2', '2', '7-7', '7-8'], 
        ['MA5', 2, 1, 3, 4], 
        ['MA10', 1, 2, 3, 4], 
        ['MA20', 3, 2, 3, 4], 
        ['MA60', 3, 1, 2, 4]
    ),
    '2to3to3to3-1': MultiSequenceRanksStrategy(
        ['2', '3', '3', '3-1'], 
        ['MA5', 2, 4, 3, 1], 
        ['MA10', 1, 2, 3, 4], 
        ['MA20', 4, 3, 2, 1], 
        ['MA60', 4, 4, 3, 2]
    ),
    '3to3to3to7-8': MultiSequenceRanksStrategy(
        ['3', '3', '3', '7-8'], 
        ['MA5', 1, 2, 3, 4], 
        ['MA10', 1, 2, 3, 4], 
        ['MA20', 1, 2, 3, 4], 
        ['MA60', 2, 2, 3, 4]
    ),
    '1-1to1-1toA+1toA+1': MultiSequenceRanksStrategy(
        ['1-1', '1-1', 'A+1', 'A+1'], 
        ['MA5', 4, 3, 1, 2], 
        ['MA10', 4, 3, 2, 1], 
        ['MA20', 1, 2, 3, 4], 
        ['MA60', 4, 4, 3, 4]
    ),
}





def get_strategy(name: str) -> BaseStrategy:
    return STRATEGY_MAP.get(name)
