import pandas as pd
from abc import ABC, abstractmethod
import re
from .shortcuts import REVERSE_SHORTCUTS

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

# --- Weekly & Daily Sequence Strategies ---

class MultiSequenceStrategy(BaseStrategy):
    def __init__(self, sequences: list):
        """
        sequences: A list of sequences. Each element can be a list of integers 
                   or a shortcut string from SEQUENCE_SHORTCUTS.
                   sequences[0] is T, sequences[1] is T-1, etc.
        """
        # Note: We resolve the shortcuts during evaluation or init.
        # Since we want to resolve SEQUENCE_SHORTCUTS, we can import it locally or from .shortcuts.
        from .shortcuts import SEQUENCE_SHORTCUTS
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
            if not seq:
                return pd.Series(True, index=df.index)
            # 快取判定邏輯 (以位階代號快取直接比對)
            if 'Sequence' in df.columns:
                try:
                    # e.g., ['MA60', 'MA20', 'MA10', 'MA5'] -> (60, 20, 10, 5)
                    periods = tuple(int(x[2:]) for x in seq)
                    shortcut_code = REVERSE_SHORTCUTS.get(periods)
                    if shortcut_code:
                        return (df['Sequence'].shift(shift) == shortcut_code).fillna(False)
                except Exception:
                    pass

            # Fallback 比較邏輯
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
        
        return combined_condition

class WeeklySequenceStrategy(MultiSequenceStrategy):
    def __init__(self, prev_seq: list, curr_seq: list):
        super().__init__([curr_seq, prev_seq])

class ThreeWeekSequenceStrategy(MultiSequenceStrategy):
    def __init__(self, t2_seq: list, t1_seq: list, t0_seq: list):
        super().__init__([t0_seq, t1_seq, t2_seq])

class MultiSequenceRanksStrategy(MultiSequenceStrategy):
    def __init__(self, sequences: list, *ma_ranks: list):
        super().__init__(sequences[::-1])
        
        self.rank_specs = []
        for spec in ma_ranks:
            ma_name = spec[0]
            ranks = spec[1:][::-1]
            self.rank_specs.append((ma_name, ranks))

    def calculate_signals(self, df: pd.DataFrame) -> pd.Series:
        combined_condition = super().calculate_signals(df)
        
        for ma_name, target_ranks in self.rank_specs:
            num_steps = len(target_ranks)
            if num_steps == 0: continue
            
            temp_cols = {shift: df[ma_name].shift(shift) for shift in range(num_steps)}
            temp_df = pd.DataFrame(temp_cols, index=df.index)
            
            temporal_ranks = temp_df.rank(axis=1, method='max')
            
            ma_condition = pd.Series(True, index=df.index)
            for shift, target_rank in enumerate(target_ranks):
                if target_rank is not None:
                    ma_condition = ma_condition & (temporal_ranks[shift] == target_rank)
            
            combined_condition = combined_condition & ma_condition.fillna(False)
        
        return combined_condition

class MultiSequenceRanksStrategy_by5M_1(MultiSequenceRanksStrategy):
    def __init__(self, sequences: list, *ma_ranks: list):
        super().__init__(sequences, *ma_ranks)

    def calculate_signals(self, df: pd.DataFrame) -> pd.Series:
        combined_condition = super().calculate_signals(df)
        
        ma5 = df['MA5']
        
        # 連續3日五日均線遞減 (T-1 < T-2, T-2 < T-3, T-3 < T-4)
        dec_1 = ma5.shift(1) < ma5.shift(2)
        dec_2 = ma5.shift(2) < ma5.shift(3)
        dec_3 = ma5.shift(3) < ma5.shift(4)
        
        # 當日五均線不再遞減 (T >= T-1)
        not_dec = ma5 >= ma5.shift(1)
        
        ma5_condition = dec_1 & dec_2 & dec_3 & not_dec
        
        return combined_condition & ma5_condition.fillna(False)
