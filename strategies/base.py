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

class MultiSequenceChangeStrategy(MultiSequenceStrategy):
    def __init__(self, sequences: list, changes: list):
        # sequences: e.g. ['7-5', '7-5', '1']
        # changes: e.g. ['小', '小', '小', '小'] corresponding to [MA5, MA10, MA20, MA60] today vs yesterday
        super().__init__(sequences[::-1])
        self.changes = changes

    def calculate_signals(self, df: pd.DataFrame) -> pd.Series:
        # 1. Base sequence conditions
        combined_condition = super().calculate_signals(df)
        
        # 2. Check MA change conditions (today vs yesterday)
        ma_names = ['MA5', 'MA10', 'MA20', 'MA60']
        for ma_name, change in zip(ma_names, self.changes):
            if change == '大':
                cond = df[ma_name] > df[ma_name].shift(1)
            elif change == '小':
                cond = df[ma_name] < df[ma_name].shift(1)
            elif change == '同':
                cond = (df[ma_name].round(2) == df[ma_name].shift(1).round(2))
            else:
                continue
            combined_condition = combined_condition & cond.fillna(False)
            
        return combined_condition


class WeeklyKBarStrategy(MultiSequenceChangeStrategy):
    """
    週K(當日K) 策略：
    1. 包含均線位階序列變化 (如 1,2,3 代表 1to2to3)
    2. 包含各週均線 (MA5, MA10, MA20, MA60) 與前一週比較之變化 (如 大大大同)
    3. 包含週K棒開盤價(開)、收盤價(收)與各週均線(5, 10, 20, 60)之位置關係 (如 60>收>5>開>20>10)
    """
    TOKEN_MAP = {
        '5': 'MA5', 'MA5': 'MA5',
        '10': 'MA10', 'MA10': 'MA10',
        '20': 'MA20', 'MA20': 'MA20',
        '60': 'MA60', 'MA60': 'MA60',
        '開': '開盤', '開盤': '開盤',
        '收': '收盤', '收盤': '收盤',
        '高': '最高', '最高': '最高',
        '低': '最低', '最低': '最低',
    }

    def __init__(self, sequences: list, changes: list, kbar_expr: str):
        super().__init__(sequences, changes)
        self.kbar_expr = kbar_expr.strip()

    def _get_series(self, df: pd.DataFrame, token: str) -> pd.Series:
        col = self.TOKEN_MAP.get(token.strip())
        if not col or col not in df.columns:
            raise ValueError(f"未知的欄位代號: {token}")
        return df[col]

    def calculate_signals(self, df: pd.DataFrame) -> pd.Series:
        combined_condition = super().calculate_signals(df)

        if not self.kbar_expr:
            return combined_condition

        # 支援 '>'、'>='、'<'、'<='、'='、'==' 以及 ',' (預設視為 '>')
        parts = re.split(r'(>=|<=|==|>|<|=|,)', self.kbar_expr)
        parts = [p.strip() for p in parts if p.strip()]

        # parts 結構應為: [token0, op0, token1, op1, token2, ...]
        if len(parts) < 3 or len(parts) % 2 == 0:
            return pd.Series(False, index=df.index)

        try:
            for i in range(0, len(parts) - 2, 2):
                left_token = parts[i]
                op = parts[i + 1]
                right_token = parts[i + 2]

                left_s = self._get_series(df, left_token)
                right_s = self._get_series(df, right_token)

                if op in ('>', ','):
                    cond = left_s > right_s
                elif op == '>=':
                    cond = left_s >= right_s
                elif op == '<':
                    cond = left_s < right_s
                elif op == '<=':
                    cond = left_s <= right_s
                elif op in ('=', '=='):
                    cond = left_s.round(2) == right_s.round(2)
                else:
                    cond = pd.Series(False, index=df.index)

                combined_condition = combined_condition & cond.fillna(False)
        except Exception:
            return pd.Series(False, index=df.index)

        return combined_condition


def parse_weekly_kbar_strategy(name: str):
    """
    自動解析「週K(當日K)」策略名稱，例如：
    - '1,2,3_大大大同_60>收>5>開>20>10'
    - '1,2,3_大_大_大_同_60>收>5>開>20>10'
    - '1to2to3_大大大同_60,收,5,開,20,10'
    """
    parts = [p.strip() for p in name.split('_') if p.strip()]
    if len(parts) < 3:
        return None

    seq_part = parts[0]
    kbar_expr = parts[-1]
    mid_parts = parts[1:-1]

    # 1. 解析序列 (支援 ',' 或 'to')
    if ',' in seq_part:
        sequences = [s.strip() for s in seq_part.split(',') if s.strip()]
    else:
        sequences = [s.strip() for s in seq_part.split('to') if s.strip()]

    # 2. 解析均線變化 (支援 '大大大同' 或 '大_大_大_同' 或 '大,大,大,同')
    if len(mid_parts) == 1:
        chg_str = mid_parts[0]
        if ',' in chg_str:
            changes = [c.strip() for c in chg_str.split(',') if c.strip()]
        else:
            changes = list(chg_str)
    else:
        changes = mid_parts

    if len(changes) != 4 or not sequences or not kbar_expr:
        return None

    return WeeklyKBarStrategy(sequences, changes, kbar_expr)
