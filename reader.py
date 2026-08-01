#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TWSE 日線資料抓取器（含快取 / 交易日行事曆 / 重試 / 批次入庫 / CLI）
"""

import os
import sys
import re
import time
import json
import math
import sqlite3
import argparse
import logging
from logging.handlers import RotatingFileHandler
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional, Iterable, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------
# 路徑與常數
# ---------------------------

# ---------------------------
# 路徑與常數
# ---------------------------

def get_base_dir():
    if getattr(sys, 'frozen', False):
        return Path(sys.executable).resolve().parent
    else:
        return Path(__file__).resolve().parent

BASE_DIR = get_base_dir()
DEFAULT_DATA_DIR = BASE_DIR / "data"
DEFAULT_DB_PATH = DEFAULT_DATA_DIR / "twse_data.db"
CALENDAR_CACHE_TEMPLATE = "calendar_{year}.json"
TWSE_MI_INDEX = "https://www.twse.com.tw/exchangeReport/MI_INDEX"
TWSE_CALENDAR_HTML = "https://www.twse.com.tw/holidaySchedule/holidaySchedule?queryYear={roc}&response=html"

# (B) 快取欄位固定清單
CACHE_COLUMNS = ["日期", "代號", "名稱", "開盤", "最高", "最低", "收盤", "成交金額", "資料來源", "下載時間"]

# ---------------------------
# Logging
# ---------------------------

def setup_logging(log_level: str, data_dir: Path) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    level = getattr(logging, log_level.upper(), logging.INFO)

    logger = logging.getLogger()
    logger.setLevel(level)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    fh = RotatingFileHandler(data_dir / "reader.log", maxBytes=2_000_000, backupCount=3, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

# ---------------------------
# Requests Session with Retry
# ---------------------------

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def make_session(max_retries: int = 3, backoff: float = 0.5, timeout: int = 12, verify: bool = True) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=max_retries,
        read=max_retries,
        connect=max_retries,
        status=max_retries,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update({"User-Agent": "Mozilla/5.0"})
    s.request = _with_timeout_and_verify(s.request, timeout, verify)  # inject default timeout & verify
    return s

def _with_timeout_and_verify(request_func, timeout: int, verify: bool):
    def wrapper(method, url, **kwargs):
        if "timeout" not in kwargs:
            kwargs["timeout"] = timeout
        if "verify" not in kwargs:
            kwargs["verify"] = verify
        return request_func(method, url, **kwargs)
    return wrapper

# ---------------------------
# DB
# ---------------------------

def init_db(db_path: Path) -> None:
    logging.info("初始化 SQLite：%s", db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_prices (
                日期 TEXT,
                代號 TEXT,
                名稱 TEXT,
                開盤 REAL,
                最高 REAL,
                最低 REAL,
                收盤 REAL,
                成交金額 INTEGER CHECK(成交金額 >= 0),
                資料來源 TEXT,
                下載時間 TEXT,
                PRIMARY KEY (日期, 代號)
            )
            """
        )
        # (A) 索引＋PRAGMA 提升批次效能
        info = {row[1]: (row[2] or "").upper() for row in cur.execute("PRAGMA table_info(stock_prices)")}
        if info.get("成交金額") and info["成交金額"] not in {"INTEGER", "INT"}:
            logging.warning("資料表 stock_prices 的「成交金額」欄位型別為 %s，建議調整為 INTEGER。", info["成交金額"])
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_stock_prices_code_date
            ON stock_prices(代號, 日期)
        """)
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")
        conn.commit()

def bulk_upsert(db_path: Path, rows: Iterable[Tuple]) -> int:
    rows = list(rows)
    if not rows:
        return 0
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.executemany(
            """
            INSERT INTO stock_prices
            (日期, 代號, 名稱, 開盤, 最高, 最低, 收盤, 成交金額, 資料來源, 下載時間)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(日期, 代號) DO UPDATE SET
                名稱=excluded.名稱,
                開盤=excluded.開盤,
                最高=excluded.最高,
                最低=excluded.最低,
                收盤=excluded.收盤,
                成交金額=excluded.成交金額,
                資料來源=excluded.資料來源,
                下載時間=excluded.下載時間
            """,
            rows,
        )
        conn.commit()
        return cur.rowcount

# ---------------------------
# 交易日行事曆
# ---------------------------

def roc_year(dt: date) -> int:
    return dt.year - 1911

def _calendar_cache_path(data_dir: Path, year: int) -> Path:
    return Path(data_dir) / CALENDAR_CACHE_TEMPLATE.format(year=year)


def _load_calendar_cache(data_dir: Path, year: int) -> Optional[Tuple[set, set]]:
    cache_path = _calendar_cache_path(data_dir, year)
    if not cache_path.exists():
        return None
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
        holidays = {date.fromisoformat(d) for d in payload.get("holidays", [])}
        makeups = {date.fromisoformat(d) for d in payload.get("makeups", [])}
        logging.info("使用快取行事曆：%s 年", year)
        return holidays, makeups
    except Exception as exc:
        logging.warning("行事曆快取檔讀取失敗：%s", exc)
        return None


def _store_calendar_cache(data_dir: Path, year: int, holidays: set, makeups: set) -> None:
    cache_path = _calendar_cache_path(data_dir, year)
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(
            json.dumps(
                {
                    "year": year,
                    "holidays": sorted(d.isoformat() for d in holidays),
                    "makeups": sorted(d.isoformat() for d in makeups),
                    "cached_at": datetime.utcnow().isoformat() + "Z",
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    except Exception as exc:
        logging.warning("無法寫入 %s 年行事曆快取檔：%s", year, exc)


def try_fetch_holidays_and_makeups(session: requests.Session, year: int, data_dir: Path, refresh: bool=False) -> Tuple[set, set]:
    """
    從 TWSE 開休市頁面解析：
    - holidays: 放假日（市場休市）
    - makeups: 調整上班日（可能為六/日轉上班日）
    若解析失敗，回傳空集合。
    """
    if not refresh:
        cached = _load_calendar_cache(data_dir, year)
        if cached is not None:
            return cached
    url = TWSE_CALENDAR_HTML.format(roc=roc_year(date(year, 1, 1)))
    logging.info("嘗試取得 %s 之開休市資訊：%s", year, url)
    try:
        r = session.get(url)
        r.raise_for_status()
        tables = pd.read_html(r.text)
    except Exception as e:
        logging.warning("解析開休市頁面失敗：%s", e)
        return set(), set()

    holidays, makeups = set(), set()
    date_pattern = re.compile(r"\d{3,4}/\d{1,2}/\d{1,2}")  # 114/1/1 或 2025/1/1

    def _to_gregorian(dstr: str) -> Optional[date]:
        dstr = str(dstr).strip()
        m = date_pattern.search(dstr)
        if not m:
            return None
        token = m.group(0)
        parts = token.split("/")
        if len(parts[0]) <= 3:  # ROC 年
            y = int(parts[0]) + 1911
        else:
            y = int(parts[0])
        try:
            return date(y, int(parts[1]), int(parts[2]))
        except ValueError:
            return None

    for df in tables:
        cols = "".join(map(str, df.columns))
        if not any(k in cols for k in ("日", "期")):
            continue
        for _, row in df.iterrows():
            row_text = " ".join(map(lambda x: str(x), row.values))
            d = _to_gregorian(row_text)
            if not d or d.year != year:
                continue
            txt = row_text
            if any(k in txt for k in ("休市", "放假", "停止交易", "補假", "中秋", "春節", "國慶", "連假", "除夕")):
                holidays.add(d)
            if any(k in txt for k in ("補行上班", "調整上班", "補班")):
                makeups.add(d)

    logging.info("解析到 %d 個休市日、%d 個補班日（%s）", len(holidays), len(makeups), year)
    _store_calendar_cache(data_dir, year, holidays, makeups)
    return holidays, makeups

def build_trading_days(session: requests.Session, start: date, end: date, data_dir: Path, refresh_calendar: bool=False) -> List[date]:
    """
    優先使用官方開休市頁面推導交易日：
      交易日 = 所有平日(一~五) - 休市日 + 補班日(如落在週末)
    若失敗，退回保守模式：平日(一~五)，且後續以 API 有資料為準。
    """
    # (A) 修正：涵蓋所有跨年的年份
    years = list(range(start.year, end.year + 1))
    holidays_all, makeups_all = set(), set()
    for y in years:
        h, m = try_fetch_holidays_and_makeups(session, y, data_dir, refresh=refresh_calendar)
        holidays_all |= h
        makeups_all |= m

    days: List[date] = []
    cur = start
    while cur <= end:
        is_weekday = 1 <= cur.isoweekday() <= 5
        is_makeup = cur in makeups_all  # 週末補班
        if (is_weekday and cur not in holidays_all) or is_makeup:
            days.append(cur)
        cur += timedelta(days=1)

    if not days:
        logging.warning("行事曆解析為空，改用平日(一~五)保守模式。")
        cur = start
        days = []
        while cur <= end:
            if 1 <= cur.isoweekday() <= 5:
                days.append(cur)
            cur += timedelta(days=1)

    logging.info("交易日範圍：%s ~ %s，共 %d 天（含補班日）", start, end, len(days))
    return days

# ---------------------------
# 下載 + 快取
# ---------------------------

def cache_path_for(day: date, data_dir: Path, out_format: str) -> Path:
    ext = ".csv.gz" if out_format == "csv.gz" else ".csv"
    return data_dir / f"ohlcv_{day.strftime('%Y%m%d')}{ext}"


def cache_candidates_for(day: date, data_dir: Path, preferred_format: str) -> List[Path]:
    primary = cache_path_for(day, data_dir, preferred_format)
    alt_format = "csv" if preferred_format == "csv.gz" else "csv.gz"
    candidates = [primary]
    alt_path = cache_path_for(day, data_dir, alt_format)
    if alt_path != primary:
        candidates.append(alt_path)
    return candidates

def fetch_one_day(session: requests.Session, day: date, data_dir: Path, force: bool=False,
                  cache_format: str="csv", from_cache_only: bool=False) -> Optional[pd.DataFrame]:
    """
    下載單日 TWSE ALL 報表，儲存為 CSV 快取並回傳清理後的 DataFrame。
    回傳 None 表示該日沒有可用資料或失敗。
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    if from_cache_only:
        force = False
    candidates = cache_candidates_for(day, data_dir, cache_format)
    target_path = candidates[0]

    cache_hit_path: Optional[Path] = None
    if not force:
        for candidate in candidates:
            if candidate.exists():
                cache_hit_path = candidate
                break

    # (B) 讀快取時固定欄位順序，缺欄則重抓
    if cache_hit_path is not None:
        logging.info("快取命中：%s", cache_hit_path.resolve())
        df = pd.read_csv(cache_hit_path)
        missing = [c for c in CACHE_COLUMNS if c not in df.columns]
        if not missing:
            df = df[CACHE_COLUMNS]
            if cache_hit_path != target_path and not target_path.exists():
                try:
                    compression = "gzip" if target_path.suffix == ".gz" else None
                    df.to_csv(target_path, index=False, encoding="utf-8-sig", compression=compression)
                    logging.info("已同步建立快取：%s", target_path.resolve())
                except Exception as exc:
                    logging.warning("同步快取失敗：%s", exc)
            return df
        logging.warning("快取欄位缺失：%s；忽略快取改為重抓。", missing)

    if from_cache_only:
        logging.info("from-cache-only 啟用，未找到快取，略過下載：%s", day)
        return None

    params = {"response": "json", "date": day.strftime("%Y%m%d"), "type": "ALL"}
    logging.info("下載 %s ...", day.isoformat())
    try:
        r = session.get(TWSE_MI_INDEX, params=params)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        logging.warning("下載失敗 %s：%s", day, e)
        return None

    if data.get("stat") != "OK":
        logging.info("該日無交易資料或尚未開盤：%s", day)
        return None

    df = None
    for table in data.get("tables", []):
        fields = table.get("fields") or []
        rows = table.get("data") or []
        if fields[:2] == ["證券代號", "證券名稱"]:
            df = pd.DataFrame(rows, columns=fields)
            break

    if df is None or df.empty:
        logging.warning("未找到證券表格：%s；keys=%s", day, list(data.keys()))
        return None

    rename_map = {
        "證券代號": "代號",
        "證券名稱": "名稱",
        "開盤價": "開盤",
        "最高價": "最高",
        "最低價": "最低",
        "收盤價": "收盤",
        "成交金額": "成交金額",  # 單位常為千元
    }
    df = df.rename(columns=rename_map, errors="ignore")

    # 僅保留四碼股票（排除權證/可轉債等），可依需求調整
    df = df[df["代號"].astype(str).str.match(r"^[1-9]\d{3}$", na=False)]

    for col in ["開盤", "最高", "最低", "收盤", "成交金額"]:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .replace(["--", "", "nan", "None"], pd.NA)
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["日期"] = day.strftime("%Y-%m-%d")
    df["資料來源"] = "TWSE"
    df["下載時間"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df = df[CACHE_COLUMNS]
    df = df.dropna(subset=["收盤"])

    # 快取
    compression = "gzip" if target_path.suffix == ".gz" else None
    df.to_csv(target_path, index=False, encoding="utf-8-sig", compression=compression)
    logging.info("已快取：%s（%d 筆）", target_path.resolve(), len(df))
    return df

# ---------------------------
# 主流程
# ---------------------------

def daterange_by_args(args) -> Tuple[date, date, int]:
    """根據 --from/--to/--days 推導日期範圍與目標「交易日數」"""
    tz_today = date.today()
    if args.date_from and args.date_to:
        start = datetime.strptime(args.date_from, "%Y-%m-%d").date()
        end = datetime.strptime(args.date_to, "%Y-%m-%d").date()
        n_days = None
    elif args.days:
        end = tz_today
        start = end - timedelta(days=max(args.days * 2, args.days + 30))  # 給寬一點的原始範圍，之後再以交易日裁切
        n_days = args.days
    else:
        # 預設抓最近 60 個交易日
        end = tz_today
        start = end - timedelta(days=180)
        n_days = 60
    return start, end, n_days or 0

def as_rows(df: pd.DataFrame) -> Iterable[Tuple]:
    for r in df.itertuples(index=False):
        volume = getattr(r, "成交金額")
        volume_value = None if pd.isna(volume) else int(float(volume))
        yield (
            r.日期, r.代號, r.名稱, r.開盤, r.最高, r.最低, r.收盤, volume_value, r.資料來源, r.下載時間
        )

def run(args) -> None:
    setup_logging(args.log_level, Path(args.data_dir))
    session = make_session(max_retries=args.max_retries, backoff=0.6, timeout=12, verify=not args.no_verify)

    data_dir = Path(args.data_dir)
    db_path = Path(args.db_path) if args.db_path else DEFAULT_DB_PATH
    init_db(db_path)

    start, end, target_trade_days = daterange_by_args(args)
    all_days = build_trading_days(session, start, end, data_dir, refresh_calendar=args.refresh_calendar)

    # 若指定 --days，只取最後 N 個交易日
    if target_trade_days:
        all_days = all_days[-target_trade_days:]
        logging.info("取最後 %d 個交易日：%s ~ %s", target_trade_days, all_days[0], all_days[-1])

    total_inserted = 0
    collected_rows: List[Tuple] = []
    consecutive_failures = 0
    halt_on_fail = max(args.halt_on_fail, 0)

    # (C) 中斷保護：確保殘餘 flush
    try:
        for i, d in enumerate(all_days, 1):
            df = fetch_one_day(
                session,
                d,
                data_dir,
                force=args.force,
                cache_format=args.out_format,
                from_cache_only=args.from_cache_only,
            )
            should_break = False
            if df is not None and not df.empty:
                collected_rows.extend(as_rows(df))
                logging.info("進度：%d/%d 交易日；目前累積 %d 筆", i, len(all_days), len(collected_rows))
                consecutive_failures = 0
            else:
                consecutive_failures += 1
                logging.warning("交易日 %s 資料取得失敗（連續 %d 次）", d, consecutive_failures)
                if halt_on_fail and consecutive_failures >= halt_on_fail:
                    logging.error("連續失敗達 %d 次，提早停止。", consecutive_failures)
                    should_break = True
            if should_break:
                break
            time.sleep(args.sleep)

            # 以批量大小寫入，避免記憶體暴衝
            if len(collected_rows) >= args.batch_size:
                inserted = bulk_upsert(db_path, collected_rows)
                total_inserted += inserted
                logging.info("批次入庫 %d 筆（總計 %d）", inserted, total_inserted)
                collected_rows.clear()
    finally:
        if collected_rows:
            inserted = bulk_upsert(db_path, collected_rows)
            total_inserted += inserted
            logging.info("收尾入庫 %d 筆（總計 %d）", inserted, total_inserted)

    logging.info("完成。DB 路徑：%s；資料夾：%s", db_path.resolve(), data_dir.resolve())
    try:
        print(f"🎉 全部完成，總共處理 {total_inserted} 筆資料")
    except UnicodeEncodeError:
        print(f"全部完成，總共處理 {total_inserted} 筆資料")
    print(f"DB：{db_path.resolve()}")
    print(f"快取資料夾：{data_dir.resolve()}")

# ---------------------------
# CLI
# ---------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="TWSE 日線抓取器")
    grp_range = p.add_mutually_exclusive_group()
    grp_range.add_argument("--days", type=int, help="抓最近 N 個『交易日』")
    p.add_argument("--from", dest="date_from", help="起始日期 YYYY-MM-DD（與 --to 搭配）")
    p.add_argument("--to", dest="date_to", help="結束日期 YYYY-MM-DD（與 --from 搭配）")

    p.add_argument("--sleep", type=float, default=0.2, help="每日下載間隔秒數（避免過快）")
    p.add_argument("--max-retries", type=int, default=3, help="HTTP 下載最大重試次數")
    p.add_argument("--batch-size", type=int, default=5000, help="DB 批次寫入筆數")
    p.add_argument("--force", action="store_true", help="無視快取，強制重抓並覆寫 CSV")
    p.add_argument("--log-level", default="INFO", help="DEBUG / INFO / WARNING / ERROR")
    p.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR), help="資料快取資料夾")
    p.add_argument("--out-format", choices=["csv", "csv.gz"], default="csv", help="快取輸出格式（csv 或 csv.gz）")
    p.add_argument("--from-cache-only", action="store_true", help="僅使用既有快取，不執行網路下載")
    p.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="SQLite 路徑")
    p.add_argument("--refresh-calendar", action="store_true", help="忽略快取，強制重新抓取交易日行事曆")
    p.add_argument("--halt-on-fail", type=int, default=20, help="連續抓取失敗達指定次數後提前停止（0 表示不停）")
    p.add_argument("--no-verify", action="store_true", help="停用 SSL 憑證驗證 (慎用)")
    return p.parse_args(argv)

if __name__ == "__main__":
    args = parse_args()
    run(args)
