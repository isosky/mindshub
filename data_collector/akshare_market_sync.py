#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse
import json
import math
import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
import time as time_lib
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import mysql.connector
import pandas as pd

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from base.config import dbconfig, investdbconfig  # noqa: E402


ADJUST_TYPE = "qfq"
LOOKBACK_DAYS = 120
WATCHLIST_INITIAL_DAYS = 365
FETCH_RETRY_TIMES = 3


@dataclass
class SymbolWindow:
    symbol_code: str
    symbol_type: str
    start_date: date
    end_date: date


def connect_summary_db(dictionary: bool = False):
    conn = mysql.connector.connect(**dbconfig)
    cursor = conn.cursor(dictionary=dictionary)
    return conn, cursor


def connect_invest_db(dictionary: bool = False):
    conn = mysql.connector.connect(**investdbconfig)
    cursor = conn.cursor(dictionary=dictionary)
    return conn, cursor


def _parse_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    if isinstance(value, date):
        return value
    value = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"无法解析日期: {value}")


def _month_shift(value: date, months: int) -> date:
    year = value.year + (value.month - 1 + months) // 12
    month = (value.month - 1 + months) % 12 + 1
    # 统一回退到每月最后一天，避免 31 号跨月越界
    if month in (1, 3, 5, 7, 8, 10, 12):
        max_day = 31
    elif month == 2:
        leap = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
        max_day = 29 if leap else 28
    else:
        max_day = 30
    day = min(value.day, max_day)
    return date(year, month, day)


def _is_intraday_trading_time(now_value: datetime) -> bool:
    if now_value.weekday() >= 5:
        return False
    now_time = now_value.time()
    morning_start = time(9, 30)
    morning_end = time(11, 30)
    afternoon_start = time(13, 0)
    afternoon_end = time(15, 0)
    in_morning = morning_start <= now_time <= morning_end
    in_afternoon = afternoon_start <= now_time <= afternoon_end
    return in_morning or in_afternoon


def _normalize_symbol(symbol_code: str, symbol_type: Optional[str] = None) -> Tuple[str, str, str]:
    """
    返回:
    - unified_code: 统一代码（000001.SZ）
    - symbol_type: stock/etf/index
    - exchange_prefixed: 交易所前缀代码（sz000001）
    """
    raw = (symbol_code or "").strip().upper()
    if not raw:
        raise ValueError("symbol_code 不能为空")

    exchange = None
    digits = None

    if len(raw) == 8 and (raw.startswith("SH") or raw.startswith("SZ")) and raw[2:].isdigit():
        exchange = raw[:2]
        digits = raw[2:]
    elif len(raw) == 9 and raw[6:] in (".SH", ".SZ") and raw[:6].isdigit():
        digits = raw[:6]
        exchange = raw[7:]
    elif len(raw) == 6 and raw.isdigit():
        digits = raw
    else:
        raise ValueError(f"不支持的代码格式: {symbol_code}")

    symbol_type_norm = (symbol_type or "").strip().lower()
    if symbol_type_norm not in ("stock", "etf", "index"):
        if digits.startswith(("5", "1")):
            symbol_type_norm = "etf"
        elif digits.startswith(("0", "3", "6", "8", "4")):
            symbol_type_norm = "stock"
        else:
            symbol_type_norm = "index"

    if not exchange:
        if symbol_type_norm == "index":
            # 常用宽基默认上交所，避免无交易所导致接口异常
            exchange = "SH"
        else:
            if digits.startswith(("6", "5", "9")):
                exchange = "SH"
            else:
                exchange = "SZ"

    unified_code = f"{digits}.{exchange}"
    exchange_prefixed = f"{exchange.lower()}{digits}"
    return unified_code, symbol_type_norm, exchange_prefixed


def _pick_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _calc_rsi(close_series: pd.Series, period: int) -> pd.Series:
    delta = close_series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False,
                        min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False,
                        min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + rs))
    return rsi


def _calc_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    close = df["close_price"]
    df["ma5"] = close.rolling(5, min_periods=5).mean()
    df["ma10"] = close.rolling(10, min_periods=10).mean()
    df["ma20"] = close.rolling(20, min_periods=20).mean()
    df["ma60"] = close.rolling(60, min_periods=60).mean()

    df["rsi6"] = _calc_rsi(close, 6)
    df["rsi14"] = _calc_rsi(close, 14)

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df["dif"] = ema12 - ema26
    df["dea"] = df["dif"].ewm(span=9, adjust=False).mean()
    df["macd_hist"] = (df["dif"] - df["dea"]) * 2
    return df


def _fetch_stock_df(symbol_digits: str, start_date: date, end_date: date) -> pd.DataFrame:
    import akshare as ak

    return ak.stock_zh_a_hist(
        symbol=symbol_digits,
        period="daily",
        start_date=start_date.strftime("%Y%m%d"),
        end_date=end_date.strftime("%Y%m%d"),
        adjust=ADJUST_TYPE,
    )


def _fetch_stock_df_alt(exchange_prefixed: str, start_date: date, end_date: date) -> pd.DataFrame:
    import akshare as ak

    df = ak.stock_zh_a_daily(symbol=exchange_prefixed, adjust=ADJUST_TYPE)
    return _slice_df_by_date(df, start_date, end_date)


def _fetch_etf_df(symbol_digits: str, start_date: date, end_date: date) -> pd.DataFrame:
    import akshare as ak

    return ak.fund_etf_hist_em(
        symbol=symbol_digits,
        period="daily",
        start_date=start_date.strftime("%Y%m%d"),
        end_date=end_date.strftime("%Y%m%d"),
        adjust=ADJUST_TYPE,
    )


def _fetch_etf_df_alt(exchange_prefixed: str, start_date: date, end_date: date) -> pd.DataFrame:
    import akshare as ak

    df = ak.fund_etf_hist_sina(symbol=exchange_prefixed)
    return _slice_df_by_date(df, start_date, end_date)


def _fetch_index_df(exchange_prefixed: str, start_date: date, end_date: date) -> pd.DataFrame:
    import akshare as ak

    # 指数接口在不同版本 AKShare 返回字段存在差异，这里先拉全量后按日期过滤
    df = ak.stock_zh_index_daily_em(symbol=exchange_prefixed)
    if df is None or df.empty:
        return pd.DataFrame()

    date_col = _pick_column(df, ["date", "日期"])
    if not date_col:
        return pd.DataFrame()

    temp = df.copy()
    temp[date_col] = pd.to_datetime(temp[date_col], errors="coerce").dt.date
    temp = temp[(temp[date_col] >= start_date) & (temp[date_col] <= end_date)]
    return temp


def _fetch_index_df_alt(exchange_prefixed: str, start_date: date, end_date: date) -> pd.DataFrame:
    import akshare as ak

    df = ak.stock_zh_index_daily(symbol=exchange_prefixed)
    return _slice_df_by_date(df, start_date, end_date)


def _retry_fetch(fetch_func, *args, **kwargs) -> pd.DataFrame:
    last_error = None
    for attempt in range(1, FETCH_RETRY_TIMES + 1):
        try:
            return fetch_func(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt >= FETCH_RETRY_TIMES:
                break
            time_lib.sleep(attempt)
    raise last_error


def _slice_df_by_date(raw_df: pd.DataFrame, start_date: date, end_date: date) -> pd.DataFrame:
    if raw_df is None or raw_df.empty:
        return pd.DataFrame()

    df = raw_df.copy()
    date_col = _pick_column(df, ["date", "日期", "交易日期"])

    if not date_col:
        df = df.reset_index()
        date_col = _pick_column(df, ["date", "日期", "交易日期", "index"])
        if not date_col:
            date_col = df.columns[0]

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.date
    df = df[(df[date_col] >= start_date) & (df[date_col] <= end_date)]
    return df


def _fetch_with_fallback(primary_fetch, fallback_fetch, primary_name: str, fallback_name: str) -> Tuple[pd.DataFrame, str]:
    primary_error = None
    try:
        return _retry_fetch(primary_fetch), primary_name
    except Exception as exc:  # noqa: BLE001
        primary_error = exc

    try:
        return _retry_fetch(fallback_fetch), fallback_name
    except Exception as fallback_exc:  # noqa: BLE001
        raise RuntimeError(
            f"主接口失败({primary_name}): {primary_error}; 备用接口失败({fallback_name}): {fallback_exc}"
        ) from fallback_exc


def _standardize_kline_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df is None or raw_df.empty:
        return pd.DataFrame()

    df = raw_df.copy()

    date_col = _pick_column(df, ["日期", "date", "交易日期"])
    open_col = _pick_column(df, ["开盘", "open"])
    high_col = _pick_column(df, ["最高", "high"])
    low_col = _pick_column(df, ["最低", "low"])
    close_col = _pick_column(df, ["收盘", "close"])
    volume_col = _pick_column(df, ["成交量", "volume"])
    amount_col = _pick_column(df, ["成交额", "amount"])
    turn_col = _pick_column(df, ["换手率", "turnover", "turnover_rate"])
    change_amt_col = _pick_column(df, ["涨跌额", "change", "change_amount"])
    change_pct_col = _pick_column(
        df, ["涨跌幅", "pct_chg", "change_percent", "change_pct"])

    required = [date_col, open_col, high_col, low_col, close_col, volume_col]
    if any(col is None for col in required):
        return pd.DataFrame()

    temp = pd.DataFrame()
    temp["trade_date"] = pd.to_datetime(df[date_col], errors="coerce").dt.date
    temp["open_price"] = _to_numeric(df[open_col])
    temp["high_price"] = _to_numeric(df[high_col])
    temp["low_price"] = _to_numeric(df[low_col])
    temp["close_price"] = _to_numeric(df[close_col])
    temp["volume"] = _to_numeric(df[volume_col])
    temp["amount"] = _to_numeric(df[amount_col]) if amount_col else pd.NA
    temp["turnover_rate"] = _to_numeric(df[turn_col]) if turn_col else pd.NA
    temp["change_amount"] = _to_numeric(
        df[change_amt_col]) if change_amt_col else pd.NA
    temp["change_pct"] = _to_numeric(
        df[change_pct_col]) if change_pct_col else pd.NA

    temp = temp.dropna(subset=["trade_date", "close_price"])\
               .sort_values("trade_date")\
               .drop_duplicates(subset=["trade_date"], keep="last")

    temp["prev_close_price"] = temp["close_price"].shift(1)

    # 部分 AKShare 接口不返回涨跌额/涨跌幅，使用收盘价与昨收兜底计算。
    computed_change_amount = temp["close_price"] - temp["prev_close_price"]
    computed_change_pct = (computed_change_amount /
                           temp["prev_close_price"]) * 100
    temp["change_amount"] = temp["change_amount"].where(
        temp["change_amount"].notna(), computed_change_amount
    )
    temp["change_pct"] = temp["change_pct"].where(
        temp["change_pct"].notna(), computed_change_pct
    )

    return temp


def _float_or_none(value) -> Optional[float]:
    if value is None or pd.isna(value):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isfinite(number):
        return number
    return None


def _get_latest_trade_date(symbol_code: str) -> Optional[date]:
    conn, cursor = connect_invest_db()
    cursor.execute(
        "select max(trade_date) from stock_daily_kline_indicator where symbol_code=%s and adjust_type=%s",
        [symbol_code, ADJUST_TYPE],
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if not row or not row[0]:
        return None
    return row[0]


def _build_rows_for_upsert(symbol_code: str, symbol_type: str, df: pd.DataFrame) -> List[Tuple]:
    rows = []
    for item in df.to_dict(orient="records"):
        rows.append((
            symbol_code,
            symbol_type,
            item.get("trade_date"),
            ADJUST_TYPE,
            _float_or_none(item.get("open_price")),
            _float_or_none(item.get("high_price")),
            _float_or_none(item.get("low_price")),
            _float_or_none(item.get("close_price")),
            _float_or_none(item.get("prev_close_price")),
            _float_or_none(item.get("change_amount")),
            _float_or_none(item.get("change_pct")),
            _float_or_none(item.get("volume")),
            _float_or_none(item.get("amount")),
            _float_or_none(item.get("turnover_rate")),
            _float_or_none(item.get("ma5")),
            _float_or_none(item.get("ma10")),
            _float_or_none(item.get("ma20")),
            _float_or_none(item.get("ma60")),
            _float_or_none(item.get("rsi6")),
            _float_or_none(item.get("rsi14")),
            _float_or_none(item.get("dif")),
            _float_or_none(item.get("dea")),
            _float_or_none(item.get("macd_hist")),
            "akshare",
        ))
    return rows


def _upsert_market_rows(rows: List[Tuple]) -> int:
    if not rows:
        return 0

    sql = (
        "insert into stock_daily_kline_indicator ("
        "symbol_code, symbol_type, trade_date, adjust_type, "
        "open_price, high_price, low_price, close_price, prev_close_price, "
        "change_amount, change_pct, volume, amount, turnover_rate, "
        "ma5, ma10, ma20, ma60, rsi6, rsi14, dif, dea, macd_hist, data_source, "
        "created_at, updated_at"
        ") values ("
        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),now()"
        ") on duplicate key update "
        "symbol_type=values(symbol_type), "
        "open_price=values(open_price), high_price=values(high_price), low_price=values(low_price), "
        "close_price=values(close_price), prev_close_price=values(prev_close_price), "
        "change_amount=values(change_amount), change_pct=values(change_pct), "
        "volume=values(volume), amount=values(amount), turnover_rate=values(turnover_rate), "
        "ma5=values(ma5), ma10=values(ma10), ma20=values(ma20), ma60=values(ma60), "
        "rsi6=values(rsi6), rsi14=values(rsi14), dif=values(dif), dea=values(dea), macd_hist=values(macd_hist), "
        "data_source=values(data_source), updated_at=now()"
    )

    conn, cursor = connect_invest_db()
    cursor.executemany(sql, rows)
    affected = cursor.rowcount
    conn.commit()
    cursor.close()
    conn.close()
    return affected


def _create_job_log(job_name: str, run_mode: str) -> int:
    conn, cursor = connect_invest_db()
    cursor.execute(
        "insert into market_sync_job_log (job_name, run_mode, started_at, status, created_at, updated_at) "
        "values (%s,%s,now(),'running',now(),now())",
        [job_name, run_mode],
    )
    job_id = cursor.lastrowid
    conn.commit()
    cursor.close()
    conn.close()
    return job_id


def _finish_job_log(
    job_id: int,
    status: str,
    total_symbols: int,
    success_symbols: int,
    failed_symbols: int,
    upsert_rows: int,
    detail: Dict,
    error_summary: Optional[str] = None,
):
    conn, cursor = connect_invest_db()
    cursor.execute(
        "update market_sync_job_log set "
        "finished_at=now(), status=%s, total_symbols=%s, success_symbols=%s, failed_symbols=%s, "
        "upsert_rows=%s, error_summary=%s, detail_json=%s, updated_at=now() where id=%s",
        [
            status,
            total_symbols,
            success_symbols,
            failed_symbols,
            upsert_rows,
            error_summary,
            json.dumps(detail, ensure_ascii=False),
            job_id,
        ],
    )
    conn.commit()
    cursor.close()
    conn.close()


def _load_plan_symbol_windows(limit: Optional[int] = None) -> List[SymbolWindow]:
    conn, cursor = connect_summary_db(dictionary=True)
    sql = (
        "select stock_code, period_start, period_end "
        "from investment_review_plans "
        "where stock_code is not null and stock_code<>'' "
        "and period_start is not null and period_end is not null"
    )
    cursor.execute(sql)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    merged: Dict[str, Dict] = {}
    for row in rows:
        code = row.get("stock_code")
        start = row.get("period_start")
        end = row.get("period_end")
        if not code or not start or not end:
            continue

        normalized_code, symbol_type, _ = _normalize_symbol(code)
        start_window = _month_shift(start, -2)
        end_window = end + timedelta(days=30)

        if normalized_code not in merged:
            merged[normalized_code] = {
                "symbol_type": symbol_type,
                "start_date": start_window,
                "end_date": end_window,
            }
        else:
            merged[normalized_code]["start_date"] = min(
                merged[normalized_code]["start_date"], start_window
            )
            merged[normalized_code]["end_date"] = max(
                merged[normalized_code]["end_date"], end_window
            )

    items = [
        SymbolWindow(
            symbol_code=code,
            symbol_type=meta["symbol_type"],
            start_date=meta["start_date"],
            end_date=meta["end_date"],
        )
        for code, meta in merged.items()
    ]
    items.sort(key=lambda item: item.symbol_code)
    if limit:
        items = items[:limit]
    return items


def _load_watchlist_symbol_windows(limit: Optional[int] = None) -> List[SymbolWindow]:
    conn, cursor = connect_invest_db(dictionary=True)
    cursor.execute(
        "select symbol_code, symbol_type from market_watchlist where enabled=1 order by id desc"
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    today = date.today()
    windows: List[SymbolWindow] = []
    for row in rows:
        raw_code = row.get("symbol_code")
        raw_type = row.get("symbol_type")
        if not raw_code:
            continue

        normalized_code, symbol_type, _ = _normalize_symbol(raw_code, raw_type)
        latest = _get_latest_trade_date(normalized_code)
        if latest:
            start = latest - timedelta(days=LOOKBACK_DAYS)
        else:
            start = today - timedelta(days=WATCHLIST_INITIAL_DAYS)

        windows.append(
            SymbolWindow(
                symbol_code=normalized_code,
                symbol_type=symbol_type,
                start_date=start,
                end_date=today,
            )
        )

    windows.sort(key=lambda item: item.symbol_code)
    if limit:
        windows = windows[:limit]
    return windows


def _merge_windows(*groups: List[SymbolWindow]) -> List[SymbolWindow]:
    merged: Dict[str, SymbolWindow] = {}
    for group in groups:
        for item in group:
            if item.symbol_code not in merged:
                merged[item.symbol_code] = item
            else:
                old = merged[item.symbol_code]
                merged[item.symbol_code] = SymbolWindow(
                    symbol_code=item.symbol_code,
                    symbol_type=old.symbol_type,
                    start_date=min(old.start_date, item.start_date),
                    end_date=max(old.end_date, item.end_date),
                )
    return sorted(merged.values(), key=lambda x: x.symbol_code)


def _fetch_market_df(symbol_code: str, symbol_type: str, start_date: date, end_date: date) -> pd.DataFrame:
    unified_code, normalized_type, exchange_prefixed = _normalize_symbol(
        symbol_code, symbol_type)
    digits = unified_code.split(".")[0]

    if normalized_type == "stock":
        raw, _ = _fetch_with_fallback(
            primary_fetch=lambda: _fetch_stock_df(
                digits, start_date, end_date),
            fallback_fetch=lambda: _fetch_stock_df_alt(
                exchange_prefixed, start_date, end_date),
            primary_name="stock_zh_a_hist",
            fallback_name="stock_zh_a_daily",
        )
    elif normalized_type == "etf":
        raw, _ = _fetch_with_fallback(
            primary_fetch=lambda: _fetch_etf_df(digits, start_date, end_date),
            fallback_fetch=lambda: _fetch_etf_df_alt(
                exchange_prefixed, start_date, end_date),
            primary_name="fund_etf_hist_em",
            fallback_name="fund_etf_hist_sina",
        )
    else:
        raw, _ = _fetch_with_fallback(
            primary_fetch=lambda: _fetch_index_df(
                exchange_prefixed, start_date, end_date),
            fallback_fetch=lambda: _fetch_index_df_alt(
                exchange_prefixed, start_date, end_date),
            primary_name="stock_zh_index_daily_em",
            fallback_name="stock_zh_index_daily",
        )

    standardized = _standardize_kline_df(raw)
    if standardized.empty:
        return standardized

    standardized = _calc_indicators(standardized)
    return standardized


def _sync_one_symbol(item: SymbolWindow, dry_run: bool = False) -> Dict:
    latest = _get_latest_trade_date(item.symbol_code)
    start_date = item.start_date
    if latest:
        start_date = max(item.start_date, latest -
                         timedelta(days=LOOKBACK_DAYS))

    if start_date > item.end_date:
        return {
            "symbol_code": item.symbol_code,
            "symbol_type": item.symbol_type,
            "status": "skipped",
            "reason": "start_date > end_date",
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": item.end_date.strftime("%Y-%m-%d"),
            "upsert_rows": 0,
        }

    df = _fetch_market_df(
        item.symbol_code, item.symbol_type, start_date, item.end_date)
    if df.empty:
        return {
            "symbol_code": item.symbol_code,
            "symbol_type": item.symbol_type,
            "status": "empty",
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": item.end_date.strftime("%Y-%m-%d"),
            "upsert_rows": 0,
        }

    rows = _build_rows_for_upsert(item.symbol_code, item.symbol_type, df)
    upsert_rows = len(rows)
    if not dry_run:
        _upsert_market_rows(rows)

    return {
        "symbol_code": item.symbol_code,
        "symbol_type": item.symbol_type,
        "status": "success",
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": item.end_date.strftime("%Y-%m-%d"),
        "upsert_rows": upsert_rows,
    }


def sync_market_data(
    mode: str = "all",
    run_mode: str = "manual",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None,
    dry_run: bool = False,
) -> Dict:
    mode = (mode or "all").strip().lower()
    run_mode = (run_mode or "manual").strip().lower()
    if mode not in ("all", "plan", "watchlist"):
        raise ValueError("mode 仅支持 all/plan/watchlist")
    if run_mode not in ("manual", "intraday_30m", "close_confirm"):
        raise ValueError("run_mode 仅支持 manual/intraday_30m/close_confirm")

    now_value = datetime.now()
    if run_mode == "intraday_30m" and not _is_intraday_trading_time(now_value):
        return {
            "status": "skipped",
            "reason": "当前不在交易时段",
            "run_mode": run_mode,
            "timestamp": now_value.strftime("%Y-%m-%d %H:%M:%S"),
        }

    plan_windows: List[SymbolWindow] = []
    watchlist_windows: List[SymbolWindow] = []

    if mode in ("all", "plan"):
        plan_windows = _load_plan_symbol_windows(limit=limit)
    if mode in ("all", "watchlist"):
        watchlist_windows = _load_watchlist_symbol_windows(limit=limit)

    windows = _merge_windows(plan_windows, watchlist_windows)

    manual_start = _parse_date(start_date)
    manual_end = _parse_date(end_date)
    if manual_start or manual_end:
        if not manual_start or not manual_end:
            raise ValueError("start_date 和 end_date 需要同时传入")
        for i, item in enumerate(windows):
            windows[i] = SymbolWindow(
                symbol_code=item.symbol_code,
                symbol_type=item.symbol_type,
                start_date=manual_start,
                end_date=manual_end,
            )

    job_id = _create_job_log(job_name="akshare_market_sync", run_mode=run_mode)

    details = []
    failed_symbols = []
    success_count = 0
    total_upsert_rows = 0

    try:
        for item in windows:
            try:
                result = _sync_one_symbol(item, dry_run=dry_run)
                details.append(result)
                if result["status"] == "success":
                    success_count += 1
                    total_upsert_rows += result.get("upsert_rows", 0)
                elif result["status"] in ("skipped", "empty"):
                    success_count += 1
                else:
                    failed_symbols.append(item.symbol_code)
            except Exception as exc:  # noqa: BLE001
                details.append({
                    "symbol_code": item.symbol_code,
                    "symbol_type": item.symbol_type,
                    "status": "failed",
                    "error": str(exc),
                })
                failed_symbols.append(item.symbol_code)

        failed_count = len(failed_symbols)
        total_count = len(windows)
        status = "success" if failed_count == 0 else (
            "partial_success" if success_count > 0 else "failed")

        output = {
            "job_id": job_id,
            "status": status,
            "run_mode": run_mode,
            "mode": mode,
            "dry_run": dry_run,
            "total_symbols": total_count,
            "success_symbols": success_count,
            "failed_symbols": failed_count,
            "upsert_rows": total_upsert_rows,
            "failed_symbol_list": failed_symbols,
            "details": details,
        }

        _finish_job_log(
            job_id=job_id,
            status=status,
            total_symbols=total_count,
            success_symbols=success_count,
            failed_symbols=failed_count,
            upsert_rows=total_upsert_rows,
            detail=output,
            error_summary=None if not failed_symbols else f"failed symbols: {','.join(failed_symbols)}",
        )
        return output
    except Exception as exc:  # noqa: BLE001
        error_output = {
            "job_id": job_id,
            "status": "failed",
            "error": str(exc),
            "run_mode": run_mode,
            "mode": mode,
        }
        _finish_job_log(
            job_id=job_id,
            status="failed",
            total_symbols=len(windows),
            success_symbols=success_count,
            failed_symbols=max(1, len(windows) - success_count),
            upsert_rows=total_upsert_rows,
            detail=error_output,
            error_summary=str(exc),
        )
        raise


def main():
    parser = argparse.ArgumentParser(description="AKShare 行情与指标同步到 investdb")
    parser.add_argument("--mode", default="all",
                        choices=["all", "plan", "watchlist"])
    parser.add_argument(
        "--run-mode",
        dest="run_mode",
        default="manual",
        choices=["manual", "intraday_30m", "close_confirm"],
    )
    parser.add_argument("--start-date", dest="start_date", help="YYYY-MM-DD")
    parser.add_argument("--end-date", dest="end_date", help="YYYY-MM-DD")
    parser.add_argument("--limit", type=int, default=None, help="仅测试前 N 个标的")
    parser.add_argument("--dry-run", action="store_true", help="仅拉取与计算，不写入数据库")
    args = parser.parse_args()

    result = sync_market_data(
        mode=args.mode,
        run_mode=args.run_mode,
        start_date=args.start_date,
        end_date=args.end_date,
        limit=args.limit,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
