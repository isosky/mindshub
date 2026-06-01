#!/usr/bin/python
# -*- coding: utf-8 -*-

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

import mysql.connector

from base.config import investdbconfig
from data_collector.akshare_market_sync import _normalize_symbol, sync_market_data


def _connect_invest_db(dictionary: bool = False):
    conn = mysql.connector.connect(**investdbconfig)
    cursor = conn.cursor(dictionary=dictionary)
    return conn, cursor


def _serialize_value(value: Any):
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    if isinstance(value, date):
        return value.strftime('%Y-%m-%d')
    if isinstance(value, Decimal):
        return float(value)
    return value


def _serialize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {key: _serialize_value(value) for key, value in (row or {}).items()}


def _serialize_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [_serialize_row(item) for item in (rows or [])]


def _to_int(value, default=None):
    if value in (None, ''):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_bool(value, default=False):
    if value in (True, 1, '1', 'true', 'True', 'yes', 'on'):
        return True
    if value in (False, 0, '0', 'false', 'False', 'no', 'off'):
        return False
    return default


def trigger_market_sync(payload: Optional[Dict[str, Any]] = None):
    payload = payload or {}
    result = sync_market_data(
        mode=payload.get('mode', 'all'),
        run_mode=payload.get('run_mode', 'manual'),
        start_date=payload.get('start_date'),
        end_date=payload.get('end_date'),
        limit=_to_int(payload.get('limit')),
        dry_run=_to_bool(payload.get('dry_run'), default=False),
    )
    return result


def list_sync_job_logs(limit: int = 20, run_mode: Optional[str] = None, status: Optional[str] = None):
    sql = (
        'select id, job_name, run_mode, started_at, finished_at, status, '
        'total_symbols, success_symbols, failed_symbols, upsert_rows, error_summary, detail_json, created_at, updated_at '
        'from market_sync_job_log where 1=1'
    )
    params: List[Any] = []

    if run_mode:
        sql += ' and run_mode=%s'
        params.append(run_mode)
    if status:
        sql += ' and status=%s'
        params.append(status)

    sql += ' order by id desc limit %s'
    params.append(_to_int(limit, default=20) or 20)

    conn, cursor = _connect_invest_db(dictionary=True)
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    serialized_rows = _serialize_rows(rows)
    for row in serialized_rows:
        detail_text = row.get('detail_json')
        if not detail_text:
            row['detail'] = None
            continue
        try:
            row['detail'] = json.loads(detail_text)
        except Exception:  # noqa: BLE001
            row['detail'] = None

    return serialized_rows


def query_kline_indicators(symbol_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None, limit: int = 1200):
    if not symbol_code:
        raise ValueError('symbol_code 不能为空')

    normalized_symbol, _, _ = _normalize_symbol(symbol_code)

    sql = (
        'select symbol_code, trade_date, open_price, high_price, low_price, close_price, '
        'prev_close_price, change_amount, change_pct, volume, amount, turnover_rate, '
        'ma5, ma10, ma20, ma60, rsi6, rsi14, dif, dea, macd_hist '
        'from stock_daily_kline_indicator '
        'where symbol_code=%s and adjust_type=%s'
    )
    params: List[Any] = [normalized_symbol, 'qfq']

    if start_date:
        sql += ' and trade_date >= %s'
        params.append(start_date)
    if end_date:
        sql += ' and trade_date <= %s'
        params.append(end_date)

    sql += ' order by trade_date asc limit %s'
    params.append(_to_int(limit, default=1200))

    conn, cursor = _connect_invest_db(dictionary=True)
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    serialized_rows = _serialize_rows(rows)

    chart = {
        'dates': [row['trade_date'] for row in serialized_rows],
        'kline': [
            [row['open_price'], row['close_price'],
                row['low_price'], row['high_price']]
            for row in serialized_rows
        ],
        'volume': [row['volume'] for row in serialized_rows],
        'amount': [row['amount'] for row in serialized_rows],
        'turnover_rate': [row['turnover_rate'] for row in serialized_rows],
        'ma5': [row['ma5'] for row in serialized_rows],
        'ma10': [row['ma10'] for row in serialized_rows],
        'ma20': [row['ma20'] for row in serialized_rows],
        'ma60': [row['ma60'] for row in serialized_rows],
        'rsi6': [row['rsi6'] for row in serialized_rows],
        'rsi14': [row['rsi14'] for row in serialized_rows],
        'dif': [row['dif'] for row in serialized_rows],
        'dea': [row['dea'] for row in serialized_rows],
        'macd_hist': [row['macd_hist'] for row in serialized_rows],
    }

    return {
        'symbol_code': normalized_symbol,
        'count': len(serialized_rows),
        'rows': serialized_rows,
        'chart': chart,
    }


def list_watchlist(enabled: Optional[int] = None, symbol_type: Optional[str] = None):
    sql = (
        'select id, symbol_code, symbol_name, symbol_type, enabled, remark, created_by, created_at, updated_at '
        'from market_watchlist where 1=1'
    )
    params: List[Any] = []

    if enabled in (0, 1):
        sql += ' and enabled=%s'
        params.append(enabled)

    if symbol_type:
        sql += ' and symbol_type=%s'
        params.append(symbol_type)

    sql += ' order by enabled desc, id desc'

    conn, cursor = _connect_invest_db(dictionary=True)
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    return _serialize_rows(rows)


def add_watchlist(symbol_code: str, symbol_type: Optional[str] = None, symbol_name: Optional[str] = None, remark: Optional[str] = None, created_by: Optional[str] = None):
    if not symbol_code:
        raise ValueError('symbol_code 不能为空')

    normalized_symbol, normalized_type, _ = _normalize_symbol(
        symbol_code, symbol_type)

    conn, cursor = _connect_invest_db()
    cursor.execute(
        'insert into market_watchlist '
        '(symbol_code, symbol_name, symbol_type, enabled, remark, created_by, created_at, updated_at) '
        'values (%s,%s,%s,1,%s,%s,now(),now()) '
        'on duplicate key update '
        'symbol_name=values(symbol_name), symbol_type=values(symbol_type), remark=values(remark), '
        'created_by=values(created_by), enabled=1, updated_at=now()',
        [normalized_symbol, symbol_name, normalized_type,
            remark, created_by or 'system'],
    )
    conn.commit()
    affected = cursor.rowcount
    cursor.close()
    conn.close()

    return {
        'symbol_code': normalized_symbol,
        'symbol_type': normalized_type,
        'affected_rows': affected,
    }


def remove_watchlist(symbol_code: str):
    if not symbol_code:
        raise ValueError('symbol_code 不能为空')

    normalized_symbol, _, _ = _normalize_symbol(symbol_code)
    conn, cursor = _connect_invest_db()
    cursor.execute('delete from market_watchlist where symbol_code=%s', [
                   normalized_symbol])
    conn.commit()
    affected = cursor.rowcount
    cursor.close()
    conn.close()

    return {
        'symbol_code': normalized_symbol,
        'affected_rows': affected,
    }


def toggle_watchlist(symbol_code: str, enabled: bool):
    if not symbol_code:
        raise ValueError('symbol_code 不能为空')

    normalized_symbol, _, _ = _normalize_symbol(symbol_code)
    enabled_int = 1 if enabled else 0

    conn, cursor = _connect_invest_db()
    cursor.execute(
        'update market_watchlist set enabled=%s, updated_at=now() where symbol_code=%s',
        [enabled_int, normalized_symbol],
    )
    conn.commit()
    affected = cursor.rowcount
    cursor.close()
    conn.close()

    return {
        'symbol_code': normalized_symbol,
        'enabled': enabled_int,
        'affected_rows': affected,
    }
