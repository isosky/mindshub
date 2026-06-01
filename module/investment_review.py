#!/usr/bin/python
# -*- coding: utf-8 -*-

import json
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from base.base import connect_database


def _serialize_value(value):
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    if isinstance(value, date):
        return value.strftime('%Y-%m-%d')
    if isinstance(value, Decimal):
        return float(value)
    return value


def _serialize_row(row):
    if not row:
        return row
    return {key: _serialize_value(value) for key, value in row.items()}


def _serialize_rows(rows):
    return [_serialize_row(row) for row in (rows or [])]


def _json_dump(data):
    if data is None:
        return None
    return json.dumps(data, ensure_ascii=False)


def _to_decimal(value, digits=4):
    if value in (None, ''):
        return None
    if isinstance(value, Decimal):
        return value.quantize(Decimal('1.' + ('0' * digits)))
    try:
        decimal_value = Decimal(str(value))
        return decimal_value.quantize(Decimal('1.' + ('0' * digits)))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _to_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_optional_int(value):
    if value in (None, ''):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _clean_text(value):
    if value is None:
        return None
    text = str(value).strip()
    return text if text != '' else None


def _parse_period(entry_form):
    period = entry_form.get('period') or []
    if isinstance(period, (list, tuple)) and len(period) >= 2:
        return period[0] or None, period[1] or None
    return None, None


def _parse_numeric_volume(value):
    if value in (None, ''):
        return None
    parts = re.findall(r'\d+(?:\.\d+)?', str(value))
    if not parts:
        return None
    return _to_decimal(parts[0], digits=4)


def _parse_position_ratio(value):
    if value in (None, ''):
        return None
    text = str(value).strip().replace('%', '')
    decimal_value = _to_decimal(text, digits=4)
    if decimal_value is None:
        return None
    return (decimal_value / Decimal('100')).quantize(Decimal('1.0000'))


def _parse_entry_range(entry_zone):
    if not entry_zone:
        return None
    parts = re.findall(r'\d+(?:\.\d+)?', str(entry_zone))
    if not parts:
        return None
    if len(parts) == 1:
        value = _to_decimal(parts[0], digits=4)
        return {'min': value, 'max': value}
    first = _to_decimal(parts[0], digits=4)
    second = _to_decimal(parts[1], digits=4)
    if first is None or second is None:
        return None
    return {'min': min(first, second), 'max': max(first, second)}


def _sort_time_key(value):
    if value is None:
        return ''
    return str(value)


def _get_entry_executions(execution_records):
    rows = []
    for item in execution_records or []:
        if item.get('action') not in ('买入', '加仓'):
            continue
        price = _to_decimal(item.get('price'))
        volume = _parse_numeric_volume(item.get('volume'))
        if price is None or volume is None or volume <= 0:
            continue
        rows.append({
            'time': item.get('time'),
            'price': price,
            'volume': volume,
        })
    rows.sort(key=lambda item: _sort_time_key(item.get('time')))
    return rows


def _get_exit_executions(execution_records):
    rows = []
    for item in execution_records or []:
        if item.get('action') not in ('卖出', '减仓'):
            continue
        price = _to_decimal(item.get('price'))
        volume = _parse_numeric_volume(item.get('volume'))
        if price is None or volume is None or volume <= 0:
            continue
        rows.append({
            'time': item.get('time'),
            'price': price,
            'volume': volume,
        })
    rows.sort(key=lambda item: _sort_time_key(item.get('time')), reverse=True)
    return rows


def _calculate_avg_entry_price(execution_records):
    entry_rows = _get_entry_executions(execution_records)
    total_amount = Decimal('0')
    total_volume = Decimal('0')
    for item in entry_rows:
        total_amount += item['price'] * item['volume']
        total_volume += item['volume']
    if total_volume <= 0:
        return None
    return (total_amount / total_volume).quantize(Decimal('1.0000'))


def _calculate_exit_price(execution_records):
    exit_rows = _get_exit_executions(execution_records)
    if not exit_rows:
        return None
    return exit_rows[0]['price']


def _calculate_exited_volume(execution_records):
    exit_rows = _get_exit_executions(execution_records)
    total_volume = Decimal('0')
    for item in exit_rows:
        total_volume += item['volume']
    return total_volume if total_volume > 0 else None


def _calculate_risk_reward(entry_form):
    entry_range = _parse_entry_range(entry_form.get('entryZone'))
    stop_loss = _to_decimal(entry_form.get('stopLoss'))
    target_price = _to_decimal(entry_form.get('targetPrice'))
    if not entry_range or stop_loss is None or target_price is None:
        return None
    entry_price = (entry_range['min'] + entry_range['max']) / Decimal('2')
    denominator = entry_price - stop_loss
    if denominator <= 0:
        return None
    ratio = (target_price - entry_price) / denominator
    return ratio.quantize(Decimal('1.0000')) if ratio.is_finite() else None


def _calculate_execution_deviation(entry_form, modifications, execution_records):
    entry_rows = _get_entry_executions(execution_records)
    if not entry_rows:
        return '未执行'
    deviations = []
    first_entry = entry_rows[0]
    entry_range = _parse_entry_range(entry_form.get('entryZone'))
    if entry_range:
        if first_entry['price'] > entry_range['max']:
            deviations.append('高于计划区间买入')
        elif first_entry['price'] < entry_range['min']:
            deviations.append('低于计划区间提前买入')
    if len(modifications or []) > 1:
        deviations.append(f'有{len(modifications) - 1}次计划调整')
    has_reduce = any(item.get('action') ==
                     '减仓' for item in (execution_records or []))
    if has_reduce:
        deviations.append('过程中存在主动减仓')
    return '，'.join(deviations) if deviations else '基本按计划执行'


def _build_review_metrics(entry_form, review_form, review_summary, modifications, execution_records):
    avg_entry_price = _calculate_avg_entry_price(execution_records)
    exit_price = _calculate_exit_price(execution_records)
    exited_volume = _calculate_exited_volume(execution_records)
    pnl_amount = None
    pnl_ratio = None
    if avg_entry_price is not None and exit_price is not None and exited_volume is not None:
        pnl_amount = ((exit_price - avg_entry_price) *
                      exited_volume).quantize(Decimal('1.00'))
        if avg_entry_price > 0:
            pnl_ratio = (((exit_price - avg_entry_price) / avg_entry_price)
                         * Decimal('100')).quantize(Decimal('1.0000'))

    return {
        'review_status': _clean_text(review_summary.get('status')) or 'pending',
        'avg_entry_price': avg_entry_price,
        'exit_price': exit_price,
        'realized_pnl_amount': pnl_amount,
        'realized_pnl_ratio': pnl_ratio,
        'risk_reward_ratio': _calculate_risk_reward(entry_form),
        'execution_deviation': _calculate_execution_deviation(entry_form, modifications, execution_records),
        'did_well': review_form.get('didWell'),
        'did_wrong': review_form.get('didWrong'),
        'buy_emotion': review_form.get('buyEmotion'),
        'hold_emotion': review_form.get('holdEmotion'),
        'sell_emotion': review_form.get('sellEmotion'),
        'improvement_action': review_form.get('improvementAction'),
        'review_snapshot_json': _json_dump({
            'reviewForm': review_form,
            'reviewSummary': review_summary,
        }),
        'reviewed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }


def _resolve_plan(cursor, plan_id=None, plan_code=None):
    if plan_id:
        cursor.execute(
            'select * from investment_review_plans where id=%s limit 1',
            [int(plan_id)],
        )
        row = cursor.fetchone()
        if row:
            return row
    if plan_code:
        cursor.execute(
            'select * from investment_review_plans where plan_code=%s limit 1',
            [plan_code],
        )
        row = cursor.fetchone()
        if row:
            return row
    return None


def query_investment_review_plan_list(keyword=None, plan_status=None):
    conn, cursor = connect_database(dictionary=True)
    sql = (
        'select id, plan_code, stock_code, stock_name, industry, record_type, plan_type, '
        'trade_direction, plan_status, period_start, period_end, open_strategy, close_strategy, '
        'plan_score, created_at, updated_at '
        'from investment_review_plans where 1=1'
    )
    params = []
    if keyword:
        sql += ' and (stock_code like %s or stock_name like %s or tags_text like %s)'
        like_keyword = f'%{keyword}%'
        params.extend([like_keyword, like_keyword, like_keyword])
    if plan_status:
        sql += ' and plan_status=%s'
        params.append(plan_status)
    sql += ' order by updated_at desc, id desc'
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return _serialize_rows(rows)


def get_investment_review_plan_detail(plan_id=None, plan_code=None):
    conn, cursor = connect_database(dictionary=True)
    plan_row = _resolve_plan(cursor, plan_id=plan_id, plan_code=plan_code)
    if not plan_row:
        cursor.close()
        conn.close()
        return None

    cursor.execute(
        'select * from investment_review_plan_modifications where plan_id=%s and ifnull(is_deleted, 0)=0 '
        'order by modification_time desc, id desc',
        [plan_row['id']],
    )
    modification_rows = cursor.fetchall()

    cursor.execute(
        'select * from investment_review_executions where plan_id=%s and ifnull(is_deleted, 0)=0 '
        'order by execution_time desc, id desc',
        [plan_row['id']],
    )
    execution_rows = cursor.fetchall()

    cursor.execute(
        'select * from investment_review_reviews where plan_id=%s limit 1',
        [plan_row['id']],
    )
    review_row = cursor.fetchone()
    cursor.close()
    conn.close()
    return {
        'plan': _serialize_row(plan_row),
        'modifications': _serialize_rows(modification_rows),
        'executions': _serialize_rows(execution_rows),
        'review': _serialize_row(review_row),
    }


def _save_plan_main(cursor, payload, snapshot_json):
    entry_form = payload.get('entryForm') or payload.get('entry_form') or {}
    review_summary = payload.get(
        'reviewSummary') or payload.get('review_summary') or {}
    period_start, period_end = _parse_period(entry_form)
    plan_code = _clean_text(payload.get('planCode') or payload.get(
        'plan_code') or payload.get('currentPlanId') or payload.get('current_plan_id'))
    if not plan_code:
        plan_code = f'plan-{int(datetime.now().timestamp() * 1000)}'

    plan_row = _resolve_plan(
        cursor,
        plan_id=payload.get('id') or payload.get('plan_id'),
        plan_code=plan_code,
    )

    values = [
        plan_code,
        _clean_text(entry_form.get('stockCode')) or '',
        _clean_text(entry_form.get('stockName')) or '',
        _clean_text(entry_form.get('industry')),
        _clean_text(payload.get('recordType')
                    or payload.get('record_type')) or 'real',
        _clean_text(entry_form.get('planType')) or '执行计划',
        _clean_text(payload.get('tradeDirection')
                    or payload.get('trade_direction')) or 'long',
        _clean_text(review_summary.get('status')) or _clean_text(
            payload.get('planStatus') or payload.get('plan_status')) or 'draft',
        period_start,
        period_end,
        _clean_text(entry_form.get('openStrategy')),
        _clean_text(entry_form.get('closeStrategy')),
        entry_form.get('reason'),
        _clean_text(entry_form.get('entryZone')),
        _to_decimal(entry_form.get('stopLoss')),
        _to_decimal(entry_form.get('targetPrice')),
        entry_form.get('marketStatus'),
        entry_form.get('sectorStatus'),
        _clean_text(entry_form.get('tags')),
        _to_int(review_summary.get('score'), plan_row.get(
            'plan_score') if plan_row else 0),
        snapshot_json if not plan_row else plan_row.get(
            'initial_plan_snapshot_json') or snapshot_json,
        snapshot_json,
    ]

    if plan_row:
        cursor.execute(
            'update investment_review_plans set '
            'plan_code=%s, stock_code=%s, stock_name=%s, industry=%s, record_type=%s, '
            'plan_type=%s, trade_direction=%s, plan_status=%s, period_start=%s, period_end=%s, '
            'open_strategy=%s, close_strategy=%s, reason=%s, entry_zone=%s, stop_loss=%s, '
            'target_price=%s, market_status=%s, sector_status=%s, tags_text=%s, plan_score=%s, '
            'initial_plan_snapshot_json=%s, current_plan_snapshot_json=%s, updated_at=now() '
            'where id=%s',
            values + [plan_row['id']],
        )
        return plan_row['id'], plan_code

    cursor.execute(
        'insert into investment_review_plans '
        '(plan_code, stock_code, stock_name, industry, record_type, plan_type, trade_direction, '
        'plan_status, period_start, period_end, open_strategy, close_strategy, reason, entry_zone, '
        'stop_loss, target_price, market_status, sector_status, tags_text, plan_score, '
        'initial_plan_snapshot_json, current_plan_snapshot_json, created_at, updated_at) '
        'values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),now())',
        values,
    )
    return cursor.lastrowid, plan_code


def _replace_modifications(cursor, plan_id, modifications):
    cursor.execute(
        'select id from investment_review_plan_modifications where plan_id=%s', [plan_id])
    existing_ids = {row['id'] for row in (cursor.fetchall() or [])}
    active_ids = []
    for item in modifications or []:
        item_id = _to_optional_int(item.get('id'))
        params = [
            item.get('time') or datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S'),
            _clean_text(item.get('label')),
            _clean_text(item.get('tagType') or item.get('tag_type')),
            _clean_text(item.get('title')) or '未命名修改',
            item.get('reason'),
            item.get('plan') or item.get(
                'updatedPlan') or item.get('updated_plan'),
            item.get('evaluation'),
            _json_dump(item),
        ]
        if item_id and item_id in existing_ids:
            cursor.execute(
                'update investment_review_plan_modifications set '
                'modification_time=%s, modification_label=%s, tag_type=%s, title=%s, '
                'reason=%s, updated_plan=%s, evaluation=%s, plan_snapshot_json=%s, '
                'is_deleted=0, deleted_at=null, updated_at=now() '
                'where id=%s and plan_id=%s',
                params + [item_id, plan_id],
            )
            active_ids.append(item_id)
            continue

        cursor.execute(
            'insert into investment_review_plan_modifications '
            '(plan_id, modification_time, modification_label, tag_type, title, reason, updated_plan, evaluation, plan_snapshot_json, is_deleted, deleted_at, created_at, updated_at) '
            'values (%s,%s,%s,%s,%s,%s,%s,%s,%s,0,null,now(),now())',
            [plan_id] + params,
        )
        active_ids.append(cursor.lastrowid)

    if active_ids:
        placeholders = ','.join(['%s'] * len(active_ids))
        cursor.execute(
            'update investment_review_plan_modifications set is_deleted=1, deleted_at=now(), updated_at=now() '
            f'where plan_id=%s and ifnull(is_deleted, 0)=0 and id not in ({placeholders})',
            [plan_id] + active_ids,
        )
    else:
        cursor.execute(
            'update investment_review_plan_modifications set is_deleted=1, deleted_at=now(), updated_at=now() '
            'where plan_id=%s and ifnull(is_deleted, 0)=0',
            [plan_id],
        )


def _replace_executions(cursor, plan_id, execution_records):
    cursor.execute(
        'select id from investment_review_executions where plan_id=%s', [plan_id])
    existing_ids = {row['id'] for row in (cursor.fetchall() or [])}
    active_ids = []
    for item in execution_records or []:
        item_id = _to_optional_int(item.get('id'))
        params = [
            item.get('time') or datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S'),
            _clean_text(item.get('action')) or '买入',
            _to_decimal(item.get('price')),
            _parse_numeric_volume(item.get('volume')),
            _parse_position_ratio(item.get('position')),
            _clean_text(item.get('position')),
            item.get('note'),
        ]
        if item_id and item_id in existing_ids:
            cursor.execute(
                'update investment_review_executions set '
                'execution_time=%s, action=%s, price=%s, volume=%s, position_ratio=%s, '
                'position_text=%s, note=%s, is_deleted=0, deleted_at=null, updated_at=now() '
                'where id=%s and plan_id=%s',
                params + [item_id, plan_id],
            )
            active_ids.append(item_id)
            continue

        cursor.execute(
            'insert into investment_review_executions '
            '(plan_id, execution_time, action, price, volume, position_ratio, position_text, note, is_deleted, deleted_at, created_at, updated_at) '
            'values (%s,%s,%s,%s,%s,%s,%s,%s,0,null,now(),now())',
            [plan_id] + params,
        )
        active_ids.append(cursor.lastrowid)

    if active_ids:
        placeholders = ','.join(['%s'] * len(active_ids))
        cursor.execute(
            'update investment_review_executions set is_deleted=1, deleted_at=now(), updated_at=now() '
            f'where plan_id=%s and ifnull(is_deleted, 0)=0 and id not in ({placeholders})',
            [plan_id] + active_ids,
        )
    else:
        cursor.execute(
            'update investment_review_executions set is_deleted=1, deleted_at=now(), updated_at=now() '
            'where plan_id=%s and ifnull(is_deleted, 0)=0',
            [plan_id],
        )


def _upsert_review(cursor, plan_id, payload):
    entry_form = payload.get('entryForm') or payload.get('entry_form') or {}
    review_form = payload.get('reviewForm') or payload.get('review_form') or {}
    review_summary = payload.get(
        'reviewSummary') or payload.get('review_summary') or {}
    modifications = payload.get('modifications') or []
    execution_records = payload.get(
        'executionRecords') or payload.get('execution_records') or []
    metrics = _build_review_metrics(
        entry_form, review_form, review_summary, modifications, execution_records)

    cursor.execute(
        'select id from investment_review_reviews where plan_id=%s limit 1', [plan_id])
    review_row = cursor.fetchone()
    params = [
        plan_id,
        metrics['review_status'],
        metrics['avg_entry_price'],
        metrics['exit_price'],
        metrics['realized_pnl_amount'],
        metrics['realized_pnl_ratio'],
        None,
        None,
        metrics['risk_reward_ratio'],
        metrics['execution_deviation'],
        metrics['did_well'],
        metrics['did_wrong'],
        metrics['buy_emotion'],
        metrics['hold_emotion'],
        metrics['sell_emotion'],
        metrics['improvement_action'],
        payload.get('reviewConclusion') or payload.get('review_conclusion'),
        metrics['review_snapshot_json'],
        metrics['reviewed_at'],
    ]
    if review_row:
        cursor.execute(
            'update investment_review_reviews set '
            'review_status=%s, avg_entry_price=%s, exit_price=%s, realized_pnl_amount=%s, '
            'realized_pnl_ratio=%s, max_floating_pnl_amount=%s, max_floating_pnl_ratio=%s, '
            'risk_reward_ratio=%s, execution_deviation=%s, did_well=%s, did_wrong=%s, '
            'buy_emotion=%s, hold_emotion=%s, sell_emotion=%s, improvement_action=%s, '
            'review_conclusion=%s, review_snapshot_json=%s, reviewed_at=%s, updated_at=now() '
            'where plan_id=%s',
            params[1:] + [plan_id],
        )
        return

    cursor.execute(
        'insert into investment_review_reviews '
        '(plan_id, review_status, avg_entry_price, exit_price, realized_pnl_amount, realized_pnl_ratio, '
        'max_floating_pnl_amount, max_floating_pnl_ratio, risk_reward_ratio, execution_deviation, '
        'did_well, did_wrong, buy_emotion, hold_emotion, sell_emotion, improvement_action, '
        'review_conclusion, review_snapshot_json, reviewed_at, created_at, updated_at) '
        'values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),now())',
        params,
    )


def save_investment_review_plan_bundle(payload):
    payload = payload or {}
    conn, cursor = connect_database(dictionary=True)
    snapshot_json = _json_dump(payload)
    try:
        plan_id, plan_code = _save_plan_main(cursor, payload, snapshot_json)
        _replace_modifications(
            cursor, plan_id, payload.get('modifications') or [])
        _replace_executions(cursor, plan_id, payload.get(
            'executionRecords') or payload.get('execution_records') or [])
        conn.commit()
    except Exception:
        conn.rollback()
        cursor.close()
        conn.close()
        raise

    cursor.close()
    conn.close()
    return get_investment_review_plan_detail(plan_id=plan_id, plan_code=plan_code)


def save_investment_review_modification(plan_id=None, plan_code=None, payload=None):
    payload = payload or {}
    conn, cursor = connect_database(dictionary=True)
    plan_row = _resolve_plan(cursor, plan_id=plan_id, plan_code=plan_code)
    if not plan_row:
        cursor.close()
        conn.close()
        raise ValueError('未找到对应计划')
    modification_id = _to_optional_int(payload.get('id'))
    params = [
        payload.get('time') or datetime.now().strftime(
            '%Y-%m-%d %H:%M:%S'),
        _clean_text(payload.get('label')),
        _clean_text(payload.get('tagType') or payload.get('tag_type')),
        _clean_text(payload.get('title')) or '未命名修改',
        payload.get('reason'),
        payload.get('plan') or payload.get(
            'updatedPlan') or payload.get('updated_plan'),
        payload.get('evaluation'),
        _json_dump(payload),
    ]
    if modification_id:
        cursor.execute(
            'update investment_review_plan_modifications set '
            'modification_time=%s, modification_label=%s, tag_type=%s, title=%s, reason=%s, '
            'updated_plan=%s, evaluation=%s, plan_snapshot_json=%s, is_deleted=0, deleted_at=null, updated_at=now() '
            'where id=%s and plan_id=%s',
            params + [modification_id, plan_row['id']],
        )
    else:
        cursor.execute(
            'insert into investment_review_plan_modifications '
            '(plan_id, modification_time, modification_label, tag_type, title, reason, updated_plan, evaluation, plan_snapshot_json, is_deleted, deleted_at, created_at, updated_at) '
            'values (%s,%s,%s,%s,%s,%s,%s,%s,%s,0,null,now(),now())',
            [plan_row['id']] + params,
        )
    conn.commit()
    cursor.close()
    conn.close()
    return get_investment_review_plan_detail(plan_id=plan_row['id'])


def save_investment_review_execution(plan_id=None, plan_code=None, payload=None):
    payload = payload or {}
    conn, cursor = connect_database(dictionary=True)
    plan_row = _resolve_plan(cursor, plan_id=plan_id, plan_code=plan_code)
    if not plan_row:
        cursor.close()
        conn.close()
        raise ValueError('未找到对应计划')
    cursor.execute(
        'insert into investment_review_executions '
        '(plan_id, execution_time, action, price, volume, position_ratio, position_text, note, created_at, updated_at) '
        'values (%s,%s,%s,%s,%s,%s,%s,%s,now(),now())',
        [
            plan_row['id'],
            payload.get('time') or datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S'),
            _clean_text(payload.get('action')) or '买入',
            _to_decimal(payload.get('price')),
            _parse_numeric_volume(payload.get('volume')),
            _parse_position_ratio(payload.get('position')),
            _clean_text(payload.get('position')),
            payload.get('note'),
        ],
    )
    conn.commit()
    cursor.close()
    conn.close()
    return get_investment_review_plan_detail(plan_id=plan_row['id'])


def delete_investment_review_modification(plan_id=None, plan_code=None, modification_id=None):
    modification_id = _to_optional_int(modification_id)
    if not modification_id:
        raise ValueError('缺少修改记录ID')
    conn, cursor = connect_database(dictionary=True)
    plan_row = _resolve_plan(cursor, plan_id=plan_id, plan_code=plan_code)
    if not plan_row:
        cursor.close()
        conn.close()
        raise ValueError('未找到对应计划')
    cursor.execute(
        'update investment_review_plan_modifications set is_deleted=1, deleted_at=now(), updated_at=now() '
        'where id=%s and plan_id=%s',
        [modification_id, plan_row['id']],
    )
    conn.commit()
    cursor.close()
    conn.close()
    return get_investment_review_plan_detail(plan_id=plan_row['id'])


def delete_investment_review_execution(plan_id=None, plan_code=None, execution_id=None):
    execution_id = _to_optional_int(execution_id)
    if not execution_id:
        raise ValueError('缺少执行记录ID')
    conn, cursor = connect_database(dictionary=True)
    plan_row = _resolve_plan(cursor, plan_id=plan_id, plan_code=plan_code)
    if not plan_row:
        cursor.close()
        conn.close()
        raise ValueError('未找到对应计划')
    cursor.execute(
        'update investment_review_executions set is_deleted=1, deleted_at=now(), updated_at=now() '
        'where id=%s and plan_id=%s',
        [execution_id, plan_row['id']],
    )
    conn.commit()
    cursor.close()
    conn.close()
    return get_investment_review_plan_detail(plan_id=plan_row['id'])


def save_investment_review_review(plan_id=None, plan_code=None, payload=None):
    payload = payload or {}
    conn, cursor = connect_database(dictionary=True)
    plan_row = _resolve_plan(cursor, plan_id=plan_id, plan_code=plan_code)
    if not plan_row:
        cursor.close()
        conn.close()
        raise ValueError('未找到对应计划')
    full_payload = {
        'entryForm': payload.get('entryForm') or payload.get('entry_form') or {},
        'reviewForm': payload.get('reviewForm') or payload.get('review_form') or {},
        'reviewSummary': payload.get('reviewSummary') or payload.get('review_summary') or {},
        'modifications': payload.get('modifications') or [],
        'executionRecords': payload.get('executionRecords') or payload.get('execution_records') or [],
        'reviewConclusion': payload.get('reviewConclusion') or payload.get('review_conclusion'),
    }
    _upsert_review(cursor, plan_row['id'], full_payload)
    review_summary = full_payload.get('reviewSummary') or {}
    cursor.execute(
        'update investment_review_plans set plan_score=%s, plan_status=%s, updated_at=now() where id=%s',
        [
            _to_int(review_summary.get('score'),
                    plan_row.get('plan_score') or 0),
            _clean_text(review_summary.get('status')) or plan_row.get(
                'plan_status') or 'draft',
            plan_row['id'],
        ],
    )
    conn.commit()
    cursor.close()
    conn.close()
    return get_investment_review_plan_detail(plan_id=plan_row['id'])
