#!/usr/bin/python
# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request
from extensions import auth
from fund import fund_base, fund_estimate, fund_order, fund_total, fund_setting, fund_review
from data_collector import fund_collector

bp = Blueprint('fund', __name__, url_prefix='')


@bp.route('/get_fund_info')
@auth.login_required
def get_fund_info():
    temp, temp1 = fund_base.get_fund_info()
    return jsonify({'data': temp, 'listdata': temp1})


@bp.route('/get_order_data')
@auth.login_required
def get_order_data():
    temp = fund_order.get_order_data()
    return jsonify({'data': temp})


@bp.route('/get_funds_for_cost_update')
@auth.login_required
def get_funds_for_cost_update():
    temp, temp_dict = fund_order.get_funds_for_cost_update()
    return jsonify({'data': temp, 'data_list': temp_dict})


@bp.route('/get_fund_base')
@auth.login_required
def get_fund_base():
    temp_data = fund_order.get_fund_base()
    return jsonify({'data': temp_data})


@bp.route('/add_new_fund', methods=['POST'])
@auth.login_required
def add_new_fund():
    json_data = request.get_json(force=True)
    new_fund_code = json_data['new_fund_code']
    new_fund_name = json_data['new_fund_name']
    msg = fund_order.add_new_fund(new_fund_code, new_fund_name)
    return jsonify({"msg": msg})


@bp.route('/get_cost_info')
@auth.login_required
def get_cost_info():
    temp = fund_order.get_cost_info()
    return jsonify({'data': temp})


@bp.route('/get_fund_closed_net_value')
@auth.login_required
def get_fund_closed_net_value():
    temp = fund_order.get_fund_closed_net_value()
    return jsonify({'data': temp})


@bp.route('/commitorders', methods=['POST'])
@auth.login_required
def commitorders():
    json_data = request.get_json(force=True)
    ordertype = json_data['ordertype']
    orderform = json_data['orderform']
    if ordertype:
        temp = fund_order.add_buy_order(orderform)
    else:
        temp = fund_order.add_sell_order(orderform)
    return jsonify({'res': temp})


@bp.route('/update_fund_cost', methods=['POST'])
@auth.login_required
def update_fund_cost():
    json_data = request.get_json(force=True)
    fund_code = json_data['fund_code']
    cost = json_data['cost']
    res = fund_order.update_fund_cost(fund_code, cost)
    return jsonify(res)


@bp.route('/fund_update_once')
@auth.login_required
def fund_update_once():
    fund_order.fund_update_once()
    return jsonify({'res': 'ok'})


@bp.route('/getfundnow', methods=['POST'])
@auth.login_required
def getfundnow():
    json_data = request.get_json(force=True)
    click_fund_code = json_data['click_fund_code']
    res = fund_collector.collect_fund_net_estimate(click_fund_code)
    return jsonify(res)


@bp.route('/collect_all_fund_net')
@auth.login_required
def collect_all_fund_net():
    res = fund_collector.collect_all_fund_net_estimate()
    return jsonify(res)


@bp.route('/get_fund_estimate_data')
@auth.login_required
def get_fund_estimate_data():
    res = fund_estimate.get_fund_estimate_data()
    return jsonify(res)


@bp.route('/getestimatebuydata', methods=['POST'])
@auth.login_required
def getestimatebuydata():
    json_data = request.get_json(force=True)
    fund_code = json_data.get('fund_code')
    esd, y, x = fund_estimate.getestimatebuydata(fund_code=fund_code)
    return jsonify({"data": esd, "y": y, "x": x})


@bp.route('/get_fund_review', methods=['POST'])
@auth.login_required
def get_fund_review():
    json_data = request.get_json(force=True)
    reviewform = json_data['reviewform']
    oneday = json_data['reviewform']
    temp = fund_total.get_fund_review(reviewform, getoneday=oneday)
    if temp:
        return jsonify({"response_code": 200, 'res': temp})
    else:
        return jsonify({"response_code": 404, "res": temp})


@bp.route('/add_fund_review', methods=['POST'])
@auth.login_required
def add_fund_review():
    json_data = request.get_json(force=True)
    reviewform = json_data['reviewform']
    if "isupdate" in json_data.keys():
        temp = fund_total.add_fund_review(reviewform, True)
        return jsonify({'res': temp})
    temp = fund_total.add_fund_review(reviewform)
    return jsonify({'res': temp})


@bp.route('/get_fund_total_data')
@auth.login_required
def get_fund_total_data():
    temp = fund_total.get_fund_total_data()
    return jsonify({'data': temp})


@bp.route('/getfryfundtable')
@auth.login_required
def getfryfundtable():
    temp = fund_total.get_fund_total_data(getfry=True)
    return jsonify({'data': temp})


@bp.route('/getfundalldata')
@auth.login_required
def getfundalldata():
    temp = fund_total.get_fund_total_data(getall=True)
    return jsonify({'data': temp})


@bp.route('/get_fund_remain_chart_data', methods=['POST'])
@auth.login_required
def get_fund_remain_chart_data():
    json_data = request.get_json(force=True)
    fund_code = json_data['fund_code']
    res = fund_total.get_fund_remain_chart_data(fund_code)
    return jsonify(res)


@bp.route('/get_fund_total_chart_data', methods=['POST'])
@auth.login_required
def get_fund_total_chart_data():
    json_data = request.get_json(force=True)
    fund_code = json_data['fund_code']
    res = fund_total.get_fund_total_chart_data(fund_code)
    return jsonify(res)


@bp.route('/get_fund_treemap_label')
@auth.login_required
def get_fund_treemap_label():
    res = fund_total.get_fund_treemap_label()
    return jsonify(res)


@bp.route('/getfundcalendar', methods=['POST'])
@auth.login_required
def getfundcalendar():
    json_data = request.get_json(force=True)

    mode = json_data['mode']
    if mode == 'fund':
        fund_code = json_data['fund_code']
        temp, temp1, sform = fund_review.getfundcalendar(fund_code)
        return jsonify({'data': temp, 'bs_data': temp1, "sform": sform})
    if mode == 'author':
        temp, temp1, sform = fund_review.getfundcalendarbyauthor()
        return jsonify({'data': temp, 'bs_data': temp1, "sform": sform})


@bp.route('/getreviewtabledata', methods=['POST'])
@auth.login_required
def getreviewtabledata():
    json_data = request.get_json(force=True)
    fund_code = json_data['fund_code']
    fund_review_time = json_data['fund_review_time']
    temp = fund_review.getreviewtabledata(fund_code, fund_review_time)
    return jsonify(temp)


@bp.route('/getfunder')
@auth.login_required
def getfunder():
    temp, temp_list = fund_review.getfunder()
    return jsonify({"data": temp, "data_list": temp_list})


@bp.route('/getfundlabel')
@auth.login_required
def getfundlabel():
    temp = fund_review.getfundlabel()
    return jsonify(temp)


@bp.route('/commitfunderreview', methods=['POST'])
@auth.login_required
def commitfunderreview():
    json_data = request.get_json(force=True)
    funderreviewform = json_data['funderreviewform']
    temp = fund_review.commitfunderreview(funderreviewform)
    return jsonify({'res': temp})


@bp.route('/getfunderreview', methods=['POST'])
@auth.login_required
def getfunderreview():
    json_data = request.get_json(force=True)
    funder_id = json_data['funder_id']
    temp = fund_review.getfunderreview(funder_id)
    return jsonify(temp)


@bp.route('/get_fund_base_label')
@auth.login_required
def get_fund_base_label():
    temp, temp_option = fund_setting.get_fund_base_label()
    return jsonify({"data": temp, "data_option": temp_option})


@bp.route('/get_fund_customer_label_option')
@auth.login_required
def get_fund_customer_label_option():
    option_data, table_data = fund_setting.get_fund_customer_label_option()
    temp = {'option_data': option_data, 'table_data': table_data}
    return jsonify(temp)


@bp.route('/delete_fund_customer_label', methods=['POST'])
@auth.login_required
def delete_fund_customer_label():
    json_data = request.get_json(force=True)
    fund_label_id = json_data['fund_operation_label_id']
    fund_setting.delete_fund_customer_label(fund_label_id)
    return jsonify({"data": "ok"})


@bp.route('/add_fund_customer_label', methods=['POST'])
@auth.login_required
def add_fund_customer_label():
    json_data = request.get_json(force=True)
    fund_customer_label_selected = json_data['fund_customer_label_selected']
    fund_customer_fund_selected = json_data['fund_customer_fund_selected']
    temp = fund_setting.add_fund_customer_label(
        fund_customer_label_selected, fund_customer_fund_selected)
    return jsonify({'res': temp})


@bp.route('/add_fund_author', methods=['POST'])
@auth.login_required
def add_fund_author():
    json_data = request.get_json(force=True)
    new_author = json_data['new_author']
    apps_selected = json_data['apps_selected']
    isfirm = json_data['isfirm']
    temp = fund_setting.add_fund_author(new_author, apps_selected, isfirm)
    return jsonify(temp)


@bp.route('/add_new_label', methods=['POST'])
@auth.login_required
def add_new_label():
    json_data = request.get_json(force=True)
    new_fund_label = json_data['new_fund_label']
    temp = fund_setting.add_new_label(new_fund_label)
    return jsonify({'res': temp})


@bp.route('/add_fund_label', methods=['POST'])
@auth.login_required
def add_fund_label():
    json_data = request.get_json(force=True)
    fund_base_label_selected = json_data['fund_base_label_selected']
    fund_had_code_selected = json_data['fund_had_code_selected']
    temp = fund_setting.add_fund_label(
        fund_base_label_selected, fund_had_code_selected)
    return jsonify({'res': temp})


@bp.route('/get_fund_base_label_data')
@auth.login_required
def get_fund_base_label_data():
    temp = fund_setting.get_fund_base_label_data()
    return jsonify(temp)


@bp.route('/get_author_app_option')
@auth.login_required
def get_author_app_option():
    temp = fund_setting.get_author_app_option()
    return jsonify(temp)


@bp.route('/get_fund_author_data')
@auth.login_required
def get_fund_author_data():
    temp = fund_setting.get_fund_author_data()
    return jsonify(temp)
