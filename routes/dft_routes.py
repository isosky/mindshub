#!/usr/bin/python
# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request
from extensions import auth
from module import dft

bp = Blueprint('dft', __name__, url_prefix='')


@bp.route('/getdftdata')
@auth.login_required
def getdftdata():
    temp = dft.get_dft()
    unread_count = dft.get_dft_data_unread_count()
    return jsonify({'data': temp, 'unread_count': unread_count})


@bp.route('/commitdft', methods=['POST'])
@auth.login_required
def commitdft():
    json_data = request.get_json(force=True)
    dftform = json_data['dftform']
    mode = json_data['mode']
    if mode == 'add':
        temp = dft.add_dft(dftform, isbyupdate=False)
    else:
        temp = dft.update_dft(dftform)
    return jsonify(temp)
