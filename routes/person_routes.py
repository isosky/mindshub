#!/usr/bin/python
# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request
from extensions import auth
from module import person, task

bp = Blueprint('person', __name__, url_prefix='')


@bp.route('/getpersonoptions')
@auth.login_required
def getpersonoptions():
    res = person.getpersonoptions()
    return jsonify(res)


@bp.route('/getperson')
@auth.login_required
def getperson():
    res = person.get_person()
    return jsonify(res)


@bp.route('/getperson_option')
@auth.login_required
def getperson_option():
    res = person.get_person_count()
    return jsonify(res)


@bp.route('/addperson', methods=['POST'])
@auth.login_required
def addperson():
    json_data = request.get_json(force=True)
    company = json_data['company']
    department = json_data['department']
    person_name = json_data['person_name']
    post = json_data['post']
    force = json_data['force']
    temp = person.add_person(company, department, person_name, post, force)
    return jsonify(temp)


@bp.route('/getperson_data', methods=['POST'])
@auth.login_required
def getperson_data():
    json_data = request.get_json(force=True)
    task_id = json_data['task_id']
    res = task.get_person_by_task_id(task_id)
    return jsonify(res)
