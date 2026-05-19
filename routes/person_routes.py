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


@bp.route('/getscatterdata', methods=['POST'])
@auth.login_required
def getscatterdata():
    json_data = request.get_json(force=True)
    type = json_data.get('type')
    sub_type = json_data.get('sub_type')
    person_id = json_data.get('person_id')
    return jsonify(person.get_scatter_data_from_task(type, sub_type, person_id))


@bp.route('/updateperson', methods=['POST'])
@auth.login_required
def updateperson():
    json_data = request.get_json(force=True)
    company = json_data['company']
    department = json_data['department']
    person_name = json_data['person_name']
    post = json_data['post']
    person_id = json_data['person_id']
    temp = person.update_person(
        company, department, person_name, post, person_id)
    return jsonify(temp)


@bp.route('/getpersonprofile', methods=['POST'])
@auth.login_required
def getpersonprofile():
    json_data = request.get_json(force=True)
    person_profile_id = json_data['person_profile_id']
    temp = person.get_person_profile(person_profile_id)
    return jsonify(temp)


@bp.route('/getpersontask', methods=['POST'])
@auth.login_required
def getpersontask():
    json_data = request.get_json(force=True)
    person_profile_id = json_data['person_profile_id']
    temp = person.get_person_task(person_profile_id)
    return jsonify(temp)


@bp.route('/addpersonprofile', methods=['POST'])
@auth.login_required
def addpersonprofile():
    json_data = request.get_json(force=True)
    person_profile_start = json_data['person_profile_start']
    person_profile_end = json_data['person_profile_end']
    person_profile_company = json_data['person_profile_company']
    person_profile_department = json_data['person_profile_department']
    person_profile_post = json_data['person_profile_post']
    person_profile_id = json_data['person_profile_id']
    person_profile_name = json_data['person_profile_name']
    temp = person.add_person_profile(person_profile_start, person_profile_end, person_profile_company,
                                     person_profile_department, person_profile_post, person_profile_id, person_profile_name)
    return jsonify(temp)
