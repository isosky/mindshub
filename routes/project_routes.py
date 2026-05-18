#!/usr/bin/python
# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request
from extensions import auth
from module import project

bp = Blueprint('project', __name__, url_prefix='')


@bp.route('/getproject')
@auth.login_required
def getproject():
    res = project.get_project()
    return jsonify(res)


@bp.route('/get_task_by_project_id', methods=['POST'])
@auth.login_required
def get_task_by_project_id():
    json_data = request.get_json(force=True)
    project_id = json_data['project_id']
    project_task = project.get_task_by_project_id(project_id)
    project_pie_data = project.get_project_task_piechart_by_project_id(
        project_id)
    project_bar_data = project.get_project_task_barchart_by_project_id(
        project_id)
    return jsonify({"project_task_data": project_task, 'project_pie_data': project_pie_data, 'project_bar_data': project_bar_data})


@bp.route('/get_person_by_project_id', methods=['POST'])
@auth.login_required
def get_person_by_project_id():
    json_data = request.get_json(force=True)
    project_id = json_data['project_id']
    project_person = project.get_person_by_project_id(project_id)
    project_person_graph = project.get_project_person_graph_data(project_id)
    return jsonify({"project_person_data": project_person, "project_person_graph": project_person_graph})


@bp.route('/update_project_desc', methods=['POST'])
@auth.login_required
def update_project_desc():
    json_data = request.get_json(force=True)
    project_id = json_data.get('project_id')
    project_desc = json_data.get('project_desc')
    project.update_project_desc(project_id, project_desc)
    return jsonify({"msg": 'ok'})


@bp.route('/update_project_detail', methods=['POST'])
@auth.login_required
def update_project_detail():
    json_data = request.get_json(force=True)
    project_id = json_data.get('project_id')
    project_detail = project.update_project_detail(project_id)
    return jsonify(project_detail)
