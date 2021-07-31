import csv
import json
from itertools import groupby
from operator import itemgetter
import pulp
import math
from io import StringIO
import boto3
import traceback

def handler(event, context):
    params = event.get('queryStringParameters')
    if params is None:
        return {
            'usage': {
                'fields': ['quests', 'items'],
                'quest_fields': list(quests[0].keys()),
                'item_fields': list(items[0].keys()),
                'objective': "'ap' or 'qp'",
                'items': {item['item']: 0 for item in items},
                'quests': [quest['quest'] for quest in quests]
            }
        }
    params = decode_params(
        params,
        fields='list',
        quest_fields='list',
        item_fields='list',
        objective=['ap', 'lap'],
        items='dict',
        quests='list',
    )
    items, quests, drop_rates = get_data('items', 'quests', 'drop_rates')
    try:
        key = get_key(params['items'])
        param_items = format_param_items(items, params['items'])
        if params['quests']:
            quests = filter_quests(quests, params['quests'], key)
        quest_keys = [quest[key] for quest in quests]
        drop_rates = filter_drop_rates(drop_rates, param_items, quest_keys, key)
        item_counts, quest_laps = solve(params['objective'], param_items, quests, drop_rates, key)
        item_counts = format_value(item_counts)
        quest_laps = format_value(quest_laps)
        result = format_result(item_counts, quest_laps, params, items, quests, key)
    except ParamError as e:
        return {
            'statusCode': 400,
            'body': json.dumps(e.body)
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'message': traceback.format_exc()})
        }
    else:
        return {
            'statusCode': 200,
            'body': json.dumps(result, ensure_ascii=False)
        }

def decode_params(params, **keys):
    values = {}
    for key, type_ in keys.items():
        value = params.get(key)
        if type_ is list and value not in type_:
            raise ParamError(
                title=f'Specify {value}',
                invalid_params=[{
                    'name': value,
                    'reason': f'must be {" or ".join(type_)}'
                }]
            )
        elif type_ == 'list':
            if value:
                value = value.split(',')
            else:
                value = []
        elif type_ == 'dict':
            if value:
                value = dict(item.split(':') for item in value.split(','))
            else:
                value = {}
        values[key] = value
    return values

def get_data(*keys):
    s3 = boto3.resource('s3')
    data = []
    for key in keys:
        obj = s3.Object('fgodrop', key + '.csv')
        response = obj.get()
        body = response['Body'].read()
        with StringIO(body.decode('utf-8'), newline='') as s:
            reader = csv.DictReader(s)
            data.append(list(reader))
    return data

def get_key(param_items):
    if all(len(k) == len(str.encode(k, 'utf-8')) for k in param_items.keys()):
        return 'id'
    else:
        return 'name'

def format_param_items(items, param_items):
    try:
        param_items = {item: int(count) for item, count in param_items.items()}
    except ValueError:
        raise ParamError(
            message='Numbers of items must be positive integers',
            invalid_params={
                'name': 'item',
                'reason': 'must be like "string:integer,string:integer,..."'
            }
        )
    return param_items

def filter_quests(quests, param_quests, key):
    if key == 'id':
        get_area = lambda quest: quest['id'][:2]
        get_section = lambda quest: quest['id'][0]
    else:
        get_area = lambda quest: quest['area']
        get_section = lambda quest: quest['section']

    quests = [
        quest for quest in quests
        if quest[key] in param_quests
        or get_area(quest) in param_quests
        or get_section(quest) in param_quests
    ]
    return quests

def filter_drop_rates(drop_rates, items, quests, key):
    return [
        row for row in drop_rates
        if row['item_' + key] in items 
        and row['quest_' + key] in quests
    ]

def solve(objective, items, quests, drop_rates, key):
    quest_keys = [quest[key] for quest in quests]
    problem = pulp.LpProblem(sense=pulp.LpMinimize)
    quest_lap_variables = pulp.LpVariable.dicts('lap', quest_keys, lowBound=0)
    if objective == 'lap':
        problem.setObjective(pulp.lpSum(quest_lap_variables.values()))
    elif objective == 'ap':
        problem.setObjective(pulp.lpSum(int(quest['ap']) * quest_lap_variables[quest['id']] for quest in quests))
    ig = itemgetter('item_' + key)
    item_count_expressions = {
        item: pulp.LpAffineExpression(
            {
                quest_lap_variables[row['quest_' + key]]: float(row['drop_rate'])
                for row in group
            },
            name=item
        )
        for item, group in groupby(sorted(drop_rates, key=ig), key=ig)
    }
    for item, expression in item_count_expressions.items():
        problem.addConstraint(pulp.LpConstraint(expression, pulp.LpConstraintGE, rhs=items[item]))

    problem.solve()

    return item_count_expressions, quest_lap_variables

def format_value(variables):
    return {
        key: value
        for key, variable in variables.items()
        if (value:=pulp.value(variable)) > 0
    }

def format_result(item_counts, quest_laps, params, items, quests, key):
    if not params['fields']:
        params['fields'] = ['quests', 'items']
    if params['quest_fields']:
        quest_to_info = {quest[key]: quest for quest in quests}
    if params['item_fields']:
        item_to_info = {item[key]: item for item in items}

    result = {}
    if 'quests' in params['fields']:
        result['quests'] = [
            dict(
                **{
                    key: quest,
                    'lap': math.ceil(lap)
                },
                **{k: quest_to_info[quest].get(k, '') for k in params['quest_fields']}
            )
            for quest, lap in quest_laps.items()
        ]
    if 'items' in params['fields']:
        result['items'] = [
            dict(
                **{
                    key: item,
                    'count': round(count)
                },
                **{k: item_to_info[item].get(k, '') for k in params['item_fields']}
            )
            for item, count in item_counts.items()
        ]
    return result

class ParamError(Exception):
    def __init__(self, **body):
        self.body = body

