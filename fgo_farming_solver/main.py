import json
from itertools import groupby
from operator import itemgetter
import pulp
import math
from io import StringIO
import uuid
import boto3
from decimal import Decimal
from pathlib import Path
import traceback

def handler(event, context):
    params = event.get('queryStringParameters')
    if params is None:
        return {
            'usage': {
                'fields': 'string,string,...',
                'quest_fields': 'string,string,...',
                'item_fields': 'string,string,...',
                'objective': "'ap' or 'qp'",
                'items': 'string:int,string:int,...',
                'quests': 'string,string,...',
                'ap_coefficients': 'string:float,string:float,...',
                'drop_merge_method': "'1' or '2' or 'add'"
            }
        }
    params = decode_params(
        params,
        fields='list',
        quest_fields='list',
        item_fields='list',
        objective='ap',
        items='dict',
        quests='list',
        ap_coefficients='dict',
        drop_merge_method='add'
    )
    data = get_data()
    items = data['items']
    quests = data['quests']
    drop_rates = data['drop_rates']
    try:
        params['items'] = format_param_items(params['items'])
        params['ap_coefficients'] = format_ap_coefficients(params['ap_coefficients'])
        quests = format_quests(quests)
        drop_rates = merge_drop_rates(drop_rates, quests, params['drop_merge_method'])
        quests = filter_quests(quests, params['quests'], params['ap_coefficients'])
        quest_ids = [quest['id'] for quest in quests]
        drop_rates = filter_drop_rates(drop_rates, params['items'], quest_ids)
        item_counts, quest_laps = solve(params['objective'], params['items'], items, quests, drop_rates)
        item_counts = format_value(item_counts)
        quest_laps = format_value(quest_laps)
        result = format_result(item_counts, quest_laps, items, quests, drop_rates, params)
        if 'id' in params['fields']:
            uuid_ = uuid.uuid1()
            result['id'] = str(uuid_)
            save_to_dynamodb(result)
        result = filter_result(result, params)
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
        if type_ == 'list':
            if value:
                value = value.split(',')
            else:
                value = []
        elif type_ == 'dict':
            if value:
                value = dict(item.split(':') for item in value.split(','))
            else:
                value = {}
        else:
            if not value:
                value = type_
        values[key] = value
    return values

def get_data():
    s3 = boto3.resource('s3')
    path = Path('/tmp/all.json')
    if not path.exists():
        obj = s3.Object('fgodrop', 'all.json')
        obj.download_file(str(path))
    with path.open('r', encoding='utf-8') as f:
        data = json.load(f)
    return data

def get_key(param_items):
    if all(len(k) == len(str.encode(k, 'utf-8')) for k in param_items.keys()):
        return 'id'
    else:
        return 'name'

def format_param_items(param_items):
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

def format_ap_coefficients(ap_coefficients):
    try:
        ap_coefficients = {quest: float(ap_coefficient) for quest, ap_coefficient in ap_coefficients.items()}
    except ValueError:
        raise ParamError(
            message='Numbers of ap_coefficients must be positive floats',
            invalid_params={
                'name': 'ap_coefficient',
                'reason': 'must be like "string:float,string:float,..."'
            }
        )
    return ap_coefficients

def format_quests(quests):
    keys = ('section', 'area', 'name', 'id')
    return [
        {
            key: value if key in keys else int(value) if value else None
            for key, value in quest.items()
        }
        for quest in quests
    ]

def filter_quests(quests, param_quests, ap_coefficients):
    get_area = lambda quest: quest['id'][:2]
    get_section = lambda quest: quest['id'][0]

    if param_quests:
        quests = [
            quest for quest in quests
            if quest['id'] in param_quests
            or get_area(quest) in param_quests
            or get_section(quest) in param_quests
        ]
    for quest in quests:
        ap_coefficient = (
            ap_coefficients.get(quest['id'])
            or ap_coefficients.get(get_area(quest))
            or ap_coefficients.get(get_section(quest))
            or 1
        )
        quest['ap'] = math.floor(quest['ap'] * ap_coefficient)
    return quests

def merge_drop_rates(drop_rates, quests, drop_merge_method):
    drop_rates = drop_rates.copy()
    if drop_merge_method == 'add':
        samples_1s = {row['id']: row['samples_1'] for row in quests}
        samples_2s = {row['id']: row['samples_2'] for row in quests}
        for row in drop_rates:
            samples_1 = samples_1s[row['quest_id']] or 0
            samples_2 = samples_2s[row['quest_id']] or 0
            drop_rate_1, drop_rate_2 = row.pop('drop_rate_1', 0), row.pop('drop_rate_2', 0)
            if samples_1 or samples_2:
                row['drop_rate'] = (drop_rate_1*samples_1 + drop_rate_2*samples_2) / (samples_1 + samples_2)
            else:
                row['drop_rate'] = 0
    else:
        primary = drop_merge_method
        secondary = '1' if primary == '2' else '2'
        for row in drop_rates:
            drop_rate_primary = row.pop('drop_rate_' + primary, 0)
            drop_rate_secondary = row.pop('drop_rate_' + secondary, 0)
            row['drop_rate'] = drop_rate_primary or drop_rate_secondary
    return drop_rates


def filter_drop_rates(drop_rates, items, quests):
    return [
        row for row in drop_rates
        if row['quest_id'] in quests
    ]

def solve(objective, param_items, items, quests, drop_rates):
    quest_ids = [quest['id'] for quest in quests]
    problem = pulp.LpProblem(sense=pulp.LpMinimize)
    quest_lap_variables = pulp.LpVariable.dicts('lap', quest_ids, lowBound=0)
    if objective == 'lap':
        problem.setObjective(pulp.lpSum(quest_lap_variables.values()))
    elif objective == 'ap':
        problem.setObjective(pulp.lpSum(quest['ap'] * quest_lap_variables[quest['id']] for quest in quests))
    ig = itemgetter('item_id')
    item_count_expressions = {
        item: pulp.LpAffineExpression(
            {
                quest_lap_variables[row['quest_id']]: float(row['drop_rate'])
                for row in group
            },
            name=item
        )
        for item, group in groupby(sorted(drop_rates, key=ig), key=ig)
    }
    for item, expression in item_count_expressions.items():
        if item in param_items:
            problem.addConstraint(pulp.LpConstraint(expression, pulp.LpConstraintGE, rhs=param_items[item]))

    problem.solve()

    return item_count_expressions, quest_lap_variables

def format_value(variables):
    return {
        key: value
        for key, variable in variables.items()
        if (value:=pulp.value(variable)) > 0
    }

def format_result(item_counts, quest_laps, items, quests, drop_rates, params):
    quest_to_info = {quest['id']: quest for quest in quests}
    item_to_info = {item['id']: item for item in items}

    result = {
        'params': {
            k: v for k, v in params.items()
            if v and 'fields' not in k
        },
        'quests': [
            {
                **{
                    'id': quest,
                    'lap': math.ceil(lap)
                },
                **quest_to_info[quest],
            }
            for quest, lap in quest_laps.items()
        ],
        'items': [
            {
                **{
                    'id': item,
                    'count': round(count)
                },
                **item_to_info[item],
            }
            for item, count in item_counts.items()
        ],
        'drop_rates': [
            row for row in drop_rates
            if row['quest_id'] in quest_laps
        ],
        'total_lap': sum(math.ceil(lap) for lap in quest_laps.values()),
        'total_ap': sum(int(quest_to_info[quest]['ap'] * lap) for quest, lap in quest_laps.items())
    }
    return result

def save_to_dynamodb(result):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('fgo-farming-solver-results')
    item = result.copy()
    item['params']['ap_coefficients'] = {k: Decimal(v) for k, v in item['params']['ap_coefficients'].items()}
    item['drop_rates'] = [
        {k: Decimal(str(v)) if k == 'drop_rate' else v for k, v in row.items()}
        for row in item['drop_rates']
    ]
    table.put_item(Item=item)

def filter_result(result, params):
    if params['fields']:
        if params['quest_fields']:
            result['quests'] = {k: v for k, v in result['quests'].items() if k in params['quest_fields']}
        if params['item_fields']:
            result['items'] = {k: v for k, v in result['items'].items() if k in params['item_fields']}
        result = {k: v for k, v in result.items() if k in params['fields']}
    return result

class ParamError(Exception):
    def __init__(self, **body):
        self.body = body

