import os
import boto3
import gzip
import json
import math
from pathlib import Path
from time import time
from decimal import Decimal
from itertools import groupby
from operator import itemgetter


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
        fields=[],
        quest_fields=[],
        item_fields=[],
        objective='ap',
        items={},
        quests=[],
        ap_coefficients={},
        drop_merge_method='add'
    )
    try:
        validate_params(params, objective=('ap', 'lap'), drop_merge_method=('add', '1', '2'))
        params = format_params(params, items=int, ap_coefficients=float)
    except ParamError as e:
        return {
            'statusCode': 400,
            'body': json.dumps(e.body)
        }

    data = get_data()
    items, quests, drop_rates = data['items'], data['quests'], data['drop_rates']

    quests = filter_quests(quests, params['quests'], params['ap_coefficients'])
    drop_rates = filter_drop_rates(drop_rates, quests)
    drop_rates = merge_drop_rates(drop_rates, quests, params['drop_merge_method'])

    item_counts, quest_laps = solve(params['objective'], params['items'], quests, drop_rates)
    result = format_result(item_counts, quest_laps, items, quests, drop_rates, params)
    if 'id' in params['fields']:
        result['id'] = context.aws_request_id
        result['unix_time'] = int(time())
        put_dynamodb(result)
    result = filter_result(result, params)
    
    return {
        'statusCode': 200,
        'body': json.dumps(result, ensure_ascii=False)
    }


def decode_params(params, **keys):
    values = {}
    for key, default in keys.items():
        value = params.get(key)
        if not value:
            value = default
        elif default == []:
            value = value.split(',')
        elif default == {}:
            value = dict(item.split(':') for item in value.split(','))
        values[key] = value
    return values


def validate_params(params, **values):
    for key, choices in values.items():
        if (value:=params.get(key, '')) not in choices:
            raise ParamError(
                message=key + ' is invalid',
                params=params,
                invalid_params={
                    'name': key,
                    'value': value,
                    'reason': 'must be ' + ' or '.join(choices)
                }
            )


def format_params(params, **formatters):
    params = params.copy()
    for key, formatter in formatters.items():
        try:
            params[key] = {k: formatter(v) for k, v in params[key].items()}
        except ValueError:
            raise ParamError(
                message=f'Numbers of {key} is invalid',
                params=params,
                invalid_params={
                    'name': key,
                    'reason': f'must be like "string:number,string:number,..."'
                }
            )
    return params


s3 = None
obj = None

def get_data():
    global s3, obj
    s3 = s3 or boto3.resource('s3')
    bucket_name = os.getenv('BUCKET_NAME')
    key = 'all.json.gz'
    obj = obj or s3.Object(bucket_name, key)
    gz_path = Path('/tmp/' + key)
    if not gz_path.exists():
        obj.download_file(str(gz_path))
    with gzip.open(gz_path, 'rt', encoding='utf-8') as f:
        return json.load(f)


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


def filter_drop_rates(drop_rates, quests):
    quest_ids = [row['id'] for row in quests]
    return [
        row for row in drop_rates
        if row['quest_id'] in quest_ids
    ]


def merge_drop_rates(drop_rates, quests, drop_merge_method):
    drop_rates = drop_rates.copy()
    if drop_merge_method == 'add':
        samples_1s = {row['id']: row.get('samples_1', 0) for row in quests}
        samples_2s = {row['id']: row.get('samples_2', 0) for row in quests}
        for row in drop_rates:
            samples_1 = samples_1s.get(row['quest_id'], 0)
            samples_2 = samples_2s.get(row['quest_id'], 0)
            drop_rate_1, drop_rate_2 = row.pop('drop_rate_1', 0), row.pop('drop_rate_2', 0)
            if samples_1 or samples_2:
                try:
                    row['drop_rate'] = (drop_rate_1*samples_1 + drop_rate_2*samples_2) / (samples_1 + samples_2)
                except TypeError:
                    print(f'{drop_rate_1=}, {drop_rate_2=}, {samples_1=}, {samples_2=}')
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


def solve(objective, param_items, quests, drop_rates):
    import pulp

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
                quest_lap_variables[row['quest_id']]: row['drop_rate']
                for row in rows
            },
            name=item
        )
        for item, rows in groupby(sorted(drop_rates, key=ig), key=ig)
    }
    for item, expression in item_count_expressions.items():
        if item in param_items:
            problem.addConstraint(pulp.LpConstraint(expression, pulp.LpConstraintGE, rhs=param_items[item]))

    problem.solve()

    def format_value(variables):
        return {
            key: value
            for key, variable in variables.items()
            if (value:=pulp.value(variable)) > 0
        }

    item_counts = format_value(item_count_expressions)
    quest_laps = format_value(quest_lap_variables)

    return item_counts, quest_laps


def format_result(item_counts, quest_laps, items, quests, drop_rates, params):
    quest_to_info = {quest['id']: quest for quest in quests}
    item_to_info = {item['id']: item for item in items}

    result = {
        'params': {
            k: v for k, v in params.items()
            if v and 'fields' not in k
        },
        'quests': (quests:= [
            {
                **quest_to_info[quest],
                'lap': round(lap),
            }
            for quest, lap in quest_laps.items()
        ]),
        'items': [
            {
                **item_to_info[item],
                'count': round(count),
            }
            for item, count in item_counts.items()
        ],
        'drop_rates': [
            row for row in drop_rates
            if row['quest_id'] in quest_laps
        ],
        'total_lap': sum(quest['lap'] for quest in quests),
        'total_ap': sum(quest['ap'] * quest['lap'] for quest in quests),
    }
    return result


dynamodb = None
table = None

def put_dynamodb(result):
    global dynamodb, table
    endpoint_url = os.getenv('DYNAMODB_ENDPOINT')
    dynamodb = dynamodb or boto3.resource('dynamodb', endpoint_url=endpoint_url)
    table_name = os.getenv('TABLE_NAME')
    table = table or dynamodb.Table(table_name)
    item = result.copy()
    item['params']['ap_coefficients'] = {k: Decimal(str(v)) for k, v in item['params'].get('ap_coefficients', {}).items()}
    item['drop_rates'] = [
        {k: Decimal(str(round(v, 3))) if k == 'drop_rate' else v for k, v in row.items()}
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

