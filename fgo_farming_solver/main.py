import time
import resource

records = {}

def measure(s=''):
    records[s] = resource.getrusage(resource.RUSAGE_SELF)

measure('start import')

import boto3
import tarfile
import csv
import json
import uuid
import math
from pathlib import Path
from decimal import Decimal
from itertools import groupby
from operator import itemgetter

measure('end import')


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

    items, quests, drop_rates = get_data('items', 'quests', 'drop_rates')
    quests = format_rows(quests, ap=int, bp=int, exp=int, qp=int, samples_1=int, samples_2=int)
    drop_rates = format_rows(drop_rates, drop_rate_1=float, drop_rate_2=float)

    quests = filter_quests(quests, params['quests'], params['ap_coefficients'])
    drop_rates = filter_drop_rates(drop_rates, quests)
    drop_rates = merge_drop_rates(drop_rates, quests, params['drop_merge_method'])

    item_counts, quest_laps = solve(params['objective'], params['items'], quests, drop_rates)
    result = format_result(item_counts, quest_laps, items, quests, drop_rates, params)
    measure('start put dynamodb')
    if 'id' in params['fields']:
        result['id'] = str(uuid.uuid1())
        save_to_dynamodb(result)
    measure('end put dynamodb')
    result = filter_result(result, params)

    for key, value in records.items():
        print(f'[{key}]\ntime: {value[0]}s\nmemory: {value[2]/1024.0:.1f}mb')
    
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


def get_data(*keys):
    s3 = boto3.resource('s3')
    tar_path = Path('/tmp/all.tar.gz')
    dst_path = Path('/tmp/all')
    measure('start download')
    if not tar_path.exists():
        obj = s3.Object('fgodrop', 'all.tar.gz')
        obj.download_file(str(tar_path))
    measure('end download, start extract')
    with tarfile.open(tar_path, 'r') as t:
        t.extractall(dst_path)
    measure('end extract, start read')
    data = []
    for key in keys:
        rows = []
        with open(dst_path / (key + '.csv'), 'r', encoding='utf-8') as f:
            rows = list(csv.DictReader(f))
        data.append(rows)
    measure('end read')
    return data


def format_rows(rows, **formatters):
    return [
        {
            key: formatters[key](value or 0) if key in formatters else value
            for key, value in row.items()
        }
        for row in rows
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

