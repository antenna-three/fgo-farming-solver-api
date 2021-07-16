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
    try:
        body = solve(params)
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
            'body': json.dumps(body, ensure_ascii=False)
        }

def solve(params):
    items, quests, drop_rates = get_data('items', 'quests', 'drop_rates')
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
    fields, quest_fields, item_fields, objective, param_items, param_quests = get_params(
        params,
        fields='list',
        quest_fields='list',
        item_fields='list',
        objective=['ap', 'lap'],
        items='dict',
        quests='list',
    )
    try:
        param_items = dict([(item, int(count)) for item, count in param_items])
    except ValueError:
        raise ParamError(
            message='Numbers of items must be positive integers',
            invalid_params={
                'name': 'item',
                'reason': 'must be like "string:integer,string:integer,..."'
            }
        )

    if all(len(k) == len(str.encode(k, 'utf-8')) for k in param_items.keys()):
        key = 'id'
        get_area = lambda quest, key: quest['id'][:2]
        get_section = lambda quest, key: quest['id'][0]
    else:
        key = 'name'
        get_area = lambda quest, key: quest['area']
        get_section = lambda quest, key: quest['section']

    if param_quests:
        quests = [
            quest for quest in quests
            if quest[key] in param_quests
            or get_area(quest, key) in param_quests
            or get_section(quest, key) in param_quests
        ]

    problem = pulp.LpProblem(sense=pulp.LpMinimize)
    quest_lap_variables = {(quest:=row[key]): pulp.LpVariable(quest, lowBound=0) for row in quests}
    if objective == 'lap':
        problem.setObjective(pulp.lpSum(quest_lap_variables.values()))
    elif objective == 'ap':
        problem.setObjective(pulp.lpSum(int(row['ap']) * quest_lap_variables[row[key]] for row in quests))
    ig = itemgetter('item_' + key)
    quest_keys = [quest[key] for quest in quests]
    item_count_expressions = {
        item: pulp.LpAffineExpression(
            {
                quest_lap_variables[quest]: float(row['drop_rate'])
                for row in group
                if (quest:=row['quest_' + key]) in quest_keys
            },
            name=item
        )
        for item, group in groupby(sorted(drop_rates, key=ig), key=ig)
    }
    for item, expression in item_count_expressions.items():
        if item in param_items:
            problem.addConstraint(pulp.LpConstraint(expression, pulp.LpConstraintGE, rhs=param_items[item]))
        #problem += pulp.lpSum(drop_rate * runs[quest] for quest, drop_rate in quests_to_drop_rates.items()) >= params[item]

    problem.solve()

    if quest_fields:
        quest_to_info = {quest[key]: quest for quest in quests}
    if item_fields:
        item_to_info = {item[key]: item for item in items}

    result = {}
    if 'quests' in fields:
        result['quests'] = [
            dict(
                **{
                    key: quest,
                    'lap': math.ceil(lap)
                },
                **{k: quest_to_info[quest].get(k, '') for k in quest_fields}
            )
            for quest, lap_variable in quest_lap_variables.items()
            if (lap:=pulp.value(lap_variable)) > 0
        ]
    if 'items' in fields:
        result['items'] = [
            dict(
                **{
                    key: item,
                    'count': round(count)
                },
                **{k: item_to_info[item].get(k, '') for k in item_fields}
            )
            for item, expression in item_count_expressions.items()
            if (count:=pulp.value(expression)) > 0
        ]
    return result

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

def get_params(params, **keys):
    values = []
    for key, type_ in keys.items():
        value = params.get(key)
        if type(type_) is list and value not in type_:
            raise ParamError(
                title=f'Specify {value}',
                invalid_params=[{
                    'name': value,
                    'reason': f'must be {" or ".join(type_)}'
                }]
            )
        elif type_ == 'list' and value:
            value = value.split(',')
        elif type_ == 'dict' and value:
            value = dict(item.split(':') for item in value.split(','))
        values.append(value)
    return values

class ParamError(Exception):
    def __init__(self, **body):
        self.body = body

