import json

import pytest

from fgo_farming_solver import main


@pytest.fixture()
def apigw_event():
    """ Generates API GW Event"""
    with open('events/event.json', 'r', encoding='utf-8') as f:
        return json.load(f)


def test_lambda_handler(apigw_event, mocker):
    ret = main.handler(apigw_event, "")
    data = json.loads(ret["body"])

    assert ret["statusCode"] == 200
    assert "message" in ret["body"]
    assert data["message"] == "hello world"
    # assert "location" in data.dict_keys()


@pytest.fixture()
def query():
    return {
        "fields": "items,quests,id",
        "objective": "ap",
        "items": "00:100,01:100,02:100,03:100,08:100",
        "ap_coefficients": "0:0.5",
        "drop_merge_method": "add"
    }

def params():
    return {
        "fields": ["items", "quests", "id"],
        "objective": "ap",
        "items": {"00": "100", "01": "100", "02": "100", "03": "100", "08": "100"},
        "ap_coefficients": {"0": "0.5"},
        "drop_merge_method": "add"
    }

def test_decode_params(qurty, params):
    decoded = main.decode_params(query)
    assert decoded == params


def test_validate_params(params):
    main.validate_params(params, objective=('ap', 'lap'), drop_merge_method=('add', '1', '2'))
    with pytest.raises(main.ParamError):
        main.validate_params(params, objective=('ap', 'lap'), drop_merge_method=('1', '2'))


