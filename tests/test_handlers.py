"""Tests handlers object."""
import pytest
import tornado.web
from tornado.httpclient import HTTPError
from fixie import json
from fixie import fetch

from fixie_batch.handlers import HANDLERS


SIMULATION = {
 'simulation': {
  'archetypes': {
   'spec': [
    {'lib': 'agents', 'name': 'Sink'},
    {'lib': 'agents', 'name': 'NullRegion'},
    {'lib': 'agents', 'name': 'NullInst'},
   ],
  },
  'control': {
   'duration': 2,
   'startmonth': 1,
   'startyear': 2000,
  },
  'facility': {
   'config': {'Sink': {'capacity': '1.00', 'in_commods': {'val': 'commodity'}}},
   'name': 'Sink',
  },
  'recipe': {
   'basis': 'mass',
   'name': 'commod_recipe',
   'nuclide': {'comp': '1', 'id': 'H1'},
  },
  'region': {
   'config': {'NullRegion': None},
   'institution': {
    'config': {'NullInst': None},
    'initialfacilitylist': {'entry': {'number': '1', 'prototype': 'Sink'}},
    'name': 'SingleInstitution',
   },
   'name': 'SingleRegion',
  },
 },
}
APP = tornado.web.Application(HANDLERS)


@pytest.fixture
def app():
    return APP


@pytest.mark.gen_test
def test_spawn_valid(xdg, verify_user, http_client, base_url):
    url = base_url + '/spawn'
    body = {"user": "inigo", "token": "42", 'simulation': SIMULATION}
    exp = {'jobid': 0, 'status': True, 'message': 'Simulation spawned'}
    obs = yield fetch(url, body)
    assert exp == obs
