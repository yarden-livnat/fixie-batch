"""Tornado handlers for interfacing with fixie batch execution."""
from fixie import RequestHandler

from fixie_batch.environ import QUEUE_STATUSES
from fixie_batch.simulations import spawn, cancel, query


class Spawn(RequestHandler):

    schema = {'simulation': {'anyof_type': ['dict', 'string'], 'required': True},
              'user': {'type': 'string', 'empty': False, 'required': True},
              'token': {'type': 'string', 'regex': '[0-9a-fA-F]+', 'required': True},
              'name': {'type': 'string'},
              'path': {'type': 'string'},
              'project': {'type': 'string'},
              'permissions': {'anyof': [
                {'type': 'string', 'allowed': ['public', 'private']},
                {'type': 'list', 'schema': {'type': 'string'}},
                ]},
              'post': {'type': 'list'},
              'notify': {'type': 'list'},
              'interactive': {'type': 'boolean'},
              }
    response_keys = ('jobid', 'status', 'message')

    def post(self):
        resp = spawn(**self.request.arguments)
        response = dict(zip(self.response_keys, resp))
        self.write(response)


class Cancel(RequestHandler):

    schema = {'job': {'anyof_type': ['integer', 'string'], 'required': True},
              'user': {'type': 'string', 'empty': False, 'required': True},
              'token': {'type': 'string', 'regex': '[0-9a-fA-F]+', 'required': True},
              'project': {'type': 'string'},
              }
    response_keys = ('jobid', 'status', 'message')

    def post(self):
        resp = cancel(**self.request.arguments)
        response = dict(zip(self.response_keys, resp))
        self.write(response)


ALLOWED_STATUSES = ['all'] + list(QUEUE_STATUSES)


class Query(RequestHandler):

    schema = {'statuses': {'anyof': [
                {'type': 'string', 'allowed': ALLOWED_STATUSES},
                {'type': 'list', 'empty': False,
                 'schema': {'type': 'string', 'allowed': ALLOWED_STATUSES}},
                ]},
              'users': {'anyof': [
                {'type': 'string', 'empty': False},
                {'type': 'list', 'empty': False,
                 'schema': {'type': 'string', 'empty': False}},
                ], 'nullable': True},
              'jobs': {'anyof': [
                {'type': 'integer'},
                {'type': 'string'},
                {'type': 'list', 'empty': False,
                 'schema': {'anyof_type': ['integer', 'string']}},
                ], 'nullable': True},
              'projects': {'anyof': [
                {'type': 'string'},
                {'type': 'list', 'empty': False, 'schema': {'type': 'string'}},
                ], 'nullable': True},
              }
    response_keys = ('data', 'status', 'message')

    def post(self):
        resp = query(**self.request.arguments)
        response = dict(zip(self.response_keys, resp))
        self.write(response)


HANDLERS = [
    ('/spawn', Spawn),
    ('/cancel', Cancel),
    ('/query', Query),
]
