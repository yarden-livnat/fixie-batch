"""Tornado handlers for interfacing with fixie batch execution."""
from fixie import RequestHandler
import fixie_creds

from fixie_batch.environ import QUEUE_STATUSES
from fixie_batch.simulations import run, cancel, query


class Run(RequestHandler):

    schema = {'simulation': {'anyof_type': ['dict', 'string'], 'required': True},
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

    @fixie_creds.authenticated
    def post(self):
        resp = run(user=self.get_current_user(), **self.request.arguments)
        response = dict(zip(self.response_keys, resp))
        self.write(response)


class Cancel(RequestHandler):

    schema = {'job': {'anyof_type': ['integer', 'string'], 'required': True},
              'project': {'type': 'string'},
              }
    response_keys = ('jobid', 'status', 'message')

    @fixie_creds.authenticated
    def post(self):
        resp = cancel(user=self.get_current_user(), **self.request.arguments)
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

    @fixie_creds.authenticated
    def post(self):
        resp = query(**self.request.arguments)
        response = dict(zip(self.response_keys, resp))
        self.write(response)


HANDLERS = [
    ('/run', Run),
    ('/cancel', Cancel),
    ('/query', Query),
]
