"""Tornado handlers for interfacing with fixie batch execution."""
from fixie import RequestHandler

from fixie_batch.simulations import spawn, cancel, query


class Spawn(RequestHandler):

    schema = {'simulation': {'anyof_type': ['dict', 'string'], 'required': True},
              'user': {'type': 'string', 'empty': False, 'required': True},
              'token': {'type': 'string', 'regex': '[0-9a-fA-F]+', 'required': True},
              'name': {'type': 'string'},
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


HANDLERS = [
    ('/spawn', Spawn),
]