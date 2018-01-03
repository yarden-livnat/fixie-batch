"""Sets up the environment variables for fixie batch execution."""
import os
import itertools
import functools

from xonsh.tools import is_string, ensure_string, always_false

from fixie.environ import ENV, ENVVARS, expand_and_make_dir


QUEUE_STATUSES = frozenset(['completed', 'canceled', 'running'])


def fixie_job_status_dir(status):
    """Ensures and returns the $FIXIE_{STATUS}_JOBS_DIR"""
    fsjd = os.path.join(ENV.get('FIXIE_JOBS_DIR'), 'fixie', status)
    os.makedirs(fsjd, exist_ok=True)
    return fsjd


def distinct_status_dirs(d):
    d = expand_and_make_dir(d)
    t = 'FIXIE_{0}_JOBS_DIR'
    for x, y in itertools.combinations(QUEUE_STATUSES, 2):
        xd = ENV.get(t.format(x.upper()), None)
        yd = ENV.get(t.format(y.upper()), None)
        if xd is not None and yd is not None and xd == yd:
            msg = '${0} and ${1} must have distinct values, got {2!r}'
            raise ValueError(msg.format(x, y, xd))
        elif xd is not None and xd == d:
            msg = '${0} and new value must be distinct, got {1!r}'
            raise ValueError(msg.format(x, d))
        elif yd is not None and yd == d:
            msg = '${0} and new value must be distinct, got {1!r}'
            raise ValueError(msg.format(y, d))
    return d


t = 'FIXIE_{0}_JOBS_DIR'
for status in QUEUE_STATUSES:
    ENVVARS[t.format(status.upper())] = (
        functools.partial(fixie_job_status_dir, status), always_false,
        distinct_status_dirs, ensure_string,
        'Path to fixie ' + status + ' jobs directory, must be distinct from '
        'other status directories')
del status, t
