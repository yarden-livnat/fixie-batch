"""Tools for spawning, canceling, and querying cyclus simulations. Simulations are
spawned detached from the parent process. All information about running processes
is stored on the file system. These tools should be robust to the server or other
managing process going down.
"""
from collections.abc import Mapping

from pprintpp import pformat
from lazyasd import lazyobject
from fixie import ENV, verify_user, next_jobid, detached_call


SPAWN_XSH = """#!/usr/bin/env xonsh
import os
import json

# variables from calling process
simulation = {{simulation}}

# derived variables
out = '{{FIXIE_SIMS_DIR}}/{{jobid}}.h5'
inp = json.dumps(simulation, sort_keys=True)

# make a running jobs file
job = {
    'interactive': {{interactive}},
    'jobid': {{jobid}},
    'notify': {{notify}},
    'outfile': out,
    'pid': os.getpid(),
    'permissions': {{permissions}},
    'post': {{post}},
    'project': '{{project}}',
    'simulation': simulation,
    'user': '{{user}}',
    }
with open('{{FIXIE_RUNNING_JOBS_DIR}}/{{jobid}}.json', 'w') as f:
    json.dump(job, f, sort_keys=True, indent=1)

# run cyclus itself
with ${...}.swap(RAISE_SUBPROC_ERROR=False):
    proc = !(cyclus -f json -o @(out) @(inp))

# update and swap job file
job.update({
    'returncode': proc.returncode,
    'starttime': proc.starttime,
    'endtime': proc.endtime,
    'out': proc.out,
    'err': proc.err,
    })
jobdir = '{{FIXIE_COMPLETED_JOBS_DIR}}' if proc else '{{FIXIE_FAILED_JOBS_DIR}}'
os.remove('{{FIXIE_RUNNING_JOBS_DIR}}/{{jobid}}.json')
with open(jobdir + '/{{jobid}}.json', 'w') as f:
    json.dump(job, f, sort_keys=True, indent=1)
"""


@lazyobject
def SPAWN_TEMPLATE():
    """A jinja template for spawning simulations."""
    from jinja2 import Template
    return Template(SPAWN_XSH)


def spawn(simulation, user, token, name='', project='', permissions='public',
          post=(), notify=(), interactive=False, return_pid=False):
    """Spawning simulations let’s the batch execution service know to run a
    simulation as soon as possible.

    Parameters
    ----------
    simulation : dict or string
        Cyclus simulation, currently only dict methods are supported
    user : str
        Name of the user
    token : str
        Credential token for the user
    name : str, optional
        Alias for simulation, default ''
    project : str, optional
        Name of the project, default ''
    permissions : str or list of str, optional
        "public" (default), "private", or a list of users. Currently only
        public permissions are supported.
    post : list, optional
        Any post processing activities, not currently supported
    notify : list, optional
        Any notifications to register, not currently supported
    interactive : bool, optional
        True or False (default), not currently supported.
    return_pid : bool, optional
        Whether or not to return the PID of the detached child process.
        Default False, this is mostly for testing.

    Returns
    -------
    jobid : int
        Unique job id of this run, this is negative if job
        could not be spawned.
    status : bool
        Whether run was spawned successfully,
    message : str
        Message about status
    pid : int, if return_pid is True
        Child process id.
    """
    # validate all inputs
    if not isinstance(simulation, Mapping):
        return -1, False, 'Simulation must be dict (i.e. mapping object) currently.'
    if permissions != 'public':
        return -1, False, 'Non-public permissions are not supported yet.'
    if post:
        return -1, False, 'Post-processing activities are not supported yet.'
    if notify:
        return -1, False, 'Notifications are not supported yet.'
    if interactive:
        return -1, False, 'Interactive simulation spawning is not supported yet.'
    valid, msg, status = verify_user(user, token)
    if not status or not valid:
        return -1, False, msg
    # now we can actually spawn the simulation
    jobid = next_jobid()
    ctx = dict(
            FIXIE_COMPLETED_JOBS_DIR=ENV['FIXIE_COMPLETED_JOBS_DIR'],
            FIXIE_FAILED_JOBS_DIR=ENV['FIXIE_FAILED_JOBS_DIR'],
            FIXIE_RUNNING_JOBS_DIR=ENV['FIXIE_RUNNING_JOBS_DIR'],
            FIXIE_SIMS_DIR=ENV['FIXIE_SIMS_DIR'],
            interactive=interactive,
            jobid=jobid,
            name=name,
            notify=repr(notify),
            permissions=repr(permissions),
            post=repr(post),
            project=project,
            simulation=pformat(simulation),
            user=user,
            )
    script = SPAWN_TEMPLATE.render(ctx)
    cmd = ['xonsh', '-c', script]
    pid = detached_call(cmd)
    rtn = (jobid, True, 'Simulation spawned')
    if return_pid:
        rtn += (pid,)
    return rtn
