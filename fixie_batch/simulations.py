"""Tools for spawning, canceling, and querying cyclus simulations. Simulations are
spawned detached from the parent process. All information about running processes
is stored on the file system. These tools should be robust to the server or other
managing process going down.
"""
from collections.abc import Mapping

from pprintpp import pformat
from lazyasd import lazyobject
from fixie import ENV, verify_user, next_jobid


SPAWN_XSH = """#!/usr/bin/env xonsh
import os
import json

# variables from calling process
jobid = {{jobid}}
simulation = {{simulation}}

# derived variables
out = $FIXIE_JOBS
inp = json.dumps(simulation)

# run cyclus itself
p = ![cyclus -f json -o @(out) @(inp)]
"""


@lazyobject
def SPAWN_TEMPLATE():
    """A jinja template for spawning simulations."""
    from jinja2 import Template
    return Template(SPAWN_XSH)



def spawn(simulation, user, token, name='', project='', permisions='public',
          post=(), notify=(), interactive=False):
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

    Returns
    -------
    jobid : str
        Unique job id of this run, this is an empty string if job
        could not be spawned.
    status : bool
        Whether run was spawned successfully,
    message : str,
        Message about status
    """
    # validate all inputs
    if not isinstance(simualtion, Mapping):
        return '', False, 'Simulation must be dict (i.e. mapping object) currently.'
    if permisions != 'public':
        return '', False, 'Non-public permisions are not supported yet.'
    if post:
        return '', False, 'Post-processing activities are not supported yet.'
    if notify:
        return '', False, 'Notifications are not supported yet.'
    if interactive:
        return '', False, 'Interactive simulation spawning is not supported yet.'
    valid, msg, status = verify_user(user, token):
    if not status or not valid:
        return '', False, msg
    # now we can actually spawn the simulation
    jobid = next_jobid()
    ctx = dict(
            jobid=jobid,
            simulation=pformat(simulaton),
            )
