"""Tools for spawning, canceling, and querying cyclus simulations. Simulations are
spawned detached from the parent process. All information about running processes
is stored on the file system. These tools should be robust to the server or other
managing process going down.
"""
import os
import json
import time
import signal
from collections.abc import Mapping, Set

from pprintpp import pformat
from lazyasd import lazyobject
from fixie import (ENV, verify_user, next_jobid, detached_call,
    register_job_alias, jobids_from_alias, jobids_with_name)

from fixie_batch.environ import QUEUE_STATUSES


SPAWN_XSH = """#!/usr/bin/env xonsh
import os
import json
import time


def queued_ids():
    # sorted jobids that are in the queue
    qids = [int(j.name[:-5]) for j in os.scandir('{{FIXIE_QUEUED_JOBS_DIR}}')]
    qids.sort()
    return qids


# variables from calling process
simulation = {{simulation}}

# derived variables
out = '{{FIXIE_SIMS_DIR}}/{{jobid}}.h5'
inp = json.dumps(simulation, sort_keys=True)

# make a jobs file and add it to the queue
job = {
    'interactive': {{interactive}},
    'jobid': {{jobid}},
    'notify': {{notify}},
    'outfile': out,
    'pid': os.getpid(),
    'permissions': {{permissions}},
    'post': {{post}},
    'project': '{{project}}',
    'queue_starttime': time.time(),
    'simulation': simulation,
    'user': '{{user}}',
    }
with open('{{FIXIE_QUEUED_JOBS_DIR}}/{{jobid}}.json', 'w') as f:
    json.dump(job, f, sort_keys=True, indent=1)

# wait for the queue to be free, and then move the jobs file
qids = queued_ids()
while {{jobid}} not in qids[:{{FIXIE_NJOBS}}]:
    if {{jobid}} not in qids:
        # job cancels itself if it isn't in the queue at all!
        err = 'Job canceled itself after jobfile was removed from queue'
        job.update({'returncode': 1,
                    'out': None,
                    'err': err,
                    'queue_endtime': time.time()})
        with open('{{FIXIE_CANCELED_JOBS_DIR}}/{{jobid}}.json', 'w') as f:
            json.dump(job, f, sort_keys=True, indent=1)
        import sys
        sys.exit(err)
    time.sleep(0.1)
    qids = queued_ids()
job['queue_endtime'] = time.time()
os.remove('{{FIXIE_QUEUED_JOBS_DIR}}/{{jobid}}.json')
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
            FIXIE_CANCELED_JOBS_DIR=ENV['FIXIE_CANCELED_JOBS_DIR'],
            FIXIE_COMPLETED_JOBS_DIR=ENV['FIXIE_COMPLETED_JOBS_DIR'],
            FIXIE_FAILED_JOBS_DIR=ENV['FIXIE_FAILED_JOBS_DIR'],
            FIXIE_NJOBS=ENV['FIXIE_NJOBS'],
            FIXIE_QUEUED_JOBS_DIR=ENV['FIXIE_QUEUED_JOBS_DIR'],
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
    if name or project:
        register_job_alias(jobid, user, name=name, project=project)
    rtn = (jobid, True, 'Simulation spawned')
    if return_pid:
        rtn += (pid,)
    return rtn


STATUS_IDS = {}

t = """
def {status}_ids():
    "Set of {status} jobids."
    ids = {{int(j.name[:-5]) for j in os.scandir(ENV['FIXIE_{STATUS}_JOBS_DIR'])}}
    return ids


STATUS_IDS['{status}'] = {status}_ids
"""
g = globals()
for status in QUEUE_STATUSES:
    exec(t.format(status=status, STATUS=status.upper()), g)
del g, t, status


def cancel(job, user, token, project=''):
    """Cancels a job that is queued or running.

    Parameters
    ----------
    job : int or str
        If an integer, this is the jobid. If it is a string, it is interpreted as
        a job name, and a jobid is looked up via the aliases cache.
    user : str
        Name of the user
    token : str
        Credential token for the user
    project : str, optional
        Name of the project, default '', only used with alias lookup.

    Returns
    -------
    jobid : int
        Unique job id of the canceled run, this is negative if a unique jobid
        could not be found.
    status : bool
        Whether run was successfully canceled.
    message : str
        Message about status
    """
    # verify users
    valid, msg, status = verify_user(user, token)
    if not status or not valid:
        return -1, False, msg
    # get jobids
    qids = queued_ids()
    rids = running_ids()
    qrids = qids | rids
    if isinstance(job, str):
        jobids = jobids_from_alias(user, job, project=project)
    else:
        jobids = {job}
    # check uniqueness and get jobid
    current = jobids & qrids
    if len(current) > 1:
        msg = ('Too many jobids found! {user} in project {project!r} is '
               'has the folllowing jobids queued or running for {name!r}: '
               '{jobs}')
        msg = msg.format(user=user, project=project, name=job,
                         jobs=', '.join(map(str, current)))
        return -1, False,  msg
    elif len(current) == 0:
        return -1, False, 'No running or queued job found'
    else:
        jobid = jobids.pop()
    # get the job data, if we can find it
    base = str(jobid) + '.json'
    status_dirs = [ENV['FIXIE_QUEUED_JOBS_DIR'], ENV['FIXIE_RUNNING_JOBS_DIR']]
    for d in status_dirs:
        jobfile = os.path.join(d, base)
        if os.path.exists(jobfile):
            # can't just do a normal read here, since we may be trying to cancel
            # a job that hasn't been fully written yet.
            s = ''
            with open(jobfile) as f:
                while not s:
                    s = f.read()
            data = json.loads(s)
            break
    else:
        return -1, False, 'Job file could not be cound in queue or running.'
    # kill the job and transfer job to canceled dir
    if user != data['user']:
        return jobid, False, 'User did not start job, cannot cancel it!'
    os.kill(data['pid'], signal.SIGTERM)
    os.remove(jobfile)
    if 'queued_endtime' not in data:
        data['queued_endtime'] = time.time()
    if 'starttime' not in data:
        data['starttime'] = time.time()
    data.update({
        'returncode': 1,
        'endtime': time.time(),
        'out': None,
        'err': 'Job was canceled externally',
        })
    jobfile = os.path.join(ENV['FIXIE_CANCELED_JOBS_DIR'], base)
    with open(jobfile, 'w') as f:
        json.dump(data, f, sort_keys=True, indent=1)
    return jobid, True, 'Job canceled'


def _convert_to_statuses_set(statuses):
    """Returns a set of valid statuses AND an error message.
    On failure, the set of statues will be None.
    """
    if isinstance(statuses, str):
        statuses = {statuses}
    s = set()
    for status in statuses:
        if status == 'all':
            s |= QUEUE_STATUSES
        elif not isinstance(status, str):
            return None, 'status must be a string, got ' + repr(status)
        elif status not in QUEUE_STATUSES:
            return None, status + ' is not a valid status'
        else:
            s.add(status)
    return s, ''


def _ensure_set_of_str_or_none(x):
    """Returns an object that is a set of str or None AND an error message.
    On failure, the error message will be non-empty.
    """
    if x is None:
        return x, ''
    elif isinstance(x, str):
        return set([x]), ''
    elif isinstance(x, Set):
        for e in x:
            if not isinstance(e, str):
                return None, '{0!r} is not a string'.format(e)
        return x, ''
    else:
        y = set()
        for e in x:
            if not isinstance(e, str):
                return None, '{0!r} is not a string'.format(e)
            y.add(e)
        return y, ''


def _load_job(jobid, hint):
    """Loads a job from a jobid and a hint about which status queue it might
    be in. Returns a job dict or None (if the job could not be found), and the
    status the job was found in.
    """
    t = 'FIXIE_{0}_JOBS_DIR'
    base = str(jobid) + '.json'
    # first try the hint
    jobfile = os.path.join(ENV[t.format(hint.upper())], base)
    if os.path.exists(jobfile):
        with open(jobfile) as f:
            job = json.load(f)
        return job, hint
    # couldn't find in the hint, search other statuses.
    for status in QUEUE_STATUSES:
        jobfile = os.path.join(ENV[t.format(status.upper())], base)
        if os.path.exists(jobfile):
            with open(jobfile) as f:
                job = json.load(f)
            return job, status
    else:
        return None, None


def query(statuses='all', users=None, jobs=None, projects=None):
    """Returns the state of the jobs, filtered as approriate.

    Parameters
    ----------
    statuses : str or set of str, optional
        Which queue statuses we should pull jobs from. If 'all', every status queue is
        searched. If a set of str is provided, the statuses are ORed together.
    users : str, set of str, or None, optional
        User name(s) to filter on. If None, all user names are used. User names
        are ORed together, if multiple are provided.
    jobs : int, str, set of ints & strs, or None, optional
        The jobids and job names to filter on, if provided.  If None, filtering
        on job id/name is not done. These are ORed together.
    projects : str, set of str, or None, optional
        Project names to filer on, if not None. These are ORed together.

    Returns
    -------
    data : list of dicts or None
        The list of all jobs found. None if status is False.
    status : bool
        Whether or not the query was successful.
    message : str
        Message related to the status of the query
    """
    users, msg = _ensure_set_of_str_or_none(users)
    if msg:
        return None, False, msg
    projects, msg = _ensure_set_of_str_or_none(projects)
    if msg:
        return None, False, msg
    # get job ids from statuses
    statuses, msg = _convert_to_statuses_set(statuses)
    if statuses is None:
        return None, False, msg
    sids = set()  # all ids joined together
    ids_to_status = {}  # ids mapped to the status found
    for status in statuses:
        s = STATUS_IDS[status]()
        sids |= s
        ids_to_status.update({i: status for i in s})
    # get job ids from jobs
    if jobs is None:
        # since jobs was not provided, we want the maximal set
        # this happens to be at most the job ids we have already found
        jids = sids
    elif isinstance(jobs, int):
        jids = jobs = set([jobs])
    else:
        if isinstance(jobs, str):
            jobs = set([jobs])
        jids = set()
        for job in jobs:
            if isinstance(job, int):
                jids.add(job)
            elif isinstance(job, str):
                jids |= jobids_with_name(job)
            else:
                msg = 'type of job not reconized: {0} {1}'
                return None, False, msg.format(job, type(job))
    # Now load the jobfiles and filter based on user and project
    data = []
    jobids = sids & jids
    for jobid in sorted(jobids):
        job, status = _load_job(jobid, ids_to_status[jobid])
        if job is None:
            continue
        if users is not None and job['user'] not in users:
            continue
        if projects is not None and job['project'] not in projects:
            continue
        job['status'] = status
        data.append(job)
    return data, True, 'Jobs queried'

