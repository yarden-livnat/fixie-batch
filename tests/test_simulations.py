"""Tests batch simulation execution functionality"""
import os
import json
import time

from fixie import ENV, waitpid

from fixie_batch.simulations import spawn, cancel, query





def test_spawn(xdg, verify_user):
    jobid, status, msg, pid = spawn(SIMULATION, 'me', '42', return_pid=True)
    assert jobid == 0
    assert status
    assert msg == 'Simulation spawned'
    assert pid >= 0
    # job should be completed after waiting
    waitpid(pid, timeout=10.0)
    jobfile = ENV['FIXIE_COMPLETED_JOBS_DIR'] + '/0.json'
    assert os.path.exists(jobfile)
    with open(jobfile) as f:
        job = json.load(f)
    # test that the job is well formed.
    assert SIMULATION == job['simulation']
    assert 'me' == job['user']
    assert pid == job['pid']
    assert jobid == job['jobid']
    assert 'out' in job
    assert 'err' in job
    assert  0 == job['returncode']
    # test that a pending path file was created
    pp = ENV['FIXIE_PATHS_DIR'] + '/me-0-pending-path.json'
    assert os.path.exists(pp)


def test_self_canceling_queue(xdg, verify_user):
    """Tests that a job will cancel itself if its jobfile in the queue is
    externally removed.
    """
    ENV['FIXIE_NJOBS'] = 0
    jobid, status, msg, pid = spawn(SIMULATION, 'me', '42', return_pid=True)
    assert jobid == 0
    assert status
    assert msg == 'Simulation spawned'
    assert pid >= 0
    # make sure job is in the queue
    while len(os.listdir(ENV['FIXIE_QUEUED_JOBS_DIR'])) == 0:
        time.sleep(0.001)
    jobfile = ENV['FIXIE_QUEUED_JOBS_DIR'] + '/0.json'
    assert os.path.exists(jobfile)
    # remove the jobfile and wait for the job to finish
    os.remove(jobfile)
    waitpid(pid, timeout=2.0)
    # job should now be in canceled
    jobfile = ENV['FIXIE_CANCELED_JOBS_DIR'] + '/0.json'
    assert os.path.exists(jobfile)
    with open(jobfile) as f:
        job = json.load(f)
    # test that the job is well formed.
    assert 1 == job['returncode']
    assert 'out' in job
    assert 'err' in job


def test_cancel(xdg, verify_user):
    """Tests that a job can be canceled externally"""
    ENV['FIXIE_NJOBS'] = 0
    jobid, status, msg, pid = spawn(SIMULATION, 'me', '42', return_pid=True)
    assert jobid == 0
    assert status
    assert msg == 'Simulation spawned'
    assert pid >= 0
    # make sure job is in the queue
    while len(os.listdir(ENV['FIXIE_QUEUED_JOBS_DIR'])) == 0:
        time.sleep(0.001)
    jobfile = ENV['FIXIE_QUEUED_JOBS_DIR'] + '/0.json'
    assert os.path.exists(jobfile)
    # cancel the job
    cid, status, msg = cancel(0, 'me', '42')
    assert cid == jobid
    assert status
    assert msg == 'Job canceled'
    # job should now be in canceled
    jobfile = ENV['FIXIE_CANCELED_JOBS_DIR'] + '/0.json'
    assert os.path.exists(jobfile)
    with open(jobfile) as f:
        job = json.load(f)
    # test that the job is well formed.
    assert 1 == job['returncode']
    assert 'out' in job
    assert 'err' in job


def _jobfile(status, jobid):
    d = ENV['FIXIE_{0}_JOBS_DIR'.format(status.upper())]
    jobfile = os.path.join(d, str(jobid) + '.json')
    return jobfile


def test_query(xdg):
    # mock up some jobs
    jobs = [
        (0, 'completed', 'aperson', 'p0'),
        (1, 'failed', 'bperson', 'p1'),
        (2, 'canceled', 'aperson', 'p2'),
        (3, 'running', 'cperson', 'p0'),
        (4, 'queued', 'dperson', 'p3'),
        ]
    # write out mock jobs
    exp = []
    for jobid, status, user, project in jobs:
        jobfile = _jobfile(status, jobid)
        job = {'jobid': jobid, 'user': user, 'project': project}
        with open(jobfile, 'w') as f:
            json.dump(job, f)
        job['status'] = status
        exp.append(job)
    # test all query
    obs, flag, msg = query()
    assert exp == obs
    # test one status
    obs, flag, msg = query(statuses='completed')
    assert exp[:1] == obs
    # test two statuses
    obs, flag, msg = query(statuses={'completed', 'failed'})
    assert exp[:2] == obs
    # test one user
    obs, flag, msg = query(users='bperson')
    assert [exp[1]] == obs
    # test two users
    obs, flag, msg = query(users={'aperson', 'bperson'})
    assert exp[:3] == obs
    # test statuses and users
    obs, flag, msg = query(statuses={'completed', 'failed'}, users={'aperson', 'bperson'})
    assert exp[:2] == obs
    # test one job
    obs, flag, msg = query(jobs=0)
    assert exp[:1] == obs
    # test two jobs
    obs, flag, msg = query(jobs={0, 1})
    assert exp[:2] == obs
    # test statuses and users
    obs, flag, msg = query(statuses={'completed', 'failed'}, jobs={0, 1, 4})
    assert exp[:2] == obs
    # test one project
    obs, flag, msg = query(projects='p1')
    assert [exp[1]] == obs
    # test two projects
    obs, flag, msg = query(projects={'p1', 'p0'})
    assert [exp[0], exp[1], exp[3]] == obs
    # test projects and users
    obs, flag, msg = query(users={'aperson', 'bperson'}, projects={'p1', 'p0'})
    assert exp[:2] == obs
    # test all
    obs, flag, msg = query(users={'aperson', 'bperson'}, projects={'p1', 'p0'},
                           jobs={0, 1, 4}, statuses={'completed', 'failed', 'running'})
    assert exp[:2] == obs

