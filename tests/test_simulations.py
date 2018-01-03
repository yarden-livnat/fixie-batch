"""Tests batch simulation execution functionality"""
import os
import json
import time

from fixie import ENV, waitpid

from fixie_batch.simulations import spawn


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
