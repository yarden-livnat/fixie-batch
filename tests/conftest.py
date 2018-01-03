import os
import shutil
import tempfile

import pytest

import fixie
import fixie.tools
from fixie import environ
from fixie.environ import ENV

import fixie_batch.simulations


@pytest.fixture
def xdg_config_home(request):
    """A fixure that creates a temporary $XDG_CONFIG_HOME directory."""
    d = tempfile.mkdtemp()
    with ENV.swap(XDG_CONFIG_HOME=d):
        with environ.context():
            yield d
    shutil.rmtree(d)


@pytest.fixture
def xdg_data_home(request):
    """A fixure that creates a temporary $XDG_DATA_HOME directory."""
    d = tempfile.mkdtemp()
    with ENV.swap(XDG_DATA_HOME=d):
        with environ.context():
            yield d
    shutil.rmtree(d)


@pytest.fixture
def xdg(request):
    """A fixure that creates a temporary XDG base directory and sets
    $XDG_DATA_HOME={xdg-base}/share and $XDG_CONFIG_HOME={xdg-base}/config.
    """
    d = tempfile.mkdtemp()
    data = os.path.join(d, 'share')
    conf = os.path.join(d, 'config')
    with ENV.swap(XDG_DATA_HOME=data, XDG_CONFIG_HOME=conf):
        with environ.context():
            yield d
    shutil.rmtree(d)


def always_verify_user(user, token):
    """Always verifys the user/token pair"""
    return True, 'User verified', True


@pytest.fixture
def verify_user(monkeypatch):
    monkeypatch.setattr(fixie, 'verify_user', always_verify_user)
    monkeypatch.setattr(fixie.tools, 'verify_user', always_verify_user)
    monkeypatch.setattr(fixie_batch.simulations, 'verify_user', always_verify_user)
