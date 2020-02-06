import platform

import pexpect

from pexpect import popen_spawn


def close_process(p):
    """ For windows, PopenSpawn use wait(), otherwise use close() """
    try:
        p.close()
    except AttributeError:
        p.wait()


def is_windows():
    """ because we need to detect windows """
    return platform.system() == 'Windows'


def spawn_process(command, timeout=1):
    """ detect if windows or not, to use different spawn
        command:

https://pexpect.readthedocs.io/en/stable/overview.html#pexpect-on-windows
https://github.com/pexpect/pexpect/issues/328#issuecomment-299611118
        """
    if is_windows():
        p = pexpect.popen_spawn.PopenSpawn(command, timeout=timeout)
    else:
        p = pexpect.spawn(command,
                          timeout=timeout)
    return p
