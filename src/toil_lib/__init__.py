import argparse
import os
import tempfile
import logging
import re
import subprocess

log = logging.getLogger(__name__)

def flatten(x):
    """
    Flattens a nested array into a single list

    :param list x: The nested list/tuple to be flattened.
    """
    result = []
    for el in x:
        if hasattr(el, "__iter__") and not isinstance(el, basestring):
            result.extend(flatten(el))
        else:
            result.append(el)
    return result


def partitions(l, partition_size):
    """
    >>> list(partitions([], 10))
    []
    >>> list(partitions([1,2,3,4,5], 1))
    [[1], [2], [3], [4], [5]]
    >>> list(partitions([1,2,3,4,5], 2))
    [[1, 2], [3, 4], [5]]
    >>> list(partitions([1,2,3,4,5], 5))
    [[1, 2, 3, 4, 5]]

    :param list l: List to be partitioned
    :param int partition_size: Size of partitions
    """
    for i in xrange(0, len(l), partition_size):
        yield l[i:i + partition_size]


class UserError(Exception):
    pass

class NotInsideContainerError(Exception):
    pass

def require(expression, message):
    if not expression:
        raise UserError('\n\n' + message + '\n\n')


def required_length(nmin, nmax):
    """
    For use with argparse's action argument. Allows setting a range for nargs.
    Example: nargs='+', action=required_length(2, 3)

    :param int nmin: Minimum number of arguments
    :param int nmax: Maximum number of arguments
    :return: RequiredLength object
    """
    class RequiredLength(argparse.Action):
        def __call__(self, parser, args, values, option_string=None):
            if not nmin <= len(values) <= nmax:
                msg = 'argument "{f}" requires between {nmin} and {nmax} arguments'.format(
                    f=self.dest, nmin=nmin, nmax=nmax)
                raise argparse.ArgumentTypeError(msg)
            setattr(args, self.dest, values)
    return RequiredLength


def inside_docker_container():
    """
    Returns True if this method is called inside a Docker container.
    """
    try:
        current_docker_container_id()
    except NotInsideContainerError:
        return False
    else:
        return True


def dockerd_is_reachable():
    """
    Returns True if the docker daemon is reachable from the docker client.
    """
    try:
        subprocess.check_call(['docker', 'info'])
    except subprocess.CalledProcessError:
        log.exception('')
        return False
    else:
        return True


def current_docker_container_id():
    """
    Returns a string that represents the container ID of the current Docker container. If this
    function is invoked outside of a container a NotInsideContainerError is raised.

    >>> import subprocess
    >>> import sys
    >>> a = subprocess.check_output(['docker', 'run', '-v',
    ...                              sys.modules[__name__].__file__ + ':/foo.py',
    ...                              'python:2.7.12','python', '-c',
    ...                              'from foo import current_docker_container_id;\\
    ...                               print current_docker_container_id()'])
    int call will fail if a is not a valid hex string
    >>> int(a, 16) > 0
    True
    """
    try:
        with open('/proc/1/cgroup', 'r') as readable:
            raw = readable.read()
        ids = set(re.compile('[0-9a-f]{12,}').findall(raw))
        assert len(ids) == 1
        return ids.pop()
    except:
        logging.exception('Failed to obtain current container ID')
        raise NotInsideContainerError()
