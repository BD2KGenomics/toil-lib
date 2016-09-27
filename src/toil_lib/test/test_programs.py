from toil_lib.programs import docker_call, _container_is_running, _docker_kill
from toil.job import Job
from toil.leader import FailedJobsException
from threading import Thread
import logging
import os
import signal
import time
import uuid

_log = logging.getLogger(__name__)

def test_docker_clean(tmpdir):
    """
    Run the test container that creates a file in the work dir, and sleeps for 5 minutes.  Ensure
    that the calling job gets SIGKILLed after a minute, leaving behind the spooky/ghost/zombie
    container. Ensure that the container is killed on batch system shutdown (through the defer
    mechanism).

    This inherently also tests docker_call

    :param tmpdir: A temporary directory to work in
    :returns: None
    """
    # We need to test the behaviour of `defer` with `rm` and `detached`. We do not look at the case
    # where `rm` and `detached` are both True.  This is the truth table for the different
    # combinations at the end of the test. R = Running, X = Does not exist, E = Exists but not
    # running.
    #              None     FORGO     STOP    RM
    #    rm        X         R         X      X
    # detached     R         R         E      X
    #  Neither     R         R         E      X
    assert os.getuid() != 0, "Cannot test this if the user is root."
    data_dir = os.path.join(str(tmpdir), 'data')
    work_dir = os.path.join(str(tmpdir), 'working')
    test_file = os.path.join(data_dir, 'test.txt')
    os.mkdir(data_dir)
    os.mkdir(work_dir)
    options = Job.Runner.getDefaultOptions(os.path.join(str(tmpdir), 'jobstore'))
    options.logLevel = 'INFO'
    options.workDir = work_dir
    options.clean = 'always'
    for rm in (True, False):
        for detached in (True, False):
            if detached and rm:
                continue
            for defer in (docker_call.FORGO, docker_call.STOP, docker_call.RM, None):
                # Not using base64 logic here since it might create a name starting with a `-`.
                container_name = uuid.uuid4().hex
                print rm, detached, defer
                A = Job.wrapJobFn(_test_docker_clean_fn, data_dir, detached, rm, defer,
                                  container_name)
                try:
                    Job.Runner.startToil(A, options)
                except FailedJobsException:
                    # The file created by spooky_container would remain in the directory, and since
                    # it was created inside the container, it would have had uid and gid == 0 (root)
                    # upon creation. If the defer mechanism worked, it should now be non-zero and we
                    # check for that.
                    file_stats = os.stat(test_file)
                    assert file_stats.st_gid != 0
                    assert file_stats.st_uid != 0
                    if (rm and defer != docker_call.FORGO) or defer == docker_call.RM:
                        # These containers should not exist
                        assert _container_is_running(container_name) is None, \
                            'Container was not removed.'
                    elif defer == docker_call.STOP:
                        # These containers should exist but be non-running
                        assert _container_is_running(container_name) == False, \
                            'Container was not stopped.'
                    else:
                        # These containers will be running
                        assert _container_is_running(container_name) == True, \
                            'Container was not running.'
                finally:
                    # Prepare for the next test.
                    _docker_kill(container_name, docker_call.RM)
                    os.remove(test_file)


def _test_docker_clean_fn(job, work_dir, detached=None, rm=None, defer=None, container_name=None):
    """
    Test function for test docker_clean.  Runs a container with given flags and then dies leaving
    behind a zombie container

    :param toil.job.Job job: job
    :param work_dir: See `work_dir=` in :func:`docker_call`
    :param bool rm: See `rm=` in :func:`docker_call`
    :param bool detached: See `detached=` in :func:`docker_call`
    :param int defer: See `defer=` in :func:`docker_call`
    :param str container_name: See `container_name=` in :func:`docker_call`
    :return:
    """
    def kill_self():
        test_file = os.path.join(work_dir, 'test.txt')
        # This will kill the worker once we are sure the docker container started
        while not os.path.exists(test_file):
            _log.debug('Waiting on the file created by spooky_container.')
            time.sleep(1)
        # By the time we reach here, we are sure the container is running.
        os.kill(os.getpid(), signal.SIGKILL)  # signal.SIGINT)
    t = Thread(target=kill_self)
    # Make it a daemon thread so that thread failure doesn't hang tests.
    t.daemon = True
    t.start()
    docker_call(job, tool='quay.io/ucsc_cgl/spooky_test', work_dir=work_dir, detached=detached,
                rm=rm, defer=defer, container_name=container_name)
