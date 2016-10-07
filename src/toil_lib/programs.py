import base64
import os
import subprocess
import logging

from bd2k.util.exceptions import require

_log = logging.getLogger(__name__)


def mock_mode():
    """
    Checks whether the ADAM_GATK_MOCK_MODE environment variable is set.
    In mock mode, all docker calls other than those to spin up and submit jobs to the spark cluster
    are stubbed out and dummy files are used as inputs and outputs.
    """
    return True if int(os.environ.get('TOIL_SCRIPTS_MOCK_MODE', '0')) else False


def docker_call(job,
                tool,
                parameters=None,
                work_dir='.',
                rm=True,
                detached=False,
                env=None,
                outfile=None,
                inputs=None,
                outputs=None,
                docker_parameters=None,
                check_output=False,
                mock=None,
                defer=None,
                container_name=None,
                mounts=None):
    """
    Calls Docker, passing along parameters and tool.

    :param toil.Job.job job: The Job instance for the calling function.
    :param str tool: Name of the Docker image to be used (e.g. quay.io/ucsc_cgl/samtools)
    :param list[str] parameters: Command line arguments to be passed to the tool
    :param str work_dir: Directory to mount into the container via `-v`. Destination convention is /data
    :param bool rm: Should the container be run with the --rm flag (Should it be removed upon
           container exit)? rm and detached are mutually exclusive in Docker.  This is the flag
           passed to docker and is independent of the defer flag.  If this is set to True and
           `defer` is None, `defer` takes the value `docker_call.RM`.
    :param bool detached: Should the container be run with the --detached flag (Should it be run in
           detached mode)? See `rm` above.
    :param dict[str,str] env: Environment variables to be added (e.g. dict(JAVA_OPTS='-Xmx15G'))
    :param file outfile: Pipe output of Docker call to file handle
    :param list[str] inputs: A list of the input files.
    :param dict[str,str] outputs: A dictionary containing the outputs files as keys with either None
           or a url. The value is only used if mock=True
    :param dict[str,str] docker_parameters: Parameters to pass to docker
    :param bool check_output: When True, this function returns docker's output
    :param bool mock: Whether to run in mock mode. If this variable is unset, its value will be determined by
           the environment variable.
    :param int defer: What action should be taken on the container upon job completion?
           docker_call.FORGO will leave the container untouched.
           docker_call.STOP will attempt to stop the container with `docker stop` (useful for
           debugging).
           docker_call.RM will stop the container and then forcefully remove it from the system
           using `docker rm -f`.
           The default value is None and that shadows docker_call.FORGO
    :param str container_name: An optional name for your container.
    :param dict mounts: A dictionary of data volumes to mount into the Docker container containing host paths
           as keys and the corresponding container paths as values
    """
    from toil_lib.urls import download_url

    if mock is None:
        mock = mock_mode()
    if parameters is None:
        parameters = []
    if inputs is None:
        inputs = []
    if outputs is None:
        outputs = {}

    # Docker does not allow the --rm flag to be used when the container is run in detached mode.
    require(not (rm and detached), "Conflicting options 'rm' and 'detached'.")
    # Ensure the user has passed a valid value for defer
    require(defer in (None, docker_call.FORGO, docker_call.STOP, docker_call.RM),
            'Please provide a valid value for defer.')

    for filename in inputs:
        assert(os.path.isfile(os.path.join(work_dir, filename)))

    if mock:
        for filename, url in outputs.items():
            file_path = os.path.join(work_dir, filename)
            if url is None:
                # create mock file
                if not os.path.exists(file_path):
                    f = open(file_path, 'w')
                    f.write("contents") # FIXME
                    f.close()

            else:
                file_path = os.path.join(work_dir, filename)
                if not os.path.exists(file_path):
                    outfile = download_url(job, url, work_dir=work_dir, name=filename)
                assert os.path.exists(file_path)
        return

    if not container_name:
        container_name = _get_container_name(job)
    base_docker_call = ['docker', 'run',
                        '--log-driver=none',
                        '-v', '{}:/data'.format(os.path.abspath(work_dir))]
    if mounts:
        require(isinstance(mounts, dict), "'mounts' parameter must be a dictionary object")
        for k, v in mounts.iteritems():
            base_docker_call.extend(['-v', k + ':' + v])

    # Defer the permission fixing function.  We call this explicitly later on in this function, but
    # we defer it as well to handle unexpected job failure.
    job.defer(_fix_permissions, base_docker_call, tool, work_dir)

    base_docker_call.extend(['--name', container_name])
    if rm:
        base_docker_call.append('--rm')
        if defer is None:
            defer = docker_call.RM
    elif detached:
        base_docker_call += ['-d']
    # Defer the container on-exit action
    job.defer(_docker_kill, container_name, action=defer)

    if env:
        for e, v in env.iteritems():
            base_docker_call.extend(['-e', '{}={}'.format(e, v)])
    if docker_parameters:
        base_docker_call += docker_parameters

    _log.debug("Calling docker with %s." % " ".join(base_docker_call + [tool] + parameters))

    call = base_docker_call + [tool] + parameters

    if outfile:
        subprocess.check_call(call, stdout=outfile)
    else:
        if check_output:
            return subprocess.check_output(call)
        else:
            subprocess.check_call(call)
    # Fix root ownership of output files
    _fix_permissions(base_docker_call, tool, work_dir)

    for filename in outputs.keys():
        if not os.path.isabs(filename):
            filename = os.path.join(work_dir, filename)
        assert(os.path.isfile(filename))

docker_call.FORGO = 0
docker_call.STOP = 1
docker_call.RM = 2


def _docker_kill(container_name, action):
    """
    Kills the specified container.

    :param str container_name: The name of the container created by docker_call
    :param int action: What action should be taken on the container?  See `defer=` in
           :func:`docker_call`
    """
    running = _container_is_running(container_name)
    if running is None:
        # This means that the container doesn't exist.  We will see this if the container was run
        # with --rm and has already exited before this call.
        _log.info('The container with name "%s" appears to have already been removed.  Nothing to '
                  'do.', container_name)
    else:
        if action in (None, docker_call.FORGO):
            _log.info('The container with name %s continues to exist as we were asked to forgo a '
                      'post-job action on it.', container_name)
            return
        else:
            _log.info('The container with name %s exists. Running user-specified defer functions.',
                      container_name)
            if running and action >= docker_call.STOP:
                _log.info('Stopping container "%s".', container_name)
                subprocess.check_call(['docker', 'stop', container_name])
            else:
                _log.info('The container "%s" was not found to be running.', container_name)
            if action >= docker_call.RM:
                # If the container was run with --rm, then stop will most likely remove the
                # container.  We first check if it is running then remove it.
                running = _container_is_running(container_name)
                if running is not None:
                    _log.info('Removing container "%s".', container_name)
                    try:
                        subprocess.check_call(['docker', 'rm', '-f', container_name])
                    except subprocess.CalledProcessError as e:
                        _log.exception("'docker rm' failed.")
                else:
                    _log.info('The container "%s" was not found on the system.  Nothing to remove.',
                              container_name)


def _fix_permissions(base_docker_call, tool, work_dir):
    """
    Fix permission of a mounted Docker directory by reusing the tool

    :param list base_docker_call: Docker run parameters
    :param str tool: Name of tool
    :param str work_dir: Path of work directory to recursively chown
    """
    base_docker_call.append('--entrypoint=chown')
    # We don't need the cleanup container to persist.
    base_docker_call.append('--rm')
    stat = os.stat(work_dir)
    command = base_docker_call + [tool] + ['-R', '{}:{}'.format(stat.st_uid, stat.st_gid), '/data']
    subprocess.check_call(command)


def _get_container_name(job):
    return '--'.join([job.fileStore.jobStore.config.workflowID,
                      job.fileStore.jobID,
                      base64.b64encode(os.urandom(9), '-_')])


def _container_is_running(container_name):
    """
    Checks whether the container is running or not.

    :param container_name: Name of the container being checked.
    :returns: True if running, False if not running, None if the container doesn't exist.
    :rtype: bool
    """
    try:
        output = subprocess.check_output(['docker', 'inspect', '--format', '{{.State.Running}}',
                                          container_name]).strip()
    except subprocess.CalledProcessError:
        # This will be raised if the container didn't exist.
        _log.debug("'docker inspect' failed. Assuming container %s doesn't exist.", container_name,
                   exc_info=True)
        return None
    if output == 'true':
        return True
    elif output == 'false':
        return False
    else:
        raise AssertionError("Got unexpected value for State.Running (%s)" % output)

