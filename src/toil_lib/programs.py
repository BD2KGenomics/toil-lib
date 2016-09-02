import os
import subprocess
import logging
from bd2k.util.exceptions import panic, require
from bd2k.util.iterables import concat

_log = logging.getLogger(__name__)


def mock_mode():
    """
    Checks whether the ADAM_GATK_MOCK_MODE environment variable is set.
    In mock mode, all docker calls other than those to spin up and submit jobs to the spark cluster
    are stubbed out and dummy files are used as inputs and outputs.
    """
    return True if int(os.environ.get('TOIL_SCRIPTS_MOCK_MODE', '0')) else False


def docker_call(tool,
                parameters=None,
                work_dir='.',
                rm=True,
                env=None,
                outfile=None,
                inputs=None,
                outputs=None,
                docker_parameters=None,
                check_output=False,
                mock=None):
    """
    Calls Docker, passing along parameters and tool.

    :param str tool: Name of the Docker image to be used (e.g. quay.io/ucsc_cgl/samtools)
    :param list[str] parameters: Command line arguments to be passed to the tool
    :param str work_dir: Directory to mount into the container via `-v`. Destination convention is /data
    :param bool rm: Set to True to pass `--rm` flag.
    :param dict[str,str] env: Environment variables to be added (e.g. dict(JAVA_OPTS='-Xmx15G'))
    :param bool sudo: If True, prepends `sudo` to the docker call
    :param file outfile: Pipe output of Docker call to file handle
    :param list[str] inputs: A list of the input files.
    :param dict[str,str] outputs: A dictionary containing the outputs files as keys with either None
                                  or a url. The value is only used if mock=True
    :param dict[str,str] docker_parameters: Parameters to pass to docker
    :param bool check_output: When True, this function returns docker's output
    :param bool mock: Whether to run in mock mode. If this variable is unset, its value will be determined by
                      the environment variable.
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
                    outfile = download_url(url, work_dir=work_dir, name=filename)
                assert os.path.exists(file_path)
        return
    
    base_docker_call = ['docker', 'run',
                        '--log-driver=none',
                        '-v', '{}:/data'.format(os.path.abspath(work_dir))]
    if rm:
        base_docker_call.append('--rm')
    if env:
        for e, v in env.iteritems():
            base_docker_call.extend(['-e', '{}={}'.format(e, v)])
    if docker_parameters:
        base_docker_call += docker_parameters

    _log.debug("Calling docker with %s." % " ".join(base_docker_call + [tool] + parameters))

    docker_call = base_docker_call + [tool] + parameters

    try:
        if outfile:
            subprocess.check_call(docker_call, stdout=outfile)
        else:
            if check_output:
                return subprocess.check_output(docker_call)
            else:
                subprocess.check_call(docker_call)
    # Fix root ownership of output files
    except:
        # Panic avoids hiding the exception raised in the try block
        with panic():
            _fix_permissions(base_docker_call, tool, work_dir)
    else:
        _fix_permissions(base_docker_call, tool, work_dir)

    for filename in outputs.keys():
        if not os.path.isabs(filename):
            filename = os.path.join(work_dir, filename)
        assert(os.path.isfile(filename))


def _fix_permissions(base_docker_call, tool, work_dir):
    """
    Fix permission of a mounted Docker directory by reusing the tool

    :param list base_docker_call: Docker run parameters
    :param str tool: Name of tool
    :param str work_dir: Path of work directory to recursively chown
    """
    base_docker_call.append('--entrypoint=chown')
    stat = os.stat(work_dir)
    command = base_docker_call + [tool] + ['-R', '{}:{}'.format(stat.st_uid, stat.st_gid), '/data']
    subprocess.check_call(command)


class Docker(object):
    """
    A builder (https://en.wikipedia.org/wiki/Builder_pattern) for Popen objects representing a
    Docker client invocation. When building a Popen object, it distinguishes between three
    layers, the `popen` layer, the `docker` layer and the `extra` layer. The `popen` layer
    configures the call to Popen. The `docker` and `extra` layers then append to the first
    parameter for the Popen call (the `args` parameter), first the `docker` layer, followed by
    the `extra` layer. The extra layer is typically used to parameterize a program running inside
    the Docker container, e.g. for `docker run` or `docker exec`. Within a layer we distinguish
    between three groups: commands, positional parameters (aka arguments) and optional parameters
    (aka options). The following example illustrates how layers and groups relate to the
    parameters of a Popen invocation:

                  Popen argument
                  |         Docker command
                  |         |      Docker option
                  |         |      |     Docker argument
                  |         |      |     |         Extra command
                  |         |      |     |         |     Extra option
                  |         |      |     |         |     |     Extra argument
                  |         |      |     |         |     |     |      Popen option
                  |         |      |     |         |     |     |      |
    Popen( args=[ 'docker', 'run`, '-i', 'ubuntu', 'ls', '-l', '/' ], cwd='/tmp' )

    The `popen` layer does not have a group for commands.

    >>> (Docker()
    ...     .docker_command('run') # invoke `docker run`
    ...     .docker_params('ubuntu', interactive=True) # pass `--interactive` and run `ubuntu` image
    ...     .docker_params(rm=True)  # docker() is cumulative, also pass `--rm`
    ...     .extra_command('foo') # invoke `foo` command inside container
    ...     .extra_params('/', bar=42) # pass `--bar=42 /` to foo
    ...     .popen_params(cwd='/foo/bar') # pass `cwd='/foo/bar'` to the Popen constructor
    ...     .work_dir('/foo/bar')  # same
    ...     .build_popen_call())
    {'args': ['docker', 'run', '--interactive', '--rm', 'ubuntu', 'foo', '--bar=42', '/'], 'cwd': '/foo/bar'}
    """

    def __init__(self):
        super(Docker, self).__init__()
        self.pipe_next = None
        self.pipe_prev = None
        for layer in 'docker', 'popen', 'extra':
            for group in 'cmds', 'args', 'opts':
                setattr(self, self._attribute_name(layer, group), [])
        self.popen_params('docker')

    def docker(self, command, *args, **opts):
        self.docker_command(command)
        return self.docker_params(*args, **opts)

    def extra(self, command, *args, **opts):
        self.extra_command(command)
        return self.extra_params(*args, **opts)

    def popen_params(self, *args, **opts):
        return self._params('popen', args, opts)

    def docker_params(self, *args, **opts):
        return self._params('docker', args, opts)

    def extra_params(self, *args, **opts):
        return self._params('extra', args, opts)

    def docker_command(self, cmd, *cmds):
        return self._cmds('docker', cmd, cmds)

    def extra_command(self, cmd, *cmds):
        return self._cmds('extra', cmd, cmds)

    def build_popen_call(self):
        args, kwargs = self._build_function_call('popen')
        args.extend(self._build_program_call('docker'))
        args.extend(self._build_program_call('extra'))
        # Since we know that Popen() has only one positional argument called 'args' we can convert it into a keyword
        # argument.
        return dict(kwargs, args=args)

    def build_popen(self):
        return subprocess.Popen(**self.build_popen_call())

    def work_dir(self, path):
        return self.popen_params(cwd=path)

    def return_stdout(self):
        return self.popen_params(stdout=subprocess.PIPE)

    def return_stderr(self):
        return self.popen_params(stderr=subprocess.PIPE)

    def merge_stderr(self):
        return self.popen_params(stderr=subprocess.STDOUT)

    def _params(self, layer, args, opts):
        for group, value in ('args', args), ('opts', opts.iteritems()):
            self._group(layer, group).extend(value)
        return self

    def _cmds(self, layer, cmd, cmds):
        self._group(layer, 'cmds').extend(concat(cmd, cmds))
        return self

    @staticmethod
    def _attribute_name(layer, group):
        return '_%s_%s' % (layer, group)

    def _group(self, layer, group):
        return getattr(self, self._attribute_name(layer, group))

    def _get_layer(self, layer):
        cmds, opts, args = [self._group(layer, group) for group in 'cmds', 'opts', 'args']
        return cmds, opts, args

    def __getattr__(self, item):
        """
        >>> (Docker()
        ...     .docker('run','ubuntu')
        ...     .docker__rm(True)
        ...     .docker__i() # utilizing default argument
        ...     .docker__env('FOO=bla', 'BAR=bro') # multiple arguments
        ...     .extra__('/').extra__bar(42)
        ...     .build_popen_call())
        {'args': ['docker', 'run', '--rm', '-i', '--env=FOO=bla', '--env=BAR=bro', 'ubuntu', '--bar=42', '/']}
        """
        try:
            layer, key = item.split('__', 1)
        except ValueError:
            pass
        else:
            if layer in ('docker', 'popen', 'extra'):
                def magic_setter(*values):
                    if not values:
                        values = [True]
                    if key:
                        group = self._group(layer, 'opts')
                        for value in values:
                            group.append((key, value))
                    else:
                        group = self._group(layer, 'args')
                        for value in values:
                            group.append(value)
                    return self

                return magic_setter
        raise AttributeError(item)

    def _build_program_call(self, layer):
        cmds, opts, args = self._get_layer(layer)
        result = []
        result.extend(cmds)
        for k, v in opts:
            if len(k) == 1:
                k = '-' + k
            else:
                k = '--' + k.replace('_', '-')
            if v is True:
                result.append(k)
            elif v is False:
                pass
            else:
                result.append(k + '=' + str(v))
        result.extend(args)
        return result

    def _build_function_call(self, layer):
        cmds, opts, args = self._get_layer(layer)
        assert not cmds
        return list(args), {k: v for k, v in opts}

    def call(self, stdin=None, check=False):
        """
        Start the docker client process, optionally feeding it the given input.

        If the pipe_to() method was used to form a chain of instances, this method returns the status of the last
        failed process in the chain or 0 if all processes succeed. The returned stdout and

        :param file|str stdin: input to pass to processes stdin

        :param bool check: if True, an exception will be raised if the status of any process in the chain is
               non-zero. The exception will be raise for the last process in the chain that failed.

        :rtype: (int,str|None,str|None)
        :return: A tuple of the form (status,stdout, stderr)
        """
        kwargs = self.build_popen_call()
        if stdin is not None:
            kwargs['stdin'] = stdin
        popen = subprocess.Popen(**kwargs)
        try:
            if self.pipe_prev:
                # The subprocess documentation says this is necessary to ensure that upstream process
                stdin.close()
            if self.pipe_next:
                status, stdout, stderr = self.pipe_next.call(stdin=popen.stdout, check=check)
                popen.wait()
                if status == 0:
                    status = popen.returncode
            else:
                stdout, stderr = popen.communicate()
                status = popen.returncode
            if check and status != 0:
                raise subprocess.CalledProcessError(status, kwargs['args'], stdout)
            return status, stdout, stderr
        finally:
            popen.wait()  # ok to call wait() twice, it is idempotent

    def pipe_to(self, other):
        """
        Configure this instance to send its standard output to another instance's standard input. This method does
        not start any processes or transfer any data, it merely connects both instances. When self.call() is invoked,
        other.call() will be invoked automatically and the process created by the former will pipe its output to the
        process created by the latter.

        The method may only be called once per instance. The other instance may be connected to a third one and so
        on, forming a chain.

        :param Docker other: the other end of the pipe

        >>> DockerRun('ubuntu', i=True).extra('find', '/etc').pipe_to(
        ...     DockerRun('ubuntu', i=True).extra('grep', '^/etc/hosts$').pipe_to(
        ...         DockerRun('ubuntu', i=True).return_stdout().extra('wc', l=True))).call()
        (0, '1\\n', None)

        The last

        >>> pipe = DockerRun('ubuntu', i=True).extra('find', '/etc').pipe_to(
        ...     DockerRun('ubuntu', i=True).extra('grep', '^not found$').pipe_to(
        ...         DockerRun('ubuntu', i=True).return_stdout().extra('wc', l=True)))

        >>> pipe.call()
        (1, '0\\n', None)

        >>> pipe.call(check=True)
        Traceback (most recent call last):
        ...
        CalledProcessError: Command '['docker', 'run', '-i', 'ubuntu', 'grep', '^not found$']' returned non-zero exit status 1
        """
        require(self.pipe_next is None, 'Cannot call pipe_to() more than once')
        self.popen_params(stdout=subprocess.PIPE)
        self.pipe_next = other
        other.pipe_prev = self
        return self


class DockerRun(Docker):
    """
    >>> (DockerRun('ubuntu')
    ...     .extra_command('ls')
    ...     .extra_params('/dir/to/list', recursive=True)
    ...     .docker_env(FOO='bla', BAR='bro')
    ...     ).build_popen_call()
    {'args': ['docker', 'run', '-e=FOO=bla', '-e=BAR=bro', 'ubuntu', 'ls', '--recursive', '/dir/to/list']}
    """

    def __init__(self, image, **docker_opts):
        super(DockerRun, self).__init__()
        self.docker('run', image, **docker_opts)

    def docker_env(self, **env):
        self.docker__e(*[k + '=' + v for k, v in env.iteritems()])
        return self


class DockerRunGenomicsTool(DockerRun):
    """
    Runs an image from the cgl-docker-lib collection.
    """

    def __init__(self, image, mock=None):
        super(DockerRunGenomicsTool, self).__init__(image)
        self.mock = mock_mode() if mock is None else mock

    def check_inputs(self, *inputs):
        return self

    def check_outputs(self, *outputs):
        return self
