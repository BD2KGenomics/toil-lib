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
    layers, the `popen` layer, the `docker` layer and the `container` layer. The `popen` layer
    configures the call to Popen. The `docker` and `container` layers then append to the first
    parameter for the Popen call (the `args` parameter), first the `docker` layer, followed by
    the `container` layer. The container layer is typically used to parameterize a program running inside
    the Docker container, e.g. for `docker run` or `docker exec`. Within a layer we distinguish
    between three groups: commands, positional parameters (aka arguments) and optional parameters
    (aka options). The following example illustrates how layers and groups relate to the
    parameters of a Popen invocation:

                  Popen argument
                  |         Docker command
                  |         |      Docker option
                  |         |      |     Docker argument
                  |         |      |     |         Container command
                  |         |      |     |         |     Container option
                  |         |      |     |         |     |     Container argument
                  |         |      |     |         |     |     |      Popen option
                  |         |      |     |         |     |     |      |
    Popen( args=[ 'docker', 'run`, '-i', 'ubuntu', 'ls', '-l', '/' ], cwd='/tmp' )

    The `popen` layer does not have a group for commands.

    >>> (Docker()
    ...     .docker('run','ubuntu', interactive=True) # run `ubuntu` image, passing `--interactive`
    ...     .docker_params(rm=True)  # layers are cumulative, also pass `--rm`
    ...     .container('foo', '/', bar=42) # invoke `foo --bar=42 /` command inside the container
    ...     .popen_params(cwd='/foo/bar') # pass `cwd='/foo/bar'` to the Popen constructor
    ...     .work_dir('/foo/bar')  # ditto
    ...     .build_popen_call())
    {'args': ['docker', 'run', '--interactive', '--rm', 'ubuntu', 'foo', '--bar=42', '/'], 'cwd': '/foo/bar'}
    """

    def __init__(self, docker='docker'):
        """
        Construct an empty builder.

        :param docker: optional path to the Docker client executable
        """
        super(Docker, self).__init__()
        self.pipe_next = None
        self.pipe_prev = None
        for layer in 'docker', 'popen', 'container':
            for group in 'cmds', 'args', 'opts':
                setattr(self, self._attribute_name(layer, group), [])
        self._popen_args.append(docker)

    def docker(self, command, *args, **opts):
        """
        Specify docker command to run, including options and arguments.

        :param command: Docker client command to be invoked, e.g. `run`, `ps`

        :param args: Command line arguments to pass to docker client command, e.g. the image name in the case of `run`.
               See docker_params().

        :param opts: Command line options to pass to docker client command, e.g. --rm in the case of `run`.
               See docker_params().

        :return: self
        :rtype: Docker
        """
        self.docker_command(command)
        return self.docker_params(*args, **opts)

    def container(self, command, *args, **opts):
        """
        Specify command to run in container, including options and arguments. This should only be used when the
        Docker client command is `run`, `create` or `exec`.

        :param command: The first argument to the container entry point, for images that don't specify an entry point
        this becomes the container entry point, i.e. the path to (or name of) the program to be executed inside the
        container.

        :param args: Command line arguments to be passed to the entry point. See container_params().

        :param opts: Command line options to be passed to the entry point. See container_params().

        :return: self
        :rtype: Docker
        """
        self.container_command(command)
        return self.container_params(*args, **opts)

    def popen_params(self, **opts):
        """
        Specify keyword arguments for the invocation of the Popen constructor.
        """
        require('args' not in opts, 'Passing `args=...` is disallowed. Please use docker_params() or '
                                    'container_params() instead.')
        return self._params('popen', (), opts)

    def docker_params(self, *args, **opts):
        """
        Specify command line arguments and options for docker.

        :param args: Command line arguments to the docker client command. Arguments are always passed after options.
               They are sometimes referred to as positional arguments.

        :param opts: Command line options, aka. flags. Each keyword argument will be treated as follows. Its name
               will be converted to the command line option by prepending either `-`, for names of length 1,
               or `--` otherwise. Its value will be converted to a string and appended to the option with `=` in
               between, unless the value is a boolean. A boolean value of True will be omitted, leaving the option
               without argument. A boolean value of False will cause all previous argument-less occurrences of that
               option to be removed.

        :return: self
        :rtype: Docker

        This builder method is cumulative. Subsequent invocations will append to the argument and options. This is
        useful for repeating options of the same name.

        While this doesn't work:

        >>> Docker().docker_params(volume='/a:/tmp/a', volume='/b:/tmp/b').build_popen_call()
        Traceback (most recent call last):
        ...
        SyntaxError: keyword argument repeated

        This does:

        >>> Docker().docker_params(volume='/a:/tmp/a').docker_params(volume='/b:/tmp/b').build_popen_call()
        {'args': ['docker', '--volume=/a:/tmp/a', '--volume=/b:/tmp/b']}

        Note that magic '__' methods can be used to shorten that even further:

        >>> Docker().docker__volume('/a:/tmp/a', '/b:/tmp/b').build_popen_call()
        {'args': ['docker', '--volume=/a:/tmp/a', '--volume=/b:/tmp/b']}
        """
        return self._params('docker', args, opts)

    def container_params(self, *args, **opts):
        """
        Specify command line options and arguments for the container entry point. Same semantics as
        :meth:`docker_params` except that these options and arguments will occur at the end of the overall docker
        client invocation.
        """
        return self._params('container', args, opts)

    def docker_command(self, cmd):
        """
        Specify the Docker client command to be invoked. This builder method is not cumulative: subsquent invocations
        will override the command set by earlier ones.

        :param str cmd:
        :return: self
        :rtype: Docker
        """
        self._docker_cmds[:] = [cmd]
        return self

    def container_command(self, *cmds):
        """
        Specify the inital arguments to the entry point. For images that don't specify an entry point,
        the first argument will become the entry point while all remaining arguments will be passed to the entry
        point, at the beginning of the command line, followed by container options and container arguments (see
        :meth:`container_params`). This builder method is not cumulative: subsquent invocations
        will override earlier ones.

        :return: self
        :rtype: Docker
        """
        self._container_cmds[:] = cmds
        return self

    def build_popen_call(self):
        """
        Assemble the parameters for the Popen constructor as currently configured. See also :meth:`build_popen`.

        :return: a dictionary with keyword arguments to the Popen constructor, including the first non-keyword
                 argument called `args`

        :rtype: dict
        """
        args, kwargs = self._build_function_call('popen')
        args.extend(self._build_program_call('docker'))
        args.extend(self._build_program_call('container'))
        # Since we know that Popen() has only one positional argument called 'args' we can convert it into a keyword
        # argument.
        return dict(kwargs, args=args)

    def build_popen(self):
        """
        Call Popen as currently configured.

        :return: a Popen instance
        :rtype: subprocess.Popen
        """
        return subprocess.Popen(**self.build_popen_call())

    def work_dir(self, path):
        """
        Set the working directory the docker client will be invoked in.

        :return: self
        :rtype: Docker
        """
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
        Implements the __ magic for specifying command line options or Popen kwargs in a single
        builder method call. This is mainly useful for specifying repeated command line options.
        Instead of

        .docker_option(env='FOO=bla').docker_option(env='BAR=bro')

        you can do

        .docker__env('FOO=bla', 'BAR=bro')

        or even just

        .docker__env(FOO='bla', BAR='bro')

        >>> (Docker()
        ...     .docker__rm() # `--rm`
        ...     .docker__detach() # `--detach`
        ...     .docker__detach(True) # `--detach`, again
        ...     .docker__detach(False) # remove all preceding `--detach` occurrences (!)
        ...     .docker__i() # `-i`
        ...     .docker__env('A=1', 'B=2') # repeated option
        ...     .docker__env(C=3, D=4) # repeated option, more concise
        ...     .container__bar(42) # pass `--bar=42` to container
        ...     .container__('/') # pass `/` to container (discouraged, use `.container_params('/')` instead)
        ...     .container__self() # to prove that we don't conflict with Python's `self`
        ...     .build_popen_call())
        {'args': ['docker', '--rm', '-i', '--env=A=1', '--env=B=2', '--env=C=3', '--env=D=4', '--bar=42', '--self', '/']}
        """
        try:
            layer, key = item.split('__', 1)
        except ValueError:
            pass
        else:
            if layer in ('docker', 'popen', 'container'):
                def magic_setter(*value_args, **value_kwargs):
                    if not value_args and not value_kwargs:
                        value_args = [True]
                    group = self._group(layer, 'opts' if key else 'args')
                    for value in concat(value_args, value_kwargs.iteritems()):
                        group.append((key, value) if key else value)
                    return self

                return magic_setter
        raise AttributeError(item)

    def _build_program_call(self, layer):
        cmds, opts, args = self._get_layer(layer)
        result = []
        result.extend(cmds)
        for option, value in opts:
            if len(option) == 1:
                option = '-' + option
            else:
                option = '--' + option.replace('_', '-')
            if value is True:
                result.append(option)
            elif value is False:
                result[:] = (x for x in result if x != option)
            elif isinstance(value, tuple) and len(value) == 2:
                k, v = value
                result.append(option + '=' + str(k) + '=' + str(v))
            else:
                result.append(option + '=' + str(value))
        result.extend(args)
        return result

    def _build_function_call(self, layer):
        cmds, opts, args = self._get_layer(layer)
        assert not cmds
        return list(args), {k: v for k, v in opts}

    def call(self, stdin=None, check=False):
        """
        Start the docker client process, optionally feeding it the given input.

        If the pipe_to() method was used to form a chain of instances, this method returns the
        status of the last failed process in the chain or 0 if all processes succeed.

        :param file|str stdin: input to pass to processes stdin

        :param bool check: if True, an exception will be raised if the status of any process in the chain is
               non-zero. The exception will be raise for the last process in the chain that failed.

        :rtype: (int,str|None,str|None)
        :return: A tuple of the form (status, stdout, stderr)
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

        >>> DockerRun('ubuntu', i=True).container('find', '/etc').pipe_to(
        ...     DockerRun('ubuntu', i=True).container('grep', '^/etc/hosts$').pipe_to(
        ...         DockerRun('ubuntu', i=True).return_stdout().container('wc', l=True))).call()
        (0, '1\\n', None)

        The last

        >>> pipe = DockerRun('ubuntu', i=True).container('find', '/etc').pipe_to(
        ...     DockerRun('ubuntu', i=True).container('grep', '^not found$').pipe_to(
        ...         DockerRun('ubuntu', i=True).return_stdout().container('wc', l=True)))

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
    ...     .container_command('ls')
    ...     .container_params('/dir/to/list', recursive=True)
    ...     .docker_env(FOO='bla', BAR='bro')
    ...     ).build_popen_call()
    {'args': ['docker', 'run', '-e=FOO=bla', '-e=BAR=bro', 'ubuntu', 'ls', '--recursive', '/dir/to/list']}
    """

    def __init__(self, image, **docker_opts):
        super(DockerRun, self).__init__()
        self.docker('run', image, **docker_opts)

    def docker_env(self, **env):
        return self.docker__e(**env)


class DockerRunTool(DockerRun):
    """
    Runs an image from the cgl-docker-lib collection.
    """

    def __init__(self, image, mock=None):
        super(DockerRunTool, self).__init__(image)
        self.mock = mock_mode() if mock is None else mock

    def check_inputs(self, *inputs):
        return self

    def check_outputs(self, *outputs):
        return self
