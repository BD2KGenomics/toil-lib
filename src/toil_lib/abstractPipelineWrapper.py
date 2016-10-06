from __future__ import print_function

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import ruamel.yaml
from abc import abstractmethod
from toil_lib import require, UserError, current_docker_container_id, dockerd_is_reachable

log = logging.getLogger(__name__)

class AbstractPipelineWrapper(object):
    """
    This class can be subclassed to define wrapper scripts to run specific Toil pipelines in Docker
    containers. The commandline interface for this script is populated automatically from keys in
    the YAML config file generated from the pipeline.
    """
    def __init__(self, name, desc):
        """
        :param str name: The name of the command to start the workflow.
        :param str desc: The description of the workflow.
        """
        self._name = name
        self._desc = desc
        self._mount_path = None

    @classmethod
    def run(cls, name, desc):
        """
        Prepares and runs the pipeline. Note this method must be invoked both from inside a
        Docker container and while the docker daemon is reachable.

        :param str name: The name of the command to start the workflow.
        :param str desc: The description of the workflow.
        """
        wrapper = cls(name, desc)
        mount_path = wrapper._get_mount_path()
        # prepare parser
        arg_parser = wrapper._create_argument_parser()
        wrapper._extend_argument_parser(arg_parser)
        # prepare config file
        empty_config = wrapper.__get_empty_config()
        config_yaml = ruamel.yaml.load(empty_config)
        wrapper.__populate_parser_from_config(arg_parser, config_yaml)
        args = arg_parser.parse_args()
        for k,v in vars(args).items():
            k = k.replace('_', '-')
            if k in config_yaml:
                config_yaml[k] = v
        config_path = wrapper._get_config_path()
        with open(config_path, 'w') as writable:
            ruamel.yaml.dump(config_yaml, stream=writable)
        # prepare workdir
        workdir_path = os.path.join(mount_path, 'Toil-' + wrapper._name)
        if os.path.exists(workdir_path):
            if args.restart:
                 log.info('Reusing temporary directory: %s', workdir_path)
            else:
                raise UserError('Temporary directory {} already exists. Run with --restart '
                                'option or remove directory.'.format(workdir_path))
        else:
            os.makedirs(workdir_path)
            log.info('Temporary directory created: %s', workdir_path)

        command = wrapper._create_pipeline_command(args, workdir_path, config_path)
        wrapper._extend_pipeline_command(command, args)
        # run command
        try:
            subprocess.check_call(command)
        except subprocess.CalledProcessError as e:
            print(e, file=sys.stderr)
        finally:
            stat = os.stat(mount_path)
            log.info('Pipeline terminated, changing ownership of output files in %s from root to '
                     'uid %s and gid %s.', mount_path, stat.st_uid, stat.st_gid)
            chown_command = ['chown', '-R', '%s:%s' % (stat.st_uid, stat.st_gid), mount_path]
            subprocess.check_call(chown_command)
            if args.no_clean:
                log.info('Flag "--no-clean" was used, therefore %s was not deleted.', workdir_path)
            else:
                log.info('Cleaning up temporary directory: %s', workdir_path)
                shutil.rmtree(workdir_path)

    def __populate_parser_from_config(self, arg_parser, config_data, prefix=''):
        """
        Populates an ArgumentParser object with arguments where each argument is a key from the
        given config_data dictionary.

        :param str prefix: Prepends the key with this prefix delimited by a single '.' character.
        :param argparse.ArgumentParser arg_parser:
        :param dict config_data: The parsed yaml data from the config.
        >>> pw = AbstractPipelineWrapper('test', 'this is a test')
        >>> parser = argparse.ArgumentParser()
        >>> pw._PipelineWrapperBuilder__populate_parser_from_config(parser, {'a':None, 'b':2})
        >>> vars(parser.parse_args(['--a', '1']))
        {'a': '1', 'b': 2}
        >>> vars(parser.parse_args(['--b', '3']))
        {'a': None, 'b': '3'}

        >>> parser = argparse.ArgumentParser()
        >>> pw._PipelineWrapperBuilder__populate_parser_from_config(parser, {})
        >>> vars(parser.parse_args([]))
        {}

        >>> parser = argparse.ArgumentParser()
        >>> pw._PipelineWrapperBuilder__populate_parser_from_config(parser,
        ...                                                         dict(a={'a':'b', 'c':{'d':'e'}},
        ...                                                              f='g', h={}))
        >>> vars(parser.parse_args([]))
        {'f': 'g', 'a.a': 'b', 'a.c.d': 'e'}
        """
        for k,v in config_data.items():
            k = prefix + '.' + k if prefix else k
            if isinstance(v, dict):
                self.__populate_parser_from_config(arg_parser, v, prefix=k)
            else:
                self._add_option(arg_parser, name=k, default=v)

    def __get_empty_config(self):
        """
        Returns the config file contents as a string. The config file is generated and then deleted.
        """
        self._generate_config()
        path = self._get_config_path()
        with open(path, 'r') as readable:
            contents = readable.read()
        os.remove(path)
        return contents

    def _get_mount_path(self):
        """
        Returns the path of the mount point of the current container. If this method is invoked
        outside of a Docker container a NotInsideContainerError is raised. Likewise if the docker
        daemon is unreachable from inside the container a UserError is raised. This method is
        idempotent.
        """
        if self._mount_path is None:
            name = current_docker_container_id()
            if dockerd_is_reachable():
                # Get name of mounted volume
                blob = json.loads(subprocess.check_output(['docker', 'inspect', name]))
                mounts = blob[0]['Mounts']
                # Ensure docker.sock is mounted correctly
                sock_mnt = [x['Source'] == x['Destination']
                            for x in mounts if 'docker.sock' in x['Source']]
                require(len(sock_mnt) == 1,
                        'Missing socket mount. Requires the following: '
                         'docker run -v /var/run/docker.sock:/var/run/docker.sock')
                # Ensure formatting of command for 2 mount points
                if len(mounts) == 2:
                    require(all(x['Source'] == x['Destination'] for x in mounts),
                            'Docker Src/Dst mount points, invoked with the -v argument, '
                            'must be the same if only using one mount point aside from the docker '
                            'socket.')
                    work_mount = [x['Source'] for x in mounts if 'docker.sock' not in x['Source']]
                else:
                    # Ensure only one mirror mount exists aside from docker.sock
                    mirror_mounts = [x['Source'] for x in mounts if x['Source'] == x['Destination']]
                    work_mount = [x for x in mirror_mounts if 'docker.sock' not in x]
                    require(len(work_mount) == 1, 'Wrong number of mirror mounts provided, see '
                                                  'documentation.')
                self._mount_path = work_mount[0]
                log.info('The work mount is: %s', self._mount_path)
            else:
                raise UserError('Docker daemon is not reachable, ensure Docker is being run with: '
                                 '"-v /var/run/docker.sock:/var/run/docker.sock" as an argument.')
        return self._mount_path

    def _get_config_path(self):
        """
        Returns the path of a pipeline config file, without regard for its existence.
        """
        return '%sconfig-%s.yaml' % (os.getcwd(), self._name)

    def _generate_config(self):
        """
        Generates the config file for the pipeline.
        """
        subprocess.check_call([self._name, 'generate-config'])

    def _add_option(self, arg_parser, name, *args, **kwargs):
        """
        Add an argument to the given arg_parser with the given name.

        :param argparse.ArgumentParser arg_parser:
        :param str name: The name of the option.
        """
        arg_parser.add_argument('--' + name, *args, **kwargs)

    def _create_argument_parser(self):
        """
        Creates and returns an ArgumentParser object prepopulated with 'no clean', 'cores' and
        'restart' arguments.
        """
        parser = argparse.ArgumentParser(description=self._desc,
                                         formatter_class=argparse.RawTextHelpFormatter)
        parser.add_argument('--no-clean', action='store_true',
                            help='If this flag is used, temporary work directory is not cleaned.')
        parser.add_argument('--restart', action='store_true',
                            help='If this flag is used, a previously uncleaned workflow in the same'
                                 ' directory will be resumed')
        parser.add_argument('--cores', type=int, default=None,
                            help='Will set a cap on number of cores to use, default is all '
                                 'available cores.')
        return parser

    def _create_pipeline_command(self, args, workdir_path, config_path):
        """
        Creates and returns a list that represents a command for running the pipeline.
        """
        return ([self._name, 'run', os.path.join(workdir_path, 'jobStore'),
                 '--config', config_path,
                 '--workDir', workdir_path, '--retryCount', '1']
                 + (['--restart'] if args.restart else []))

    @abstractmethod
    def _extend_argument_parser(self, parser):
        """
        Extends the argument parser object with any pipeline specific arguments.
        """
        raise NotImplementedError()

    @abstractmethod
    def _extend_pipeline_command(self, command, args):
        """
        Extends the given list representing a pipeline command with pipeline specific options.
        """
        raise NotImplementedError()
