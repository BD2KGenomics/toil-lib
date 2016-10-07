import glob
import logging
import os
import shutil
import subprocess
from urlparse import urlparse

from bd2k.util.exceptions import require
from toil_lib.programs import docker_call


_log = logging.getLogger(__name__)


def download_url(job, url, work_dir='.', name=None, s3_key_path=None, cghub_key_path=None):
    """
    Downloads URL, can pass in file://, http://, s3://, or ftp://, gnos://cghub/analysisID, or gnos:///analysisID
    If downloading S3 URLs, the S3AM binary must be on the PATH

    :param toil.job.Job job: Toil job that is calling this function
    :param str url: URL to download from
    :param str work_dir: Directory to download file to
    :param str name: Name of output file, if None, basename of URL is used
    :param str s3_key_path: Path to 32-byte encryption key if url points to S3 file that uses SSE-C
    :param str cghub_key_path: Path to cghub key used to download from CGHub.
    :return: Path to the downloaded file
    :rtype: str
    """
    file_path = os.path.join(work_dir, name) if name else os.path.join(work_dir, os.path.basename(url))
    if cghub_key_path:
        _download_with_genetorrent(job, url, file_path, cghub_key_path)
    elif urlparse(url).scheme == 's3':
        _s3am_with_retry(job, num_cores=1, file_path=file_path, s3_url=url, mode='download', s3_key_path=s3_key_path)
    elif urlparse(url).scheme == 'file':
        shutil.copy(urlparse(url).path, file_path)
    else:
        subprocess.check_call(['curl', '-fs', '--retry', '5', '--create-dir', url, '-o', file_path])
    assert os.path.exists(file_path)
    return file_path


def download_url_job(job, url, name=None, s3_key_path=None, cghub_key_path=None):
    """Job version of `download_url`"""
    work_dir = job.fileStore.getLocalTempDir()
    fpath = download_url(job=job, url=url, work_dir=work_dir, name=name,
                         s3_key_path=s3_key_path, cghub_key_path=cghub_key_path)
    return job.fileStore.writeGlobalFile(fpath)


def _download_with_genetorrent(job, url, file_path, cghub_key_path):
    parsed_url = urlparse(url)
    analysis_id = parsed_url.path[1:]
    assert parsed_url.scheme == 'gnos', 'Improper format. gnos://cghub/ID. User supplied: {}'.format(parsed_url)
    work_dir = os.path.dirname(file_path)
    folder_path = os.path.join(work_dir, os.path.basename(analysis_id))
    parameters = ['-vv', '-c', cghub_key_path, '-d', analysis_id]
    docker_call(job=job, tool='quay.io/ucsc_cgl/genetorrent:3.8.7--9911761265b6f08bc3ef09f53af05f56848d805b',
                work_dir=work_dir, parameters=parameters)
    sample = glob.glob(os.path.join(folder_path, '*tar*'))
    assert len(sample) == 1, 'More than one sample tar in CGHub download: {}'.format(analysis_id)


def s3am_upload(job, fpath, s3_dir, num_cores=1, s3_key_path=None):
    """
    Uploads a file to s3 via S3AM
    S3AM binary must be on the PATH to use this function
    For SSE-C encryption: provide a path to a 32-byte file

    :param toil.job.Job job: Toil job that is calling this function
    :param str fpath: Path to file to upload
    :param str s3_dir: Ouptut S3 path. Format: s3://bucket/[directory]
    :param int num_cores: Number of cores to use for up/download with S3AM
    :param str s3_key_path: (OPTIONAL) Path to 32-byte key to be used for SSE-C encryption
    """
    require(s3_dir.startswith('s3://'), 'Format of s3_dir (s3://) is incorrect: %s', s3_dir)
    s3_dir = os.path.join(s3_dir, os.path.basename(fpath))
    _s3am_with_retry(job=job, num_cores=num_cores, file_path=fpath, s3_url=s3_dir, mode='upload', s3_key_path=s3_key_path)


def s3am_upload_job(job, file_id, file_name, s3_dir, s3_key_path=None):
    """Job version of s3am_upload"""
    work_dir = job.fileStore.getLocalTempDir()
    fpath = job.fileStore.readGlobalFile(file_id, os.path.join(work_dir, file_name))
    s3am_upload(job=job, fpath=fpath, s3_dir=s3_dir, num_cores=job.cores, s3_key_path=s3_key_path)


def _s3am_with_retry(job, num_cores, file_path, s3_url, mode='upload', s3_key_path=None):
    """
    Run s3am with 3 retries

    :param toil.job.Job job: Toil job that is calling this function
    :param int num_cores: Number of cores to pass to upload/download slots
    :param str file_path: Full path to the file
    :param str s3_url: S3 URL
    :param str mode: Mode to run s3am in. Either "upload" or "download"
    :param str s3_key_path: Path to the SSE-C key if using encryption
    """
    # try to find suitable credentials
    base_boto = '.boto'
    base_aws = '.aws/credentials'
    docker_home_dir = '/root'
    # map existing credential paths to their mount point within the container
    credentials_to_mount = {os.path.join(os.path.expanduser("~"), path): os.path.join(docker_home_dir, path)
                            for path in [base_aws, base_boto]
                            if os.path.exists(os.path.join(os.path.expanduser("~"), path))}
    require(os.path.isabs(file_path), "'file_path' parameter must be an absolute path")
    dir_path, file_name = file_path.rsplit('/', 1)
    # Mirror user specified paths to simplify debugging
    container_dir_path = '/data' + dir_path
    container_file = os.path.join(container_dir_path, file_name)
    mounts = {dir_path: container_dir_path}
    if s3_key_path:
        require(os.path.isabs(s3_key_path), "'s3_key_path' parameter must be an absolute path")
        key_dir_path, key_name = s3_key_path.rsplit('/', 1)
        container_key_dir_path = '/data' + key_dir_path
        container_key_file = os.path.join(container_key_dir_path, key_name)
        # if the key directory is identical to the file directory this assignment is idempotent
        mounts[key_dir_path] = container_key_dir_path
    for k, v in credentials_to_mount.iteritems():
        mounts[k] = v
    arguments = []
    url_arguments = []
    if mode == 'upload':
        arguments.extend(['upload', '--force', '--upload-slots=%s' % num_cores, '--exists=overwrite'])
        url_arguments.extend(['file://' + container_file, s3_url])
    elif mode == 'download':
        arguments.extend(['download', '--file-exists=overwrite', '--download-exists=discard'])
        url_arguments.extend([s3_url, 'file://' + container_file])
    else:
        raise ValueError('Improper mode specified. mode must be equal to "upload" or "download".')
    if s3_key_path:
        arguments.extend(['--sse-key-is-master', '--sse-key-file', container_key_file])
    arguments.extend(['--part-size=50M', '--download-slots=%s' % num_cores])
    # finally, add the url path arguments after all the tool parameters are set
    arguments.extend(url_arguments)
    # Pass credential-related environment variables into container
    env = {}
    if 'AWS_PROFILE' in os.environ:
        env['AWS_PROFILE'] = os.environ['AWS_PROFILE']
    # Run s3am with retries
    retry_count = 3
    for i in xrange(retry_count):
        try:
            docker_call(job=job, tool='quay.io/ucsc_cgl/s3am:2.0--fed932897e7fd40f4ec878362e5dd6afe15caaf0',
                        parameters=arguments, mounts=mounts, env=env)
        except subprocess.CalledProcessError:
            _log.debug('S3AM %s failed', mode, exc_info=True)
        else:
            _log.debug('S3AM %s succeeded', mode)
            return
    raise RuntimeError("S3AM failed to %s after %i retries with arguments %s. Enable 'debug' "
                       "level logging to see more information about the failed attempts." %
                       (mode, retry_count, arguments))
