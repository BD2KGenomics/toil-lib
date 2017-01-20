import os
import subprocess
import filecmp
from contextlib import closing
from uuid import uuid4

from toil_lib.test import DockerCallTest
from toil.common import Toil
from toil.job import Job


class TestUrls(DockerCallTest):

    def test_download_url_job(self):
        from toil_lib.urls import download_url_job
        j = Job.wrapJobFn(download_url_job, 'www.google.com')
        with Toil(self.options) as toil:
            toil.start(j)

    def test_download_url(self):
        with Toil(self.options) as toil:
            toil.start(Job.wrapJobFn(download_url, self.tmpdir))

    def test_upload_and_downoad_with_encryption(self):
        with Toil(self.options) as toil:
            toil.start(Job.wrapJobFn(upload_and_download_with_encryption, self.tmpdir))


def download_url(job, tmpdir):
    from toil_lib.urls import download_url
    download_url(job, work_dir=tmpdir, url='www.google.com', name='testy')
    assert os.path.exists(os.path.join(tmpdir, 'testy'))


def upload_and_download_with_encryption(job, tmpdir):
    from toil_lib.urls import s3am_upload
    from toil_lib.urls import download_url
    from boto.s3.connection import S3Connection, Bucket, Key
    # Create temporary encryption key
    key_path = os.path.join(tmpdir, 'foo.key')
    subprocess.check_call(['dd', 'if=/dev/urandom', 'bs=1', 'count=32',
                           'of={}'.format(key_path)])
    # Create test file
    upload_fpath = os.path.join(tmpdir, 'upload_file')
    with open(upload_fpath, 'wb') as fout:
        fout.write(os.urandom(1024))
    # Upload file
    random_key = os.path.join('test/', str(uuid4()), 'upload_file')
    s3_url = os.path.join('s3://cgl-driver-projects/', random_key)
    try:
        s3_dir = os.path.split(s3_url)[0]
        s3am_upload(job, fpath=upload_fpath, s3_dir=s3_dir, s3_key_path=key_path)
        # Download the file
        download_url(job, url=s3_url, name='download_file', work_dir=tmpdir, s3_key_path=key_path)
        download_fpath = os.path.join(tmpdir, 'download_file')
        assert os.path.exists(download_fpath)
        assert filecmp.cmp(upload_fpath, download_fpath)
    finally:
        # Delete the Key. Key deletion never fails so we don't need to catch any exceptions
        with closing(S3Connection()) as conn:
            b = Bucket(conn, 'cgl-driver-projects')
            k = Key(b)
            k.key = random_key
            k.delete()
