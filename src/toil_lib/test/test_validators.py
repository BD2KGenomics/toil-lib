import os
from toil_lib.urls import download_url
from toil_lib.test import DockerCallTest
from toil.job import Job
from toil.common import Toil


class TestValidators(DockerCallTest):
    def test_bam_quickcheck(self):
        with Toil(self.options) as toil:
            toil.start(Job.wrapJobFn(generate_url, self.tmpdir))


def generate_url(job, tmpdir):
    from toil_lib.validators import bam_quickcheck
    good_bam_url = 's3://cgl-pipeline-inputs/exome/ci/chr6.normal.bam'
    bad_bam_url = 's3://cgl-pipeline-inputs/exome/ci/truncated.bam'
    download_url(job, url=good_bam_url, name='good.bam', work_dir=tmpdir)
    download_url(job, url=bad_bam_url, name='bad.bam', work_dir=tmpdir)
    assert bam_quickcheck(os.path.join(tmpdir, 'good.bam'))
    assert bam_quickcheck(os.path.join(tmpdir, 'bad.bam')) == False
