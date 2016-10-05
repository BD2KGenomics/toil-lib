import os
from toil_lib.urls import download_url
from toil_lib.test import DockerCallTest
from toil.job import Job
from toil.common import Toil

class TestValidators(DockerCallTest):
    def test_bam_quickcheck(self):
        from toil_lib.validators import bam_quickcheck
        good_bam_url = 's3://cgl-pipeline-inputs/exome/ci/chr6.normal.bam'
        bad_bam_url = 's3://cgl-pipeline-inputs/exome/ci/truncated.bam'
        good_bam_job = Job.wrapJobFn(download_url, url=good_bam_url, name='good.bam', work_dir=self.tmpdir)
        with Toil(self.options) as toil:
            toil.start(good_bam_job)
        bad_bam_job = Job.wrapJobFn(download_url, url=bad_bam_url, name='bad.bam', work_dir=self.tmpdir)
        with Toil(self.options) as toil:
            toil.start(bad_bam_job)
        assert bam_quickcheck(os.path.join(self.tmpdir, 'good.bam'))
        assert bam_quickcheck(os.path.join(self.tmpdir, 'bad.bam')) == False
