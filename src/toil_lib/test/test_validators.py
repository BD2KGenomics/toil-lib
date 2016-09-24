import os

from toil_lib.urls import download_url


def test_bam_quickcheck(tmpdir):
    from toil_lib.validators import bam_quickcheck
    work_dir = str(tmpdir)
    good_bam_url = 's3://cgl-pipeline-inputs/exome/ci/chr6.normal.bam'
    bad_bam_url = 's3://cgl-pipeline-inputs/exome/ci/truncated.bam'
    download_url(url=good_bam_url, name='good.bam', work_dir=work_dir)
    download_url(url=bad_bam_url, name='bad.bam', work_dir=work_dir)
    assert bam_quickcheck(os.path.join(work_dir, 'good.bam'))
    assert bam_quickcheck(os.path.join(work_dir, 'bad.bam')) == False
