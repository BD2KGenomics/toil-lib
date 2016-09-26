import os

from toil_lib.files import tarball_files
from toil_lib.programs import docker_call


def run_fastqc(job, r1_id, r2_id):
    """
    Run Fastqc on the input reads

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str r1_id: FileStoreID of fastq read 1
    :param str r2_id: FileStoreID of fastq read 2
    :return: FileStoreID of fastQC output (tarball)
    :rtype: str
    """
    work_dir = job.fileStore.getLocalTempDir()
    job.fileStore.readGlobalFile(r1_id, os.path.join(work_dir, 'R1.fastq'))
    parameters = ['/data/R1.fastq']
    output_names = ['R1_fastqc.html', 'R1_fastqc.zip']
    if r2_id:
        job.fileStore.readGlobalFile(r2_id, os.path.join(work_dir, 'R2.fastq'))
        parameters.extend(['-t', '2', '/data/R2.fastq'])
        output_names.extend(['R2_fastqc.html', 'R2_fastqc.zip'])
    docker_call(tool='quay.io/ucsc_cgl/fastqc:0.11.5--be13567d00cd4c586edf8ae47d991815c8c72a49',
                work_dir=work_dir, parameters=parameters)
    output_files = [os.path.join(work_dir, x) for x in output_names]
    tarball_files(tar_name='fastqc.tar.gz', file_paths=output_files, output_dir=work_dir)
    return job.fileStore.writeGlobalFile(os.path.join(work_dir, 'fastqc.tar.gz'))


def run_bam_qc(job, aligned_bam_id, uuid=None):
    """
    Run BAM QC as specified by California Kids Cancer Comparison (CKCC)

    :param JobFunctionWrappingJob job:
    :param str aligned_bam_id: FileStoreID of sorted bam from STAR
    :param None|str uuid: Will prefix to output names if set
    :return: boolean flag, FileStoreID for output bam, and FileStoreID for output tar
    :rtype: tuple(bool, str, str)
    """
    work_dir = job.fileStore.getLocalTempDir()
    job.fileStore.readGlobalFile(aligned_bam_id, os.path.join(work_dir, 'rnaAligned.sortedByCoord.out.bam'))
    docker_call(tool='hbeale/treehouse_bam_qc:1.0', work_dir=work_dir, parameters=['runQC.sh', str(job.cores)])

    # Tar Output files
    output_names = ['readDist.txt', 'rnaAligned.out.md.sorted.geneBodyCoverage.curves.pdf',
                    'rnaAligned.out.md.sorted.geneBodyCoverage.txt']
    output_files = [os.path.join(work_dir, x) for x in output_names]
    prefix = '' if None else uuid + '.'
    tar_path = tarball_files(tar_name='bam_qc.tar.gz', file_paths=output_files, output_dir=work_dir, prefix=prefix)

    # Save output BAM
    sorted_bam_id = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'rnaAligned.sortedByCoord.md.bam'))

    # Check for FAIL flag
    fail_flag = True if os.path.exists(os.path.join(work_dir, 'readDist.txt_FAIL_qc.txt')) else False

    return fail_flag, sorted_bam_id, job.fileStore.writeGlobalFile(tar_path)
