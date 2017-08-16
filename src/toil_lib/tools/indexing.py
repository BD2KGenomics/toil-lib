import os

from toil.lib.docker import dockerCall


def run_bwa_index(job, ref_id):
    """
    Use BWA to create reference index files

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str ref_id: FileStoreID for the reference genome
    :return: FileStoreIDs for BWA index files
    :rtype: tuple(str, str, str, str, str)
    """
    work_dir = job.fileStore.getLocalTempDir()
    job.fileStore.readGlobalFile(ref_id, os.path.join(work_dir, 'ref.fa'))
    command = ['index', '/data/ref.fa']
    dockerCall(job=job, workDir=work_dir, parameters=command,
               tool='quay.io/ucsc_cgl/bwa:0.7.12--256539928ea162949d8a65ca5c79a72ef557ce7c')
    ids = {}
    for output in ['ref.fa.amb', 'ref.fa.ann', 'ref.fa.bwt', 'ref.fa.pac', 'ref.fa.sa']:
        ids[output.split('.')[-1]] = (job.fileStore.writeGlobalFile(os.path.join(work_dir, output)))
    job.fileStore.logToMaster('Created BWA index files')
    return ids


def run_samtools_faidx(job, ref_id):
    """
    Use Samtools to create reference index file

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str ref_id: FileStoreID for the reference genome
    :return: FileStoreID for reference index
    :rtype: str
    """
    job.fileStore.logToMaster('Created reference index')
    work_dir = job.fileStore.getLocalTempDir()
    job.fileStore.readGlobalFile(ref_id, os.path.join(work_dir, 'ref.fasta'))
    command = ['faidx', '/data/ref.fasta']
    dockerCall(job=job, workDir=work_dir, parameters=command,
               tool='quay.io/ucsc_cgl/samtools:0.1.19--dd5ac549b95eb3e5d166a5e310417ef13651994e')
    return job.fileStore.writeGlobalFile(os.path.join(work_dir, 'ref.fasta.fai'))


def run_bowtie2_index(job,
                      ref_id):
    '''
    '''
    work_dir = job.fileStore.getLocalTempDir()
    job.fileStore.readGlobalFile(ref_id, os.path.join(work_dir, 'ref.fa'))
    command = ['/data/ref.fa', '/data/ref']
    docker_parameters = ['--rm',
                         '--log-driver', 'none',
                         '-v', '{}:/data'.format(work_dir),
                         '--entrypoint=/opt/bowtie2/bowtie2-2.3.2/bowtie2-build']
    dockerCall(job=job,
               workDir=work_dir,
               parameters=command,
               dockerParameters=docker_parameters,
               tool='quay.io/ucsc_cgl/bowtie2')
    ids = {}
    for output in ['ref.1.bt2', 'ref.2.bt2', 'ref.3.bt2', 'ref.4.bt2',
                   'ref.rev.1.bt2', 'ref.rev.2.bt2']:
        ids[output] = (job.fileStore.writeGlobalFile(os.path.join(work_dir, output)))
    job.fileStore.logToMaster('Created bowtie2 index')
    return ids

    
def run_snap_index(job,
                   ref_id):
    '''
    '''
    work_dir = job.fileStore.getLocalTempDir()
    job.fileStore.readGlobalFile(ref_id, os.path.join(work_dir, 'ref.fa'))
    command = ['index', '/data/ref.fa', '/data/']
    dockerCall(job=job,
               workDir=work_dir,
               parameters=command,
               tool='quay.io/ucsc_cgl/snap')
    ids = {}
    for output in ['Genome', 'GenomeIndex', 'GenomeIndexHash', 'OverflowTable']:
        ids[output] = (job.fileStore.writeGlobalFile(os.path.join(work_dir, output)))
    job.fileStore.logToMaster('Created SNAP index')
    return ids
