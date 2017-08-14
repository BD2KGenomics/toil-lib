import logging
import os
import time

from toil.lib.docker import dockerCall

from toil_lib.tools import log_runtime

_log = logging.getLogger(__name__)

def run_freebayes(job, ref, ref_fai, bam, bai, chunksize=None):
    '''
    Calls FreeBayes to call variants.

    If job.cores > 1 and chunksize is not None, then runs FreeBayes
    parallel mode. If not, then runs single threaded FreeBayes.

    :param JobFunctionWrappingJob job: passed automatically by Toil.
    :param str ref: The reference FASTA FileStoreID.
    :param str ref_fai: The reference FASTA index FileStoreID.
    :param str bam: The FileStoreID of the BAM to call.
    :param str bai: The FileStoreID of the BAM index.
    :param int chunksize: The size of chunks to split into and call in parallel.
      Defaults to None.
    '''
    work_dir = job.fileStore.getLocalTempDir()
    file_ids = [ref, ref_fai, bam, bai]
    file_names = ['ref.fa', 'ref.fa.fai', 'sample.bam', 'sample.bam.bai']
    for file_store_id, name in zip(file_ids, file_names):
        job.fileStore.readGlobalFile(file_store_id, os.path.join(work_dir, name))

    output_vcf = os.path.join(work_dir, 'sample.vcf')

    if job.cores > 1 and chunksize:
        _log.info('Running FreeBayes parallel with %dbp chunks and %d cores',
                  chunksize,
                  job.cores)
        
        docker_parameters = ['--rm',
                             '--log-driver', 'none',
                             '-v', '{}:/data'.format(work_dir),
                             '--entrypoint=/opt/cgl-docker-lib/freebayes/scripts/fasta_generate_regions.py']
        start_time = time.time()
        dockerCall(job,
                   workDir=work_dir,
                   dockerParameters=docker_parameters,
                   parameters=['/data/ref.fa.fai', str(chunksize)],
                   tool='quay.io/ucsc_cgl/freebayes',
                   outfile=open(os.path.join(work_dir, 'regions')))
        end_time = time.time()
        log_runtime(job, start_time, end_time, 'fasta_generate_regions')

        docker_parameters = ['--rm',
                             '--log-driver', 'none',
                             '-v', '{}:/data'.format(work_dir),
                             '--entrypoint=/opt/cgl-docker-lib/freebayes/scripts/freebayes-parallel']
        start_time = time.time()
        dockerCall(job=job,
                   workDir=work_dir,
                   dockerParameters=docker_parameters,
                   parameters=['/data/regions',
                               str(job.cores),
                               '-f', '/data/ref.fa',
                               '/data/sample.bam'],
                   tool='quay.io/ucsc_cgl/freebayes',
                   outfile=open(output_vcf))
        end_time = time.time()
        log_runtime(job, start_time, end_time, 'FreeBayes Parallel')
                   
    else:
        _log.info('Running FreeBayes single threaded.')

        start_time = time.time()
        dockerCall(job=job,
                   workDir=work_dir,
                   parameters=['-f', '/data/ref.fa',
                               '/data/sample.bam'],
                   tool='quay.io/ucsc_cgl/freebayes',
                   outfile=open(output_vcf))
        end_time = time.time()
        log_runtime(job, start_time, end_time, 'FreeBayes')

    return job.fileStore.writeGlobalFile(output_vcf)


def run_platypus(job, ref, ref_fai, bam, bai, assemble=False):
    '''
    Runs Platypus to call variants.

    :param JobFunctionWrappingJob job: passed automatically by Toil.
    :param str ref: The reference FASTA FileStoreID.
    :param str ref_fai: The reference FASTA index FileStoreID.
    :param str bam: The FileStoreID of the BAM to call.
    :param str bai: The FileStoreID of the BAM index.
    :param boolean assemble: If true, runs Platypus in assembler mode.
    '''
    work_dir = job.fileStore.getLocalTempDir()
    file_ids = [ref, ref_fai, bam, bai]
    file_names = ['ref.fa', 'ref.fa.fai', 'sample.bam', 'sample.bam.bai']
    for file_store_id, name in zip(file_ids, file_names):
        job.fileStore.readGlobalFile(file_store_id, os.path.join(work_dir, name))

    parameters = ['callVariants',
                  '--refFile', '/data/ref.fa',
                  '--outputFile', '/data/sample.vcf',
                  '--bamFiles', '/data/sample.bam']

    if job.cores > 1:
        parameters.extend(['--nCPU', str(job.cores)])

    if assemble:
        parameters.append('--assemble')

    start_time = time.time()
    dockerCall(job=job,
               workDir=work_dir,
               parameters=parameters,
               tool='quay.io/ucsc_cgl/platypus')
    end_time = time.time()
    log_runtime(job, start_time, end_time, 'Platypus, assemble={}'.format(assemble))
    
    return job.fileStore.writeGlobalFile(os.path.join(work_dir, 'sample.vcf'))


def run_16gt(job, ref, genome_index, bam, dbsnp, sample_name):
    '''
    Generates the snapshot file and calls variants using 16GT.

    :param JobFunctionWrappingJob job: passed automatically by Toil.
    :param str ref: The reference FASTA FileStoreID.
    :param str genome_index: The FileStoreID of the SOAP3-dp genome index.
    :param str bam: The FileStoreID of the BAM to call.
    :param str dbsnp: The FileStoreID of the dbSNP VCF for filtration.
    :param str sample_name: The name of the sample being called.
    '''
    work_dir = job.fileStore.getLocalTempDir()
    file_ids = [ref, genome_index, bam, dbsnp]
    file_names = ['ref.fa', 'ref.fa.index', 'sample.bam', 'dbsnp.vcf']
    for file_store_id, name in zip(file_ids, file_names):
        job.fileStore.readGlobalFile(file_store_id, os.path.join(work_dir, name))

    start_time = time.time()
    dockerCall(job=job,
               workDir=work_dir,
               parameters=['/opt/cgl-docker-lib/16GT/bam2snapshot',
                           '-i', '/data/ref.fa.index',
                           '-b', '/data/sample.bam',
                           '-o', '/data/sample'],
               tool='quay.io/ucsc_cgl/16gt')
    end_time = time.time()
    log_runtime(job, start_time, end_time, '16gt bam2snapshot')

    start_time = time.time()
    dockerCall(job=job,
               workDir=work_dir,
               parameters=['/opt/cgl-docker-lib/16GT/snapshotSnpcaller',
                           '-i', '/data/ref.fa.index',
                           '-o', '/data/sample'],
               tool='quay.io/ucsc_cgl/16gt')
    end_time = time.time()
    log_runtime(job, start_time, end_time, '16gt snapshotSnpcaller')

    start_time = time.time()
    dockerCall(job=job,
               workDir=work_dir,
               parameters=['perl', '/opt/cgl-docker-lib/16GT/txt2vcf.pl',
                           '/data/sample',
                           sample_name,
                           '/data/ref.fa'],
               tool='quay.io/ucsc_cgl/16gt',
               outfile=open(os.path.join(work_dir, 'sample.vcf')))
    end_time = time.time()
    log_runtime(job, start_time, end_time, '16gt txt2vcf')

    start_time = time.time()
    dockerCall(job=job,
               workDir=work_dir,
               parameters=['perl', '/opt/cgl-docker-lib/16GT/filterVCF.pl',
                           '/data/sample.vcf',
                           '/data/dbsnp.vcf'],
               tool='quay.io/ucsc_cgl/16gt',
               outfile=open(os.path.join(work_dir, 'sample.filtered.vcf')))
    end_time = time.time()
    log_runtime(job, start_time, end_time, '16gt filterVCF')

    return job.fileStore.writeGlobalFile(os.path.join(work_dir, 'sample.filtered.vcf'))
    
        
def run_strelka(job, ref, ref_fai, bam, bai, candidate_indels=None):
    '''
    Runs Strelka's germline single sample caller.

    :param JobFunctionWrappingJob job: passed automatically by Toil.
    :param str ref: The reference FASTA FileStoreID.
    :param str ref_fai: The reference FASTA index FileStoreID.
    :param str bam: The FileStoreID of the BAM to call.
    :param str bai: The FileStoreID of the BAM index.
    :param str candidate_indels: The optional FileStoreID of the candidate
       indel GZIPed VCF.
    '''
    generate_parameters = ['/opt/strelka/bin/configureStrelkaGermlineWorkflow.py',
                           '--bam', '/data/sample.bam',
                           '--referenceFasta', '/data/ref.fa',
                           '--runDir', '/data/']
    work_dir = job.fileStore.getLocalTempDir()
    file_ids = [ref, ref_fai, bam, bai]
    file_names = ['ref.fa', 'ref.fa.fai', 'sample.bam', 'sample.bam.bai']

    if candidate_indels:
        _log.info('Candidate indels from Manta were provided for Strelka.')
        file_ids.append(candidate_indels)
        file_names.append('candidateSmallIndels.vcf.gz')
        generate_parameters.extend(['--indelCandidates', '/data/candidateSmallIndels.vcf.gz'])
    else:
        _log.info('No candidate indels provided.')

    for file_store_id, name in zip(file_ids, file_names):
        job.fileStore.readGlobalFile(file_store_id, os.path.join(work_dir, name))
    
    start_time = time.time()
    dockerCall(job=job,
               workDir=work_dir,
               parameters=generate_parameters,
               tool='quay.io/ucsc_cgl/strelka')
    end_time = time.time()
    log_runtime(job, start_time, end_time, 'Configuring Strelka')

    start_time = time.time()
    dockerCall(job=job,
               workDir=work_dir,
               parameters=['/data/runWorkflow.py',
                           '-m', 'local',
                           '-j', str(job.cores)],
               tool='quay.io/ucsc_cgl/strelka')
    end_time = time.time()
    log_runtime(job, start_time, end_time, 'Strelka')

    return job.fileStore.writeGlobalFile(os.path.join(work_dir, 'variants.vcf.gz'))


def run_manta(job, ref, ref_fai, bam, bai, candidate_indels=None):
    '''
    Runs Manta's germline single sample caller.

    :param JobFunctionWrappingJob job: passed automatically by Toil.
    :param str ref: The reference FASTA FileStoreID.
    :param str ref_fai: The reference FASTA index FileStoreID.
    :param str bam: The FileStoreID of the BAM to call.
    :param str bai: The FileStoreID of the BAM index.
    :param str candidate_indels: The optional FileStoreID of the candidate
       indel GZIPed VCF.
    '''
    work_dir = job.fileStore.getLocalTempDir()
    file_ids = [ref, ref_fai, bam, bai]
    file_names = ['ref.fa', 'ref.fa.fai', 'sample.bam', 'sample.bam.bai']
    for file_store_id, name in zip(file_ids, file_names):
        job.fileStore.readGlobalFile(file_store_id, os.path.join(work_dir, name))
    
    start_time = time.time()
    dockerCall(job=job,
               workDir=work_dir,
               parameters=['/opt/manta/bin/configManta.py',
                           '--normalBam', '/data/sample.bam',
                           '--referenceFasta', '/data/ref.fa',
                           '--runDir', '/data/'],
               tool='quay.io/ucsc_cgl/manta')
    end_time = time.time()
    log_runtime(job, start_time, end_time, 'Configuring Manta')

    start_time = time.time()
    dockerCall(job=job,
               workDir=work_dir,
               parameters=['/data/runWorkflow.py',
                           '-m', 'local',
                           '-j', str(job.cores)],
               tool='quay.io/ucsc_cgl/manta')
    end_time = time.time()
    log_runtime(job, start_time, end_time, 'Manta')

    return (job.fileStore.writeGlobalFile(os.path.join(work_dir, 'diploidSV.vcf.gz')),
            job.fileStore.writeGlobalFile(os.path.join(work_dir, 'candidateSmallIndels.vcf.gz')))


def run_samtools_mpileup(job, ref, ref_fai, bam, bai):
    '''
    Runs the samtools mpileup variant caller.
    
    :param JobFunctionWrappingJob job: passed automatically by Toil.
    :param str ref: The reference FASTA FileStoreID.
    :param str ref_fai: The reference FASTA index FileStoreID.
    :param str bam: The FileStoreID of the BAM to call.
    :param str bai: The FileStoreID of the BAM index.
    '''
    work_dir = job.fileStore.getLocalTempDir()
    file_ids = [ref, ref_fai, bam, bai]
    file_names = ['ref.fa', 'ref.fa.fai', 'sample.bam', 'sample.bam.bai']
    for file_store_id, name in zip(file_ids, file_names):
        job.fileStore.readGlobalFile(file_store_id, os.path.join(work_dir, name))

    start_time = time.time()
    dockerCall(job=job,
               workDir=work_dir,
               parameters=['mpileup',
                           '-f', '/data/ref.fa',
                           '-o', '/data/sample.vcf.gz',
                           '/data/sample.bam'],
               tool='quay.io/ucsc_cgl/samtools')
    end_time = time.time()
    log_runtime(job, start_time, end_time, 'samtools mpileup')
    
    return job.fileStore.writeGlobalFile(os.path.join(work_dir, 'sample.vcf.gz'))


def run_bcftools_call(job, vcf_gz):
    '''
    Runs the bcftools call command.
    
    :param JobFunctionWrappingJob job: passed automatically by Toil.
    :param str vcf_gz: The FileStoreID of the GZIPed VCF.
    '''
    work_dir = job.fileStore.getLocalTempDir()
    job.fileStore.readGlobalFile(vcf_gz, os.path.join(work_dir, 'sample.vcf.gz'))

    start_time = time.time()
    dockerCall(job=job,
               workDir=work_dir,
               parameters=['call',
                           '-o', '/data/sample.calls.vcf.gz',
                           '--threads', str(job.cores),
                           '/data/sample.vcf.gz'],
               tool='quay.io/ucsc_cgl/bcftools')
    end_time = time.time()
    log_runtime(job, start_time, end_time, 'bcftools call')
    
    return job.fileStore.writeGlobalFile(os.path.join(work_dir, 'sample.calls.vcf.gz'))


def run_gatk3_haplotype_caller(job, ref, ref_fai, ref_dict, bam, bai,
                               emit_threshold=10.0,
                               call_threshold=30.0):
    '''
    Runs the GATK3's HaplotypeCaller.

    :param str ref: The reference FASTA FileStoreID.
    :param str ref_fai: The reference FASTA index FileStoreID.
    :param str ref_dict: The reference sequence dictionary FileStoreID.
    :param str bam: The FileStoreID of the BAM to call.
    :param str bai: The FileStoreID of the BAM index.
    '''    
    work_dir = job.fileStore.getLocalTempDir()
    file_ids = [ref, ref_fai, ref_dict, bam, bai]
    file_names = ['ref.fa', 'ref.fa.fai', 'ref.dict', 'sample.bam', 'sample.bam.bai']
    for file_store_id, name in zip(file_ids, file_names):
        job.fileStore.readGlobalFile(file_store_id, os.path.join(work_dir, name))

    command = ['-T', 'HaplotypeCaller',
               '-nct', str(job.cores),
               '-R', 'genome.fa',
               '-I', 'input.bam',
               '-o', 'output.g.vcf',
               '-stand_call_conf', str(call_threshold),
               '-stand_emit_conf', str(emit_threshold),
               '-variant_index_type', 'LINEAR',
               '-variant_index_parameter', '128000',
               '--genotyping_mode', 'Discovery',
               '--emitRefConfidence', 'GVCF']

    # Set TMPDIR to /data to prevent writing temporary files to /tmp
    docker_parameters = ['--rm',
                         '--log-driver', 'none',
                         '-e', 'JAVA_OPTS=-Djava.io.tmpdir=/data/ -Xmx{}'.format(job.memory),
                         '-v', '{}:/data'.format(work_dir)]
    start_time = time.time()
    dockerCall(job=job, tool='quay.io/ucsc_cgl/gatk:3.5--dba6dae49156168a909c43330350c6161dc7ecc2',
               workDir=work_dir,
               parameters=parameters,
               dockerParameters=docker_parameters)
    end_time = time.time()
    log_runtime(job, start_time, end_time, "GATK3 HaplotypeCaller")

    return job.fileStore.writeGlobalFile(os.path.join(work_dir, 'output.g.vcf'))
