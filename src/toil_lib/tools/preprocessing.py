import os

from toil.job import PromisedRequirement

from toil_lib import require
from toil_lib.programs import docker_call


def run_cutadapt(job, r1_id, r2_id, fwd_3pr_adapter, rev_3pr_adapter):
    """
    Adapter trimming for RNA-seq data

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str r1_id: FileStoreID of fastq read 1
    :param str r2_id: FileStoreID of fastq read 2 (if paired data)
    :param str fwd_3pr_adapter: Adapter sequence for the forward 3' adapter
    :param str rev_3pr_adapter: Adapter sequence for the reverse 3' adapter (second fastq pair)
    :return: R1 and R2 FileStoreIDs
    :rtype: tuple
    """
    work_dir = job.fileStore.getLocalTempDir()
    if r2_id:
        require(rev_3pr_adapter, "Paired end data requires a reverse 3' adapter sequence.")
    # Retrieve files
    parameters = ['-a', fwd_3pr_adapter,
                  '-m', '35']
    if r1_id and r2_id:
        job.fileStore.readGlobalFile(r1_id, os.path.join(work_dir, 'R1.fastq'))
        job.fileStore.readGlobalFile(r2_id, os.path.join(work_dir, 'R2.fastq'))
        parameters.extend(['-A', rev_3pr_adapter,
                           '-o', '/data/R1_cutadapt.fastq',
                           '-p', '/data/R2_cutadapt.fastq',
                           '/data/R1.fastq', '/data/R2.fastq'])
    else:
        job.fileStore.readGlobalFile(r1_id, os.path.join(work_dir, 'R1.fastq'))
        parameters.extend(['-o', '/data/R1_cutadapt.fastq', '/data/R1.fastq'])
    # Call: CutAdapt
    docker_call(tool='quay.io/ucsc_cgl/cutadapt:1.9--6bd44edd2b8f8f17e25c5a268fedaab65fa851d2',
                work_dir=work_dir, parameters=parameters)
    # Write to fileStore
    if r1_id and r2_id:
        r1_cut_id = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'R1_cutadapt.fastq'))
        r2_cut_id = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'R2_cutadapt.fastq'))
    else:
        r1_cut_id = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'R1_cutadapt.fastq'))
        r2_cut_id = None
    return r1_cut_id, r2_cut_id


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
    command = ['faidx', 'ref.fasta']
    docker_call(work_dir=work_dir, parameters=command,
                tool='quay.io/ucsc_cgl/samtools:0.1.19--dd5ac549b95eb3e5d166a5e310417ef13651994e')
    return job.fileStore.writeGlobalFile(os.path.join(work_dir, 'ref.fasta.fai'))


def run_samtools_index(job, bam):
    """
    Runs SAMtools index to create (.bai) files

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str bam: FileStoreID of the bam file
    :return: BAM index FileStoreID
    :rtype: str
    """
    work_dir = job.fileStore.getLocalTempDir()
    job.fileStore.readGlobalFile(bam, os.path.join(work_dir, 'sample.bam'))
    # Call: index the bam
    parameters = ['index', '/data/sample.bam']
    docker_call(work_dir=work_dir, parameters=parameters,
                tool='quay.io/ucsc_cgl/samtools:0.1.19--dd5ac549b95eb3e5d166a5e310417ef13651994e')
    # Write to fileStore
    return job.fileStore.writeGlobalFile(os.path.join(work_dir, 'sample.bam.bai'))


def run_samtools_sort(job, bam):
    """
    Sorts BAM file using SAMtools sort

    :param JobFunctionWrappingJob job: Toil Job instance
    :param str bam: BAM FileStoreID
    :return: sorted BAM FileStoreID
    :rtype: str
    """
    work_dir = job.fileStore.getLocalTempDir()
    job.fileStore.readGlobalFile(bam, os.path.join(work_dir, 'input.bam'))
    command = ['sort',
               '-@', str(job.cores),
               '-o', '/data/output.bam',
               '/data/input.bam']
    docker_call(work_dir=work_dir,
                parameters=command,
                tool='quay.io/ucsc_cgl/samtools:1.3--256539928ea162949d8a65ca5c79a72ef557ce7c',
                outputs={'output.bam': None})
    return job.fileStore.writeGlobalFile(os.path.join(work_dir, 'output.bam'))


def run_picard_create_sequence_dictionary(job, ref_id):
    """
    Use Picard-tools to create reference dictionary

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str ref_id: FileStoreID for the reference genome
    :return: FileStoreID for reference dictionary
    :rtype: str
    """
    job.fileStore.logToMaster('Created reference dictionary')
    work_dir = job.fileStore.getLocalTempDir()
    job.fileStore.readGlobalFile(ref_id, os.path.join(work_dir, 'ref.fasta'))
    command = ['CreateSequenceDictionary', 'R=ref.fasta', 'O=ref.dict']
    docker_call(work_dir=work_dir,
                parameters=command,
                tool='quay.io/ucsc_cgl/picardtools:1.95--dd5ac549b95eb3e5d166a5e310417ef13651994e')
    return job.fileStore.writeGlobalFile(os.path.join(work_dir, 'ref.dict'))


def picard_mark_duplicates(job, bam, bai):
    """
    Runs picardtools MarkDuplicates. Assumes the BAM file is coordinate sorted.

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param bam: BAM FileStoreID
    :param bai: BAI FileStoreID
    :return: Processed BAM and BAI FilestoreIDs
    :rtype: tuple
    """
    work_dir = job.fileStore.getLocalTempDir()

    # Retrieve file path
    job.fileStore.readGlobalFile(bam, os.path.join(work_dir, 'sorted.bam'))
    job.fileStore.readGlobalFile(bai, os.path.join(work_dir, 'sorted.bai'))

    # Call: picardtools
    command = ['MarkDuplicates',
               'INPUT=sorted.bam',
               'OUTPUT=mkdups.bam',
               'METRICS_FILE=metrics.txt',
               'ASSUME_SORTED=true',
               'CREATE_INDEX=true',
               'VALIDATION_STRINGENCY=LENIENT']  # Ignores minor formatting issues

    docker_call(work_dir=work_dir,
                parameters=command,
                # picard-tools container doesn't have JAVA_OPTS variable
                env={'_JAVA_OPTIONS': '-Djava.io.tmpdir=/data/ -Xmx{}'.format(job.memory)},
                tool='quay.io/ucsc_cgl/picardtools:1.95--dd5ac549b95eb3e5d166a5e310417ef13651994e',
                outputs={'mkdups.bam': None, 'mkdups.bai': None})

    bam = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'mkdups.bam'))
    bai = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'mkdups.bai'))
    return bam, bai


def run_gatk_preprocessing(job, bam, bai, ref, ref_dict, fai, phase, mills, dbsnp, unsafe=False):
    """
    GATK Preprocessing Pipeline
    0: Mark Duplicates
    1: Create INDEL realignment intervals
    2: Realign INDELS
    3: Recalibrate Base Quality Scores
    4: Recalibrate Reads

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str bam: Sample BAM FileStoreID
    :param str bai: Bam Index FileStoreID
    :param str ref: Reference genome FileStoreID
    :param str ref_dict: Reference dictionary FileStoreID
    :param str fai: Reference index FileStoreID
    :param str phase: 1000G Indels VCF FileStoreID
    :param str mills: Mills Indels VCF FileStoreID
    :param str dbsnp: dbSNP VCF FileStoreID
    :param bool unsafe: If True, runs gatk UNSAFE mode: "-U ALLOW_SEQ_DICT_INCOMPATIBILITY"
    :return: BAM and BAI FileStoreIDs
    :rtype: tuple(str, str)
    """
    # MarkDuplicates runs best when Xmx <= 10G
    mdups_memory = min(10737418240, job.memory)

    # The MarkDuplicates disk requirement depends on the input BAM and BAI files and the output
    # BAM and BAI files. The output BAM file is approximately the same size as the input BAM file.
    mdups_disk = PromisedRequirement(lambda bam_, bai_id: 2 * (bam_.size + bai_id.size), bam, bai)
    mdups = job.wrapJobFn(picard_mark_duplicates,
                          bam,
                          bai,
                          cores=job.cores,
                          disk=mdups_disk,
                          memory=mdups_memory)

    # Get size of genome reference files for calculating disk requirements
    genome_ref_size = ref.size + ref_dict.size + fai.size

    # Get size of INDEL resource files and genome reference files
    indel_ref_size = mills.size + phase.size + genome_ref_size

    # Estimate disk resources as twice the input bam size and bai size plus reference data
    realigner_target_disk = PromisedRequirement(lambda bam_, bai_id, ref_size:
                                                2 * bam_.size + bai_id.size + ref_size,
                                                mdups.rv(0),
                                                mdups.rv(1),
                                                indel_ref_size)

    realigner_target = job.wrapJobFn(run_realigner_target_creator,
                                     mdups.rv(0),
                                     mdups.rv(1),
                                     ref, ref_dict, fai,
                                     phase, mills,
                                     unsafe=unsafe,
                                     cores=job.cores,
                                     disk=realigner_target_disk,
                                     memory=job.memory)

    # The INDEL realignment disk requirement depends on the input BAM and BAI files, the intervals
    # file, the variant resource files, and the output BAM and BAI files. Here, we assume the
    # output BAM and BAI files are approximately the same size as the input BAM and BAI files.
    indel_realign_disk = PromisedRequirement(lambda bam_, bai_id, intervals, ref_size:
                                             2 * (
                                             bam_.size + bai_id.size) + intervals.size + ref_size,
                                             mdups.rv(0),
                                             mdups.rv(1),
                                             realigner_target.rv(),
                                             indel_ref_size)

    indel_realign = job.wrapJobFn(run_indel_realignment,
                                  realigner_target.rv(),
                                  mdups.rv(0),
                                  mdups.rv(1),
                                  ref, ref_dict, fai,
                                  phase, mills,
                                  unsafe=unsafe,
                                  cores=job.cores,
                                  disk=indel_realign_disk,
                                  memory=job.memory)

    # Get size of BQSR databases and genome reference files
    bqsr_ref_size = dbsnp.size + mills.size + genome_ref_size

    # The BQSR disk requirement depends on the input BAM and BAI sizes, the reference files, and
    # the output recalibration table size.
    base_recal_disk = PromisedRequirement(lambda bam_, bai_id, ref_size:
                                          2 * bam_.size + bai_id.size + ref_size,
                                          indel_realign.rv(0),
                                          indel_realign.rv(1),
                                          bqsr_ref_size)

    base_recal = job.wrapJobFn(run_base_recalibration,
                               indel_realign.rv(0),
                               indel_realign.rv(1),
                               ref, ref_dict, fai,
                               dbsnp, mills,
                               unsafe=unsafe,
                               cores=job.cores,
                               disk=base_recal_disk,
                               memory=job.memory)

    # The PrintReads disk requirement depends on the bam and bai sizes, the recal table, the
    # genome reference files, and the output bam and bai file
    recalibrate_reads_disk = PromisedRequirement(lambda bam_, bai_id, recal, ref_size:
                                                 2 * (bam_.size + bai_id.size) + recal.size + ref_size,
                                                 indel_realign.rv(0),
                                                 indel_realign.rv(1),
                                                 base_recal.rv(),
                                                 genome_ref_size)

    recalibrate_reads = job.wrapJobFn(apply_bqsr_recalibration,
                                      base_recal.rv(),
                                      indel_realign.rv(0),
                                      indel_realign.rv(1),
                                      ref, ref_dict, fai,
                                      unsafe=unsafe,
                                      cores=job.cores,
                                      disk=recalibrate_reads_disk,
                                      memory=job.memory)

    job.addChild(mdups)
    mdups.addChild(realigner_target)
    realigner_target.addChild(indel_realign)
    indel_realign.addChild(base_recal)
    base_recal.addChild(recalibrate_reads)
    return recalibrate_reads.rv(0), recalibrate_reads.rv(1)


def run_realigner_target_creator(job, bam, bai, ref, ref_dict, fai, phase, mills, unsafe=False):
    """
    Creates intervals file needed for INDEL realignment

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str bam: Sample BAM FileStoreID
    :param str bai: Bam Index FileStoreID
    :param str ref: Reference genome FileStoreID
    :param str ref_dict: Reference dictionary FileStoreID
    :param str fai: Reference index FileStoreID
    :param str phase: 1000G Indels VCF FileStoreID
    :param str mills: Mills Indels VCF FileStoreID
    :param bool unsafe: If True, runs gatk UNSAFE mode: "-U ALLOW_SEQ_DICT_INCOMPATIBILITY"
    :return: FileStoreID for the processed bam
    :rtype: str
    """
    inputs = {'ref.fasta': ref,
              'ref.fasta.fai': fai,
              'ref.dict': ref_dict,
              'input.bam': bam,
              'input.bai': bai,
              '1000G.vcf': phase,
              'mills.vcf': mills}

    work_dir = job.fileStore.getLocalTempDir()
    for name, file_store_id in inputs.iteritems():
        job.fileStore.readGlobalFile(file_store_id, os.path.join(work_dir, name))

    # Call: GATK -- RealignerTargetCreator
    parameters = ['-T', 'RealignerTargetCreator',
                  '-R', '/data/ref.fasta',
                  '-I', '/data/input.bam',
                  '-known', '/data/1000G.vcf',
                  '-known', '/data/mills.vcf',
                  '--downsampling_type', 'NONE',
                  '-o', '/data/sample.intervals']
    if unsafe:
        parameters.extend(['-U', 'ALLOW_SEQ_DICT_INCOMPATIBILITY'])

    docker_call(tool='quay.io/ucsc_cgl/gatk:3.5--dba6dae49156168a909c43330350c6161dc7ecc2',
                inputs=inputs.keys(),
                outputs={'sample.intervals': None},
                work_dir=work_dir,
                parameters=parameters,
                env=dict(JAVA_OPTS='-Djava.io.tmpdir=/data/ -Xmx{}'.format(job.memory)))
    # Write to fileStore
    return job.fileStore.writeGlobalFile(os.path.join(work_dir, 'sample.intervals'))


def run_indel_realignment(job, intervals, bam, bai, ref, ref_dict, fai, phase, mills, unsafe=False):
    """
    Realigns BAM file at realignment target intervals

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str intervals: Indel interval FileStoreID
    :param str bam: Sample BAM FileStoreID
    :param str bai: Bam Index FileStoreID
    :param str ref: Reference genome FileStoreID
    :param str ref_dict: Reference dictionary FileStoreID
    :param str fai: Reference index FileStoreID
    :param str phase: 1000G Indels VCF FileStoreID
    :param str mills: Mills Indels VCF FileStoreID
    :param bool unsafe: If True, runs gatk UNSAFE mode: "-U ALLOW_SEQ_DICT_INCOMPATIBILITY"
    :return: FileStoreID for the processed bam
    :rtype: tuple(str, str)
    """
    inputs = {'ref.fasta': ref,
              'ref.fasta.fai': fai,
              'ref.dict': ref_dict,
              'input.bam': bam,
              'input.bai': bai,
              'target.intervals': intervals,
              'phase.vcf': phase,
              'mills.vcf': mills}

    work_dir = job.fileStore.getLocalTempDir()
    for name, file_store_id in inputs.iteritems():
        job.fileStore.readGlobalFile(file_store_id, os.path.join(work_dir, name))

    # Call: GATK -- IndelRealigner
    parameters = ['-T', 'IndelRealigner',
                  '-R', '/data/ref.fasta',
                  '-I', '/data/input.bam',
                  '-known', '/data/phase.vcf',
                  '-known', '/data/mills.vcf',
                  '-targetIntervals', '/data/target.intervals',
                  '--downsampling_type', 'NONE',
                  '-maxReads', str(720000),  # Taken from MC3 pipeline
                  '-maxInMemory', str(5400000),  # Taken from MC3 pipeline
                  '-o', '/data/output.bam']
    if unsafe:
        parameters.extend(['-U', 'ALLOW_SEQ_DICT_INCOMPATIBILITY'])
    docker_call(tool='quay.io/ucsc_cgl/gatk:3.5--dba6dae49156168a909c43330350c6161dc7ecc2',
                inputs=inputs.keys(),
                outputs={'output.bam': None, 'output.bai': None},
                work_dir=work_dir,
                parameters=parameters,
                env=dict(JAVA_OPTS='-Djava.io.tmpdir=/data/ -Xmx{}'.format(job.memory)))

    indel_bam = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'output.bam'))
    indel_bai = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'output.bai'))
    return indel_bam, indel_bai


def run_base_recalibration(job, bam, bai, ref, ref_dict, fai, dbsnp, mills, unsafe=False):
    """
    Creates recalibration table for Base Quality Score Recalibration

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str bam: BAM FileStoreID
    :param str bai: BAM Index FileStoreID
    :param str ref: Reference genome FileStoreID
    :param str ref_dict: Reference dictionary FileStoreID
    :param str fai: Reference index FileStoreID
    :param str dbsnp: dbSNP VCF FileStoreID
    :param str mills: Mills VCF FileStoreID
    :param bool unsafe: If True, runs gatk UNSAFE mode: "-U ALLOW_SEQ_DICT_INCOMPATIBILITY"
    :return: FileStoreID for the processed bam
    :rtype: str
    """
    inputs = {'ref.fasta': ref,
              'ref.fasta.fai': fai,
              'ref.dict': ref_dict,
              'input.bam': bam,
              'input.bai': bai,
              'dbsnp.vcf': dbsnp,
              'mills.vcf': mills}

    work_dir = job.fileStore.getLocalTempDir()
    for name, file_store_id in inputs.iteritems():
        job.fileStore.readGlobalFile(file_store_id, os.path.join(work_dir, name))

    # Call: GATK -- BaseRecalibrator
    parameters = ['-T', 'BaseRecalibrator',
                  '-nct', str(job.cores),
                  '-R', '/data/ref.fasta',
                  '-I', '/data/input.bam',
                  '-knownSites', '/data/dbsnp.vcf',
                  '-knownSites', '/data/mills.vcf',
                  '-o', '/data/recal_data.table']

    if unsafe:
        parameters.extend(['-U', 'ALLOW_SEQ_DICT_INCOMPATIBILITY'])

    docker_call(tool='quay.io/ucsc_cgl/gatk:3.5--dba6dae49156168a909c43330350c6161dc7ecc2',
                inputs=inputs.keys(),
                outputs={'recal_data.table': None},
                work_dir=work_dir,
                parameters=parameters,
                env=dict(JAVA_OPTS='-Djava.io.tmpdir=/data/ -Xmx{}'.format(job.memory)))

    return job.fileStore.writeGlobalFile(os.path.join(work_dir, 'recal_data.table'))


def apply_bqsr_recalibration(job, table, bam, bai, ref, ref_dict, fai, unsafe=False):
    """
    Creates BAM with recalibrated base quality scores

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str table: Recalibration table FileStoreID
    :param str bam: Bam FileStoreID
    :param str bai: Bam Index FileStoreID
    :param str ref: Reference genome FileStoreID
    :param str ref_dict: Reference dictionary FileStoreID
    :param str fai: Reference index FileStoreID
    :param bool unsafe: If True, runs gatk UNSAFE mode: "-U ALLOW_SEQ_DICT_INCOMPATIBILITY"
    :return: BAM and BAI FileStoreIDs
    :rtype: tuple(str, str)
    """
    inputs = {'ref.fasta': ref,
              'ref.fasta.fai': fai,
              'ref.dict': ref_dict,
              'recal.table': table,
              'input.bam': bam,
              'input.bai': bai}

    work_dir = job.fileStore.getLocalTempDir()
    for name, file_store_id in inputs.iteritems():
        job.fileStore.readGlobalFile(file_store_id, os.path.join(work_dir, name))

    # Call: GATK -- PrintReads
    parameters = ['-T', 'PrintReads',
                  '-nct', str(job.cores),
                  '-R', '/data/ref.fasta',
                  '-I', '/data/input.bam',
                  '-BQSR', '/data/recal.table',
                  '--emit_original_quals',
                  '-o', '/data/bqsr.bam']
    if unsafe:
        parameters.extend(['-U', 'ALLOW_SEQ_DICT_INCOMPATIBILITY'])

    docker_call(tool='quay.io/ucsc_cgl/gatk:3.5--dba6dae49156168a909c43330350c6161dc7ecc2',
                inputs=inputs.keys(),
                outputs={'bqsr.bam': None, 'bqsr.bai': None},
                work_dir=work_dir,
                parameters=parameters,
                env=dict(JAVA_OPTS='-Djava.io.tmpdir=/data/ -Xmx{}'.format(job.memory)))

    output_bam = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'bqsr.bam'))
    output_bai = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'bqsr.bai'))
    return output_bam, output_bai
