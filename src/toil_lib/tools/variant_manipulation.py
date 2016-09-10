import os
import subprocess

from toil_lib.programs import docker_call


def gatk_select_variants(job, mode, vcf_id, ref_fasta, ref_fai, ref_dict):
    """
    Isolates a particular variant type from a VCF file using GATK SelectVariants

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str mode: variant type (i.e. SNP or INDEL)
    :param str vcf_id: FileStoreID for input VCF file
    :param str ref_fasta: FileStoreID for reference genome fasta
    :param str ref_fai: FileStoreID for reference genome index file
    :param str ref_dict: FileStoreID for reference genome sequence dictionary file
    :return: FileStoreID for filtered VCF
    :rtype: str
    """
    job.fileStore.logToMaster('GATK SelectVariants: %s' % mode)

    inputs = {'genome.fa': ref_fasta,
              'genome.fa.fai': ref_fai,
              'genome.dict': ref_dict,
              'input.vcf': vcf_id}

    work_dir = job.fileStore.getLocalTempDir()
    for name, file_store_id in inputs.iteritems():
        job.fileStore.readGlobalFile(file_store_id, os.path.join(work_dir, name))

    command = ['-T', 'SelectVariants',
               '-R', 'genome.fa',
               '-V', 'input.vcf',
               '-o', 'output.vcf',
               '-selectType', mode]

    docker_call(work_dir=work_dir,
                env={'JAVA_OPTS': '-Djava.io.tmpdir=/data/ -Xmx{}'.format(job.memory)},
                parameters=command,
                tool='quay.io/ucsc_cgl/gatk:3.5--dba6dae49156168a909c43330350c6161dc7ecc2',
                inputs=inputs.keys(),
                outputs={'output.vcf': None})

    return job.fileStore.writeGlobalFile(os.path.join(work_dir, 'output.vcf'))


def gatk_variant_filtration(job, mode, vcf_id, ref_fasta, ref_fai, ref_dict):
    """
    Filters VCF file using GATK VariantFiltration. The filter expressions are based on GATK variant annotation features.

    SNP Filter Expression
    QD < 2.0 || FS > 60.0 || MQ < 40.0 || MQRankSum < -12.5 || ReadPosRankSum < -8.0

    INDEL Filter Expression
    QD < 2.0 || FS > 200.0 || ReadPosRankSum < -20.0

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str mode: variant type (SNP or INDEL)
    :param str vcf_id: FileStoreID for input VCF file
    :param str ref_fasta: FileStoreID for reference genome fasta
    :param str ref_fai: FileStoreID for reference genome index file
    :param str ref_dict: FileStoreID for reference genome sequence dictionary file
    :return: FileStoreID for filtered VCF file
    :rtype: str
    """
    mode = mode.upper()
    job.fileStore.logToMaster('GATK VariantFiltration: %s' % mode)

    inputs = {'genome.fa': ref_fasta,
              'genome.fa.fai': ref_fai,
              'genome.dict': ref_dict,
              'input.vcf': vcf_id}

    work_dir = job.fileStore.getLocalTempDir()
    for name, file_store_id in inputs.iteritems():
        job.fileStore.readGlobalFile(file_store_id, os.path.join(work_dir, name))

    # GATK hard filters:
    # https://software.broadinstitute.org/gatk/documentation/article?id=2806
    if mode == 'SNP':
        expression = '"QD < 2.0 || FS > 60.0 || MQ < 40.0 || ' \
                     'MQRankSum < -12.5 || ReadPosRankSum < -8.0"'

    elif mode == 'INDEL':
        expression = '"QD < 2.0 || FS > 200.0 || ReadPosRankSum < -20.0"'

    else:
        raise ValueError('Variant filter modes can be SNP or INDEL, got %s' % mode)

    command = ['-T', 'VariantFiltration',
               '-R', 'genome.fa',
               '-V', 'input.vcf',
               '--filterExpression', expression,
               '--filterName', 'GATK_Germline_Hard_Filter_%s' % mode,   # Documents filter in header
               '-o', 'filtered_variants.vcf']

    docker_call(work_dir=work_dir,
                env={'JAVA_OPTS': '-Djava.io.tmpdir=/data/ -Xmx{}'.format(job.memory)},
                parameters=command,
                tool='quay.io/ucsc_cgl/gatk:3.5--dba6dae49156168a909c43330350c6161dc7ecc2',
                inputs=inputs.keys(),
                outputs={'filtered_variants.vcf': None})

    # Fix extra quotation marks in FILTER line
    output = os.path.join(work_dir, 'filtered_variants.vcf')
    sed_cmd = 's/"{}"/{}/'.format(expression, expression)
    subprocess.call(['sed', '-i', sed_cmd, output])

    return job.fileStore.writeGlobalFile(output)


def gatk_variant_recalibrator(job, mode, vcf, ref_fasta, ref_fai, ref_dict, hapmap=None, omni=None, phase=None,
                              dbsnp=None, mills=None, unsafe_mode=False):
    """
    Runs either SNP or INDEL variant quality score recalibration using GATK VariantRecalibrator. Because the VQSR method
    models SNPs and INDELs differently, VQSR must be run separately for these variant types.

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str mode: Determines variant recalibration mode (SNP or INDEL)
    :param str vcf: FileStoreID for input VCF file
    :param str ref_fasta: FileStoreID for reference genome fasta
    :param str ref_fai: FileStoreID for reference genome index file
    :param str ref_dict: FileStoreID for reference genome sequence dictionary file
    :param str hapmap: FileStoreID for HapMap resource file, required for SNP VQSR
    :param str omni: FileStoreID for Omni resource file, required for SNP VQSR
    :param str phase: FileStoreID for 1000G resource file, required for SNP VQSR
    :param str dbsnp: FilesStoreID for dbSNP resource file, required for SNP and INDEL VQSR
    :param str mills: FileStoreID for Mills resource file, required for INDEL VQSR
    :param bool unsafe_mode: If True, runs gatk UNSAFE mode: "-U ALLOW_SEQ_DICT_INCOMPATIBILITY"
    :return: FileStoreID for the variant recalibration table, tranche file, and plots file
    :rtype: tuple
    """
    mode = mode.upper()
    job.fileStore.logToMaster('GATK VariantRecalibrator: %s' % mode)

    inputs = {'genome.fa': ref_fasta,
              'genome.fa.fai': ref_fai,
              'genome.dict': ref_dict,
              'input.vcf': vcf}

    # Refer to GATK documentation for description of recommended parameters:
    # https://software.broadinstitute.org/gatk/documentation/article?id=1259
    # https://software.broadinstitute.org/gatk/documentation/article?id=2805

    # This base command includes parameters for both INDEL and SNP VQSR.
    # DP is a recommended annotation for WGS data, but does not work well with exome data,
    # so it is not used here.
    command = ['-T', 'VariantRecalibrator',
               '-R', 'genome.fa',
               '-input', 'input.vcf',
               '--maxGaussians', '4',
               '-an', 'QD',   # QualByDepth
               '-an', 'FS',   # FisherStrand'
               '-an', 'SOR',  # StrandOddsRatio'
               '-an', 'ReadPosRankSum',
               '-an', 'MQRankSum',
               '-tranche', '100.0',
               '-tranche', '99.9',
               '-tranche', '99.0',
               '-tranche', '90.0',
               '-recalFile', 'output.recal',
               '-tranchesFile', 'output.tranches',
               '-rscriptFile', 'output.plots.R']

    # Parameters and resource files for SNP VQSR.
    if mode == 'SNP':
        command.extend(
            ['-resource:hapmap,known=false,training=true,truth=true,prior=15.0', 'hapmap.vcf',
             '-resource:omni,known=false,training=true,truth=true,prior=12.0', 'omni.vcf',
             '-resource:dbsnp,known=true,training=false,truth=false,prior=2.0', 'dbsnp.vcf',
             '-resource:1000G,known=false,training=true,truth=false,prior=10.0', '1000G.vcf',
             '-an', 'MQ',  # RMSMappingQuality'
             '-mode', 'SNP'])

        inputs['hapmap.vcf'] = hapmap
        inputs['omni.vcf'] = omni
        inputs['dbsnp.vcf'] = dbsnp
        inputs['1000G.vcf'] = phase

    # Parameters and resource files for INDEL VQSR
    elif mode == 'INDEL':
        command.extend(
            ['-resource:mills,known=false,training=true,truth=true,prior=12.0', 'mills.vcf',
             '-resource:dbsnp,known=true,training=false,truth=false,prior=2.0', 'dbsnp.vcf',
             '-mode', 'INDEL'])

        inputs['mills.vcf'] = mills
        inputs['dbsnp.vcf'] = dbsnp

    else:
        raise ValueError('Variant filter modes can be SNP or INDEL, got %s' % mode)

    if unsafe_mode:
        command.extend(['-U', 'ALLOW_SEQ_DICT_INCOMPATIBILITY'])

    # Delay reading in files until function is configured
    work_dir = job.fileStore.getLocalTempDir()
    for name, file_store_id in inputs.iteritems():
        job.fileStore.readGlobalFile(file_store_id, os.path.join(work_dir, name))

    docker_call(work_dir=work_dir,
                env={'JAVA_OPTS': '-Djava.io.tmpdir=/data/ -Xmx{}'.format(job.memory)},
                parameters=command,
                tool='quay.io/ucsc_cgl/gatk:3.5--dba6dae49156168a909c43330350c6161dc7ecc2',
                inputs=inputs.keys(),
                outputs={'output.recal': None, 'output.tranches': None, 'output.plots.R': None})

    recal_id = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'output.recal'))
    tranches_id = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'output.tranches'))
    plots_id = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'output.plots.R'))
    return recal_id, tranches_id, plots_id


def gatk_apply_variant_recalibration(job, mode, vcf_id, recal_table, tranches, ref_fasta, ref_fai, ref_dict,
                                     unsafe_mode=False):
    """
    Applies variant quality score recalibration to VCF file using GATK ApplyRecalibration

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str mode: Determines variant recalibration mode (SNP or INDEL)
    :param str vcf_id: FileStoreID for input VCF file
    :param str recal_table: FileStoreID for recalibration table file
    :param str tranches: FileStoreID for tranches file
    :param str ref_fasta: FileStoreID for reference genome fasta
    :param str ref_fai: FileStoreID for reference genome index file
    :param str ref_dict: FileStoreID for reference genome sequence dictionary file
    :param bool unsafe_mode: If True, runs gatk UNSAFE mode: "-U ALLOW_SEQ_DICT_INCOMPATIBILITY"
    :return: FileStoreID for recalibrated VCF file
    :rtype: str
    """
    mode = mode.upper()
    job.fileStore.logToMaster('GATK ApplyRecalibration ({} Mode)'.format(mode))

    inputs = {'genome.fa': ref_fasta,
              'genome.fa.fai': ref_fai,
              'genome.dict': ref_dict,
              'input.vcf': vcf_id,
              'recal': recal_table,
              'tranches': tranches}

    work_dir = job.fileStore.getLocalTempDir()
    for name, file_store_id in inputs.iteritems():
        job.fileStore.readGlobalFile(file_store_id, os.path.join(work_dir, name))

    # GATK recommended parameters:
    # https://software.broadinstitute.org/gatk/documentation/article?id=2805
    command = ['-T', 'ApplyRecalibration',
               '-mode', mode,
               '-R', 'genome.fa',
               '-input', 'input.vcf',
               '-o', 'vqsr.vcf',
               '-ts_filter_level', '99.0',  # Filter using second lowest tranche sensitivity
               '-recalFile', 'recal',
               '-tranchesFile', 'tranches']

    if unsafe_mode:
        command.extend(['-U', 'ALLOW_SEQ_DICT_INCOMPATIBILITY'])

    docker_call(work_dir=work_dir,
                env={'JAVA_OPTS': '-Djava.io.tmpdir=/data/ -Xmx{}'.format(job.memory)},
                parameters=command,
                tool='quay.io/ucsc_cgl/gatk:3.5--dba6dae49156168a909c43330350c6161dc7ecc2',
                inputs=inputs.keys(),
                outputs={'vqsr.vcf': None})

    return job.fileStore.writeGlobalFile(os.path.join(work_dir, 'vqsr.vcf'))


def gatk_combine_variants(job, vcfs, ref_fasta, ref_fai, ref_dict, merge_option='UNIQUIFY'):
    """
    Merges VCF files using GATK CombineVariants

    :param JobFunctionWrappingJob job: Toil Job instance
    :param dict vcfs: Dictionary of VCF FileStoreIDs {sample identifier: FileStoreID}
    :param str ref_fasta: FileStoreID for reference genome fasta
    :param str ref_fai: FileStoreID for reference genome index file
    :param str ref_dict: FileStoreID for reference genome sequence dictionary file
    :param str merge_option: Value for --genotypemergeoption flag (Default: 'UNIQUIFY')
                            'UNIQUIFY': Multiple variants at a single site are merged into a
                                        single variant record.
                            'UNSORTED': Used to merge VCFs from the same sample
    :return: FileStoreID for merged VCF file
    :rtype: str
    """
    job.fileStore.logToMaster('GATK CombineVariants')

    inputs = {'genome.fa': ref_fasta,
              'genome.fa.fai': ref_fai,
              'genome.dict': ref_dict}
    inputs.update(vcfs)

    work_dir = job.fileStore.getLocalTempDir()
    for name, file_store_id in inputs.iteritems():
        job.fileStore.readGlobalFile(file_store_id, os.path.join(work_dir, name))

    command = ['-T', 'CombineVariants',
               '-R', '/data/genome.fa',
               '-o', '/data/merged.vcf',
               '--genotypemergeoption', merge_option]

    for uuid, vcf_id in vcfs.iteritems():
        command.extend(['--variant', os.path.join('/data', uuid)])

    docker_call(work_dir=work_dir,
                env={'JAVA_OPTS': '-Djava.io.tmpdir=/data/ -Xmx{}'.format(job.memory)},
                parameters=command,
                tool='quay.io/ucsc_cgl/gatk:3.5--dba6dae49156168a909c43330350c6161dc7ecc2',
                inputs=inputs.keys(),
                outputs={'merged.vcf': None})

    return job.fileStore.writeGlobalFile(os.path.join(work_dir, 'merged.vcf'))


def gatk_combine_gvcfs(job, gvcfs, ref_fasta, ref_fai, ref_dict):
    """
    Merges GVCF files using GATK CombineGVCFs

    :param JobFunctionWrappingJob job: Toil Job instance
    :param dict gvcfs: Dictionary of GVCF FileStoreID {sample identifier: FileStoreID}
    :param str ref_fasta: FileStoreID for reference genome fasta
    :param str ref_fai: FileStoreID for reference genome index file
    :param str ref_dict: FileStoreID for reference genome sequence dictionary file
    :return: FileStoreID for merged GVCF file
    :rtype: str
    """
    job.fileStore.logToMaster('Running GATK CombineGVCFs')

    inputs = {'genome.fa': ref_fasta,
              'genome.fa.fai': ref_fai,
              'genome.dict': ref_dict}
    inputs.update(gvcfs)

    work_dir = job.fileStore.getLocalTempDir()
    for name, file_store_id in inputs.iteritems():
        job.fileStore.readGlobalFile(file_store_id, os.path.join(work_dir, name))

    command = ['-T', 'CombineGVCFs',
               '-R', '/data/genome.fa',
               '-o', '/data/merged.g.vcf']

    for uuid, vcf_id in gvcfs.iteritems():
        command.extend(['--variant', os.path.join('/data', uuid)])

    docker_call(work_dir=work_dir,
                env={'JAVA_OPTS': '-Djava.io.tmpdir=/data/ -Xmx{}'.format(job.memory)},
                parameters=command,
                tool='quay.io/ucsc_cgl/gatk:3.5--dba6dae49156168a909c43330350c6161dc7ecc2',
                inputs=inputs.keys(),
                outputs={'merged.g.vcf': None})

    return job.fileStore.writeGlobalFile(os.path.join(work_dir, 'merged.g.vcf'))
