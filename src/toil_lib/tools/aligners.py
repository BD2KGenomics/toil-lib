import os
import subprocess
import time

from toil.lib.docker import dockerCall

from toil_lib import require
from toil_lib.tools import log_runtime
from toil_lib.urls import download_url


def run_star(job, r1_id, r2_id, star_index_url, wiggle=False, sort=True):
    """
    Performs alignment of fastqs to bam via STAR

    --limitBAMsortRAM step added to deal with memory explosion when sorting certain samples.
    The value was chosen to complement the recommended amount of memory to have when running STAR (60G)

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str r1_id: FileStoreID of fastq (pair 1)
    :param str r2_id: FileStoreID of fastq (pair 2 if applicable, else pass None)
    :param str star_index_url: STAR index tarball
    :param bool wiggle: If True, will output a wiggle file and return it
    :return: FileStoreID from RSEM
    :rtype: str
    """
    work_dir = job.fileStore.getLocalTempDir()
    download_url(job, url=star_index_url, name='starIndex.tar.gz', work_dir=work_dir)
    subprocess.check_call(['tar', '-xvf', os.path.join(work_dir, 'starIndex.tar.gz'), '-C', work_dir])
    os.remove(os.path.join(work_dir, 'starIndex.tar.gz'))
    # Determine tarball structure - star index contains are either in a subdir or in the tarball itself
    star_index = os.path.join('/data', os.listdir(work_dir)[0]) if len(os.listdir(work_dir)) == 1 else '/data'
    # Parameter handling for paired / single-end data
    parameters = ['--runThreadN', str(job.cores),
                  '--genomeDir', star_index,
                  '--outFileNamePrefix', 'rna',
                  '--outSAMunmapped', 'Within',
                  '--quantMode', 'TranscriptomeSAM',
                  '--outSAMattributes', 'NH', 'HI', 'AS', 'NM', 'MD',
                  '--outFilterType', 'BySJout',
                  '--outFilterMultimapNmax', '20',
                  '--outFilterMismatchNmax', '999',
                  '--outFilterMismatchNoverReadLmax', '0.04',
                  '--alignIntronMin', '20',
                  '--alignIntronMax', '1000000',
                  '--alignMatesGapMax', '1000000',
                  '--alignSJoverhangMin', '8',
                  '--alignSJDBoverhangMin', '1',
                  '--sjdbScore', '1',
                  '--limitBAMsortRAM', '49268954168']
    # Modify paramaters based on function arguments
    if sort:
        parameters.extend(['--outSAMtype', 'BAM', 'SortedByCoordinate'])
        aligned_bam = 'rnaAligned.sortedByCoord.out.bam'
    else:
        parameters.extend(['--outSAMtype', 'BAM', 'Unsorted'])
        aligned_bam = 'rnaAligned.out.bam'
    if wiggle:
        parameters.extend(['--outWigType', 'bedGraph',
                           '--outWigStrand', 'Unstranded',
                           '--outWigReferencesPrefix', 'chr'])
    if r1_id and r2_id:
        job.fileStore.readGlobalFile(r1_id, os.path.join(work_dir, 'R1.fastq'))
        job.fileStore.readGlobalFile(r2_id, os.path.join(work_dir, 'R2.fastq'))
        parameters.extend(['--readFilesIn', '/data/R1.fastq', '/data/R2.fastq'])
    else:
        job.fileStore.readGlobalFile(r1_id, os.path.join(work_dir, 'R1.fastq'))
        parameters.extend(['--readFilesIn', '/data/R1.fastq'])
    # Call: STAR Mapping
    dockerCall(job=job, tool='quay.io/ucsc_cgl/star:2.4.2a--bcbd5122b69ff6ac4ef61958e47bde94001cfe80',
               workDir=work_dir, parameters=parameters)
    # Check output bam isnt size zero if sorted
    aligned_bam_path = os.path.join(work_dir, aligned_bam)
    if sort:
        assert(os.stat(aligned_bam_path).st_size > 0, 'Aligned bam failed to sort. Ensure sufficient memory is free.')
    # Write to fileStore
    aligned_id = job.fileStore.writeGlobalFile(aligned_bam_path)
    transcriptome_id = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'rnaAligned.toTranscriptome.out.bam'))
    log_id = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'rnaLog.final.out'))
    sj_id = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'rnaSJ.out.tab'))
    if wiggle:
        wiggle_id = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'rnaSignal.UniqueMultiple.str1.out.bg'))
        return transcriptome_id, aligned_id, wiggle_id, log_id, sj_id
    else:
        return transcriptome_id, aligned_id, log_id, sj_id


def run_bwakit(job, config,
               sort=True,
               trim=False,
               mark_secondary=False,
               benchmarking=False):
    """
    Runs BWA-Kit to align single or paired-end fastq files or realign SAM/BAM files.

    :param JobFunctionWrappingJob job: Passed by Toil automatically
    :param Namespace config: A configuration object that holds strings as attributes.
        The attributes must be accessible via the dot operator.
        The config must have:
        config.r1               FileStoreID for FASTQ file, or None if realigning SAM/BAM
        config.r2               FileStoreID for paired FASTQ file, or None if single-ended
        config.bam              FileStoreID for BAM file to be realigned, or None if aligning fastq
        config.sam              FileStoreID for SAM file to be realigned, or None if aligning fastq
        config.ref              FileStoreID for the reference genome
        config.fai              FileStoreID for the reference index file
        config.amb              FileStoreID for the reference amb file
        config.ann              FileStoreID for the reference ann file
        config.bwt              FileStoreID for the reference bwt file
        config.pac              FileStoreID for the reference pac file
        config.sa               FileStoreID for the reference sa file
        config.alt              FileStoreID for the reference alt (or None)
        config.rg_line          The read group value to use (or None -- see below)
        config.library          Read group attribute: library
        config.platform         Read group attribute: platform
        config.program_unit     Read group attribute: program unit
        config.uuid             Read group attribute: sample ID

        If specifying config.rg_line, use the following format:
            BAM read group header line (@RG), as defined on page 3 of the SAM spec.
            Tabs should be escaped, e.g., @RG\\tID:foo\\tLB:bar...
            for the read group "foo" from sequencing library "bar".
            Multiple @RG lines can be defined, but should be split by an escaped newline \\n,
            e.g., @RG\\tID:foo\\t:LB:bar\\n@RG\\tID:santa\\tLB:cruz.

    :param bool sort: If True, sorts the BAM
    :param bool trim: If True, performs adapter trimming
    :param bool mark_secondary: If True, mark shorter split reads as secondary
    :param boolean benchmarking: If true, returns the runtime along with the
      FileStoreID.
    :return: FileStoreID of BAM
    :rtype: str
    """
    work_dir = job.fileStore.getLocalTempDir()
    rg = None
    inputs = {'ref.fa': config.ref,
              'ref.fa.fai': config.fai,
              'ref.fa.amb': config.amb,
              'ref.fa.ann': config.ann,
              'ref.fa.bwt': config.bwt,
              'ref.fa.pac': config.pac,
              'ref.fa.sa': config.sa}
    samples = []
    realignment = False
    # If a fastq pair was provided
    if getattr(config, 'r1', None):
        inputs['input.1.fq.gz'] = config.r1
        samples.append('input.1.fq.gz')
    if getattr(config, 'r2', None):
        inputs['input.2.fq.gz'] = config.r2
        samples.append('input.2.fq.gz')
    if getattr(config, 'bam', None):
        inputs['input.bam'] = config.bam
        samples.append('input.bam')
        realignment = True
    if getattr(config, 'sam', None):
        inputs['input.sam'] = config.sam
        samples.append('input.sam')
        realignment = True
    # If an alt file was provided
    if getattr(config, 'alt', None):
        inputs['ref.fa.alt'] = config.alt
    for name, fileStoreID in inputs.iteritems():
        job.fileStore.readGlobalFile(fileStoreID, os.path.join(work_dir, name))
    # If a read group line was provided
    if getattr(config, 'rg_line', None):
        rg = config.rg_line
    # Otherwise, generate a read group line to place in the BAM.
    elif all(getattr(config, elem, None) for elem in ['library', 'platform', 'program_unit', 'uuid']):
        rg = "@RG\\tID:{0}".format(config.uuid)  # '\' character is escaped so bwakit gets passed '\t' properly
        rg_attributes = [config.library, config.platform, config.program_unit, config.uuid]
        for tag, info in zip(['LB', 'PL', 'PU', 'SM'], rg_attributes):
            rg += '\\t{0}:{1}'.format(tag, info)
    # If realigning, then bwakit can use pre-existing read group data
    elif realignment:
        rg = None

    # BWA Options
    opt_args = []
    if sort:
        opt_args.append('-s')
    if trim:
        opt_args.append('-a')
    if mark_secondary:
        opt_args.append('-M')
    # Call: bwakit
    parameters = ['-t', str(job.cores)] + opt_args + ['-o', '/data/aligned', '/data/ref.fa']
    if rg is not None:
        parameters = ['-R', rg] + parameters
    for sample in samples:
        parameters.append('/data/{}'.format(sample))

    start_time = time.time()
    dockerCall(job=job, tool='quay.io/ucsc_cgl/bwakit:0.7.12--c85ccff267d5021b75bb1c9ccf5f4b79f91835cc',
               parameters=parameters, workDir=work_dir)
    end_time = time.time()
    log_runtime(job, start_time, end_time, 'bwakit')

    # Either write file to local output directory or upload to S3 cloud storage
    job.fileStore.logToMaster('Aligned sample: {}'.format(config.uuid))
    bam_id = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'aligned.aln.bam'))
    if benchmarking:
        return (bam_id, (end_time - start_time))
    else:
        return bam_id


def run_bowtie2(job,
                read1,
                name1, name2, name3, name4, rev1, rev2, ref,
                read2=None,
                benchmarking=False):
    '''
    Runs bowtie2 for alignment.

    :param JobFunctionWrappingJob job: passed automatically by Toil.
    :param str read1: The FileStoreID of the first-of-pair file.
    :param str name1: The FileStoreID of the NAME.1.bt2 index file.
    :param str name2: The FileStoreID of the NAME.2.bt2 index file.
    :param str name3: The FileStoreID of the NAME.3.bt2 index file.
    :param str name4: The FileStoreID of the NAME.4.bt2 index file.
    :param str rev1: The FileStoreID of the NAME.rev.1.bt2 index file.
    :param str rev2: The FileStoreID of the NAME.rev.2.bt2 index file.
    :param str ref: The reference FASTA FileStoreID.
    :param str read2: The (optional) FileStoreID of the first-of-pair file.
    :param boolean benchmarking: If true, returns the runtime along with the
      FileStoreID.
    '''
    work_dir = job.fileStore.getLocalTempDir()
    file_ids = [ref, read1,
                name1, name2, name3, name4,
                rev1, rev2]
    file_names = ['ref.fa', 'read1.fq',
                  'ref.1.bt2', 'ref.2.bt2', 'ref.3.bt2', 'ref.4.bt2',
                  'ref.rev.1.bt2', 'ref.rev.2.bt2']

    parameters = ['-x', '/data/ref',
                  '-1', '/data/read1.fq',
                  '-S', '/data/sample.sam',
                  '-t', str(job.cores)]

    if read2:
        file_ids.append(read2)
        file_names.append('read2.fq')
        parameters.extend(['-2', '/data/read2.fq'])
    for file_store_id, name in zip(file_ids, file_names):
        job.fileStore.readGlobalFile(file_store_id, os.path.join(work_dir, name))

    start_time = time.time()
    dockerCall(job=job,
               workDir=work_dir,
               parameters=parameters,
               tool='quay.io/ucsc_cgl/bowtie2')
    end_time = time.time()
    log_runtime(job, start_time, end_time, 'bowtie2')

    sam_id = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'sample.sam'))
    if benchmarking:
        return (sam_id, (end_time - start_time))
    else:
        return sam_id


def run_snap(job,
             read1,
             genome, genome_index, genome_hash, overflow,
             read2=None,
             sort=False,
             mark_duplicates=False,
             benchmarking=False):
    '''
    Runs SNAP for alignment.

    If both first- and second-of-pair reads are passed, runs in paired mode.
    Else runs in single mode.

    :param JobFunctionWrappingJob job: passed automatically by Toil.
    :param str read1: The FileStoreID of the first-of-pair file.
    :param str genome: The FileStoreID of the SNAP Genome index.
    :param str genome_index: The FileStoreID of the SNAP GenomeIndex index.
    :param str genome_hash: The FileStoreID of the SNAP GenomeIndexHash index.
    :param str overflow: The FileStoreID of the SNAP OverflowTable index.
    :param str ref: The reference FASTA FileStoreID.
    :param str read2: The (optional) FileStoreID of the first-of-pair file.
    :param boolean sort: If true, sorts the reads. Defaults to false. If enabled,
      this function will also return the BAM Index.
    :param boolean mark_duplicates: If true, marks reads as duplicates. Defaults
      to false. This option is only valid if sort is True.
    :param boolean benchmarking: If true, returns the runtime along with the
      FileStoreID.
    '''
    work_dir = job.fileStore.getLocalTempDir()
    os.mkdir(os.path.join(work_dir, 'snap'))
    file_ids = [read1,
                genome, genome_index, genome_hash, overflow]
    file_names = ['read1.fq',
                  'snap/Genome',
                  'snap/GenomeIndex',
                  'snap/GenomeIndexHash',
                  'snap/OverflowTable']

    if read2:
        file_ids.append(read2)
        file_names.append('read2.fq')

        parameters = ['paired'
                      '/data/snap',
                      '/data/read1.fq',
                      '/data/read2.fq',
                      '-o', '-bam', '/data/sample.bam',
                      '-t', str(job.cores)]
    else:

        parameters = ['single'
                      '/data/snap',
                      '/data/read1.fq',
                      '-o', '-bam', '/data/sample.bam',
                      '-t', str(job.cores)]

    if sort:
        
        parameters.append('-so')

        if not mark_duplicates:
            parameters.extend(['-S', 'd'])

    else:

        require(not mark_duplicates,
                'Cannot run duplicate marking if sort is not enabled.')
        
    for file_store_id, name in zip(file_ids, file_names):
        job.fileStore.readGlobalFile(file_store_id, os.path.join(work_dir, name))

    start_time = time.time()
    dockerCall(job=job,
               workDir=work_dir,
               parameters=parameters,
               tool='quay.io/ucsc_cgl/snap')
    end_time = time.time()
    log_runtime(job, start_time, end_time, 'snap (sort={}, dm={})'.format(sort,
                                                                          mark_duplicates))

    bam_id = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'sample.bam'))
    if not sort:
        if benchmarking:
            return (bam_id, (end_time - start_time))
        else:
            return bam_id
    else:
        bai_id = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'sample.bam.bai'))
        if benchmarking:
            return (bam_id, bai_id, (end_time - start_time))
        else:
            return (bam_id, bai_id)
                
