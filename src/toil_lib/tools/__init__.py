import os
import subprocess


def log_runtime(job, start, end, cmd):

    elapsed_time = end - start
    
    hours = int(elapsed_time) / (60 * 60)
    minutes = int(elapsed_time - (60 * 60 * hours)) / 60
    seconds = int(elapsed_time - (60 * 60 * hours) - (60 * minutes)) % 60

    job.fileStore.logToMaster("%s ran in %dh%dm%ds" % (cmd, hours, minutes, seconds))


def get_mean_insert_size(work_dir, bam_name):
    """Function taken from MC3 Pipeline"""
    cmd = "docker run --log-driver=none --rm -v {}:/data quay.io/ucsc_cgl/samtools " \
          "view -f66 {}".format(work_dir, os.path.join(work_dir, bam_name))
    process = subprocess.Popen(args=cmd, shell=True, stdout=subprocess.PIPE)
    b_sum = 0.0
    b_count = 0.0
    while True:
        line = process.stdout.readline()
        if not line:
            break
        tmp = line.split("\t")
        if abs(long(tmp[8])) < 10000:
            b_sum += abs(long(tmp[8]))
            b_count += 1
    process.wait()
    try:
        mean = b_sum / b_count
    except ZeroDivisionError:
        mean = 150
    print "Using insert size: %d" % mean
    return int(mean)
