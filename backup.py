#!/usr/bin/python

import os, sys, argparse, threading, subprocess, logging
from os.path import normpath, basename

logging.basicConfig(filename='/var/log/backup-s3.log',format='%(asctime)s %(message)s', level=logging.INFO)

def runner(args, index, job):
    for directory in job:
        destdir = basename(normpath(directory))
        try:
            rcode = subprocess.call(["/usr/local/bin/rclone", "sync", directory, args.rclone+":"+args.bucket+"/"+destdir+"/", "--quiet"])
            if rcode != 0:
                logging.error('Backup of %s : Failed', directory)
            else:
                logging.info('Backup of %s : OK', directory)
        except:
            logging.error('rclone not found')
            sys.exit(2)

parser = argparse.ArgumentParser(description='Multi runner backup to S3.')
parser.add_argument('--dir', '-D', required=True, help='root backup (ex: /home/)')
parser.add_argument('--bucket', '-B', required=True, help='s3 bucket (ex: backup-efs)')
parser.add_argument('--rclone', '-R', required=True, help='rclone configuration name (ex: s3-backup)')
parser.add_argument('--jobs', '-J', type=int, help='number of backup runner (default: 2)', default=2)
parser.add_argument('--log', '-L', required=True, help='Path of log file (default: /var/log/backup-s3.log)', default="/var/log/backup-s3.log")

args = parser.parse_args()

max_jobs = int(args.jobs)

directories = []
for fname in os.listdir(args.dir):
    path = os.path.join(args.dir, fname)
    if os.path.isdir(path):
        if len(os.listdir(path)) > 0:
            directories.append(path)

if not directories:
    print "Directory "+args.dir+" is empty"
    exit(1)

count_dirs = len(directories)
if count_dirs < max_jobs:
    max_jobs = count_dirs

max_dirs_per_job = int(round(float(count_dirs)/round(max_jobs)))
jobs = []
for index, directory in enumerate(directories, start=1):
    try:
        jobs[-1]
        if len(jobs[-1])<max_dirs_per_job:
            jobs[-1].append(directory)
        elif len(jobs)==max_jobs:
            jobs[-1].append(directory)
        else:
            jobs.append([])
            jobs[-1].append(directory)
    except IndexError:
        jobs.append([])
        jobs[-1].append(directory)

threads = []

for index, run in enumerate(jobs, start=1):
    t = threading.Thread(target=runner, args=(args, index, run))
    threads.append(t)
    t.start()
