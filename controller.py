#!/usr/bin/python

import os, sys, argparse, subprocess, logging, boto3, hashlib

parser = argparse.ArgumentParser(description='Multi runner backup to S3.')
parser.add_argument('--dir', '-D', required=True, help='root backup (ex: /home/)')
parser.add_argument('--bucket', '-B', required=True, help='s3 bucket (ex: backup-efs)')
parser.add_argument('--rclone', '-R', required=True, help='rclone configuration name (ex: s3-backup)')
parser.add_argument('--jobs', '-J', type=int, help='number of backup runner (default: 2)', default=2)
parser.add_argument('--log', '-L', required=True, help='Path of log file (default: /var/log/backup-s3.log)', default="/var/log/backup-s3.log")
parser.add_argument('--queue', '-Q', required=True, help='Url of the SQS queue')
parser.add_argument('--region', required=True, help='AWS region')
parser.add_argument('--subdirs', help='List of optional subdirs',metavar='N',nargs='+')

args = parser.parse_args()

logging.basicConfig(filename=str(args.log),format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

def add_msg(args, job):
    region = args.region
    sqs = boto3.client('sqs',region_name=region)
    queue_url = args.queue
    for directory in job:
        try:
            msg = sqs.send_message(
                QueueUrl=queue_url,
                DelaySeconds=10,
                MessageAttributes={
                    'Title': {
                        'DataType': 'String',
                        'StringValue': directory
                    }
                },
                MessageBody=(directory)
            )
            logging.info('Adding message '+msg['MessageId']+' in queue is done')
        except:
            logging.error('Adding message '+directory+' in queue is failed')

def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def runner_files(args):
    s3 = boto3.client('s3')
    updatelist = list()
    deletelist = list()
    noupdate   = list()
    paginator  = s3.get_paginator('list_objects')
    for result in paginator.paginate(Bucket=args.bucket, Delimiter='/', Prefix=''):
        if result.get('Contents') is not None:
            for i in result.get('Contents'):
                dfile = i['Key']
                filename = os.path.join(args.dir, dfile)
                if os.path.isfile(filename):
                    dETag = str(i['ETag']).replace('"', '')
                    if str(md5(filename)) != dETag:
                        updatelist.append(filename)
                    else:
                        noupdate.append(filename)
                else:
                    deletelist.append(dfile)
    for fname in os.listdir(args.dir):
        path = os.path.join(args.dir, fname)
        if not os.path.isdir(path):
            if path not in noupdate:
                updatelist.append(path)
    for file in deletelist:
        s3.delete_object(Bucket=args.bucket, Key=file)
    for file in updatelist:
        try:
            rcode = subprocess.call(["/usr/local/bin/rclone", "copy", file, args.rclone+":"+args.bucket, "--quiet"])
            if rcode != 0:
                logging.error('Backup of file %s : Failed', file)
            else:
                logging.info('Backup of file %s : OK', file)
        except:
            logging.error('rclone not found')


if args.subdirs is not None:
    subdirs = list()
    for x in args.subdirs:
        if x.endswith('/'):
            x = x[:-1]
        subdirs.append(x)
else:
    subdirs = None

directories = list()
for fname in os.listdir(args.dir):
    path = os.path.join(args.dir, fname)
    if subdirs is not None:
        if path in subdirs:
            print path
    if os.path.isdir(path):
        if len(os.listdir(path)) > 0:
            directories.append(path)

if not directories:
    print "Directory "+args.dir+" is empty"
    exit(1)

add_msg(args, directories)
runner_files(args)
