#!/usr/bin/python

import os, argparse, subprocess, logging, boto3

parser = argparse.ArgumentParser(description='Multi runner backup to S3.')
parser.add_argument('--dir', '-D', required=True, help='root backup (ex: /home/)')
parser.add_argument('--bucket', '-B', required=True, help='s3 bucket (ex: backup-efs)')
parser.add_argument('--rclone', '-R', required=True, help='rclone configuration name (ex: s3-backup)')
parser.add_argument('--jobs', '-J', type=int, help='number of backup runner (default: 2)', default=2)
parser.add_argument('--log', '-L', required=True, help='Path of log file (default: /var/log/backup-s3.log)', default="/var/log/backup-s3.log")
parser.add_argument('--queue', '-Q', required=True, help='Url of the SQS queue')

args = parser.parse_args()

logging.basicConfig(filename=str(args.log),format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

def add_msg(args, job):
    region = os.environ["AWS_REGION"]
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

def runner_files(args, directories):
    excludes = ' --exclude '.join(directories)
    try:
        rcode = subprocess.call(["/usr/local/bin/rclone", "sync", "--exclude", excludes, args.dir, args.rclone+":"+args.bucket, "--quiet"])
        if rcode != 0:
            logging.error('Backup of files in %s : Failed', args.dir)
        else:
            logging.info('Backup of files in %s : OK', args.dir)
    except:
        logging.error('rclone not found')

directories = list()
for fname in os.listdir(args.dir):
    path = os.path.join(args.dir, fname)
    if os.path.isdir(path):
        if len(os.listdir(path)) > 0:
            directories.append(path)

if not directories:
    print "Directory "+args.dir+" is empty"
    exit(1)

runner_files(args, directories)
add_msg(args, directories)
