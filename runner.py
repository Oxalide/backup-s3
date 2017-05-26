#!/usr/bin/python

import os, sys, argparse, threading, subprocess, logging, time, boto3
from os.path import normpath, basename
from dynalock import LockerClient

parser = argparse.ArgumentParser(description='Multi runner backup to S3.')
parser.add_argument('--log', '-L', required=True, help='Path of log file (default: /var/log/backup-s3.log)', default="/var/log/backup-s3.log")
parser.add_argument('--bucket', '-B', required=True, help='s3 bucket (ex: backup-efs)')
parser.add_argument('--rclone', '-R', required=True, help='rclone configuration name (ex: s3-backup)')
parser.add_argument('--queue', '-Q', required=True, help='Url of the SQS queue')

args = parser.parse_args()

logging.basicConfig(filename=str(args.log),format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

def get_msg(args):
    region = os.environ["AWS_REGION"]
    sqs = boto3.client('sqs',region_name=region)
    queue_url = args.queue
    lock = LockerClient('backup-efs-to-s3-preprod')
    while(True):
        try:
            msg = sqs.receive_message(
                QueueUrl=queue_url,
                MessageAttributeNames=['All'],
                MaxNumberOfMessages=1,
                VisibilityTimeout=0,
                WaitTimeSeconds=0
            )
            message = msg['Messages'][0]
            body = str(message['Body'])
            if lock.get_lock(body, 5000):
                receipt_handle = message['ReceiptHandle']
                sqs.delete_message(QueueUrl=queue_url,ReceiptHandle=receipt_handle)
                logging.info('Received and deleted message: %s' % message)
                lock.release_lock(body)
                runner(args, body)
            else:
                loggin.info('Message '+body+' already locked')
        except:
            try:
                logging.info('no message to read')
                time.sleep(60)
            except KeyboardInterrupt:
                logging.info('Interrupted')
                try:
                    sys.exit(0)
                except SystemExit:
                    os._exit(0)

def runner(args, directory):
    destdir = basename(normpath(directory))
    try:
        rcode = subprocess.call(["/usr/local/bin/rclone", "sync", directory, args.rclone+":"+args.bucket+"/"+destdir+"/", "--quiet"])
        logging.info('Backup of %s : Started', directory)
        if rcode != 0:
            logging.error('Backup of %s : Failed', directory)
        else:
            logging.info('Backup of %s : OK', directory)
    except:
        logging.error('rclone not found')
        sys.exit(2)

get_msg(args)
