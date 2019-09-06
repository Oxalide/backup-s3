#!/usr/bin/python

import os, sys, argparse, threading, logging, time, boto3, ec2metadata
from subprocess import check_output, CalledProcessError
from os.path import normpath, basename
from dynalock import LockerClient

parser = argparse.ArgumentParser(description='Multi runner backup to S3.')
parser.add_argument('--log', '-L', required=True, help='Path of log file (default: /var/log/backup-s3.log)', default="/var/log/backup-s3.log")
parser.add_argument('--bucket', '-B', required=True, help='s3 bucket (ex: backup-efs)')
parser.add_argument('--rclone', '-R', required=True, help='rclone configuration name (ex: s3-backup)')
parser.add_argument('--queue', '-Q', required=True, help='Url of the SQS queue')
parser.add_argument('--locktable', required=True, help='name of dynamodb lock table')
parser.add_argument('--jobtable', required=True, help='name of dynamodb job table')
parser.add_argument('--region', required=True, help='AWS region')
parser.add_argument('--ignore-checksum', required=False, action='store_const', const='--ignore-checksum', default="", help='Ignore MD5 checksum')

args = parser.parse_args()

logging.basicConfig(filename=str(args.log),format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

def get_msg(args):
    region = args.region
    sqs = boto3.client('sqs',region_name=region)
    queue_url = args.queue
    lock = LockerClient(args.locktable, region=region)
    instanceid = ec2metadata.get('instance-id')
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
            message_locked = lock.get_lock(body, 5000)
            logging.info('lock: '+str(message_locked))
            if message_locked:
                receipt_handle = message['ReceiptHandle']
                sqs.delete_message(QueueUrl=queue_url,ReceiptHandle=receipt_handle)
                logging.info('Received and deleted message: %s' % message)
                lock.release_lock(body)
                dynamodb = boto3.resource('dynamodb',region_name=region)
                table = dynamodb.Table(args.jobtable)
                table.put_item(Item={'Instance': instanceid,'Job': body})
                runner(args, body)
                table.delete_item(Key={'Instance': instanceid,'Job': body})
                if ec2metadata.get('termination-time') is None:
                    time.sleep(5)
                else:
                    logging.info("Instance '%s' it's marked for termination, stopping runner process")
                    sys.exit(1)
            else:
                logging.info('Message '+body+' already locked')
                time.sleep(5)
        except Exception as e:
            logging.error(str(e.message)+' '+str(e.args))
            try:
                logging.info('no message to read')
                time.sleep(30)
            except KeyboardInterrupt:
                logging.info('Interrupted')
                try:
                    sys.exit(0)
                except SystemExit:
                    os._exit(0)

def runner(args, directory):
    destdir = basename(normpath(directory))
    try:
        logging.info('Backup of %s : Started', directory)
        rcode = check_output("/usr/local/bin/rclone sync "+directory+" "+args.rclone+":"+args.bucket+"/"+destdir+"/ "+args.ignore_checksum+" --quiet --config /etc/rclone.conf",shell=True)
        logging.info('Backup of %s : OK', directory)
    except CalledProcessError as e:
        if e.returncode==127:
            logging.error("rclone not found")
        elif e.returncode<=125:
            logging.error("'rclone' Failed, returned code %d", e.returncode)
        else:
            logging.error("rclone likely crashed, shell retruned code %d", e.returncode)
    except OSError as e:
        logging.error("failed to run shell: '%s'", (str(e)))

get_msg(args)
