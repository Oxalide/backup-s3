#!/usr/bin/python
import argparse, logging, ec2metadata, boto3, time


parser = argparse.ArgumentParser(description='Multi runner backup to S3.')
parser.add_argument('--log', '-L', required=True, help='Path of log file (default: /var/log/backup-s3.log)', default="/var/log/backup-s3.log")
parser.add_argument('--queue', '-Q', required=True, help='Url of the SQS queue')
parser.add_argument('--jobtable', required=True, help='name of dynamodb job table')

args = parser.parse_args()

logging.basicConfig(filename=str(args.log),format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

def get_kill(args):
    while(True):
        if ec2metadata.get('termination-time') is None:
            time.sleep(5)
        else:
            instanceid = ec2metadata.get('instance-id')
            logging.info('This instance : '+instanceid+' will be killed in few second')
            region = os.environ["AWS_REGION"]
            dynamodb = boto3.client('dynamodb',region_name=region)
            #get current job
            response = dynamodb.get_item(TableName=args.jobtable, Key={'Instance':{'S':instanceid}})
            directory = response['Item']['Job']
            queue_url = args.queue
            #add in queue
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

get_kill(args)
