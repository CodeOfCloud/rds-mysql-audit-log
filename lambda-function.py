
import boto3, botocore, json, re
from datetime import datetime

import logging
logging.basicConfig(format='%(levelname)s:%(asctime)s:%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

REGION = "ap-southeast-1"
DB_INSTANCE_NAME = "small-mysql"
BUCKET_NAME = "rds-mysql-audit-log"
LOG_BUCKET_PREFIX = DB_INSTANCE_NAME+'_demo/audit-log'
DDB_AUDIT_LOG_METADATA_TABLE = "mysql_audit_log_metadata"
LOG_NAME_PREFIX = "audit"
KINESIS_STREAM_NAME = "mysql-audit-log"

def get_metadata_from_ddb(table_name):
    dynamodb = boto3.resource('dynamodb')
    metadata_table = dynamodb.Table(table_name)
    log_metadata = metadata_table.get_item(
        Key={
            'DatabaseInstanceName': DB_INSTANCE_NAME
        }
    )

    ## example message body from dynamodb table
    '''
    {
        "DatabaseInstanceName": "small-mysql"
        "AuditLogLastWritten": 1713161934,
		"LogFileName": "audit/server_audit.log"
    }
    '''
	
    logger.info(f"get ddb - {log_metadata['Item']}")
    return log_metadata

def update_metadata_in_ddb(table_name, last_written_time, log_file_name):
	dynamodb = boto3.resource('dynamodb')
	metadata_table = dynamodb.Table(table_name)
	response = metadata_table.update_item(Key={'DatabaseInstanceName': DB_INSTANCE_NAME},
        UpdateExpression="set AuditLogLastWritten=:last_written_time, LogFileName=:log_file_name",
        ExpressionAttributeValues={
            ':last_written_time': last_written_time,
			':log_file_name': log_file_name
			},
        ReturnValues="ALL_NEW")
	logger.info(f'update ddb response - {response}')
	return
    

def download_latest_db_audit_file(db_instance_name, log_name_prefix, last_written_time):
	new_last_written_time = []   # to hold last written time from this batch call
	rds_client = boto3.client('rds',region_name=REGION)
	log_files = []  # to log files
	log_marker = ""   # to holder marker for pagination

	# download audit log file
	more_logs_remaining = True
	while more_logs_remaining:
		db_audi_logs = rds_client.describe_db_log_files(DBInstanceIdentifier=db_instance_name, FilenameContains=log_name_prefix, FileLastWritten=last_written_time, Marker=log_marker)
		if 'Marker' in db_audi_logs and db_audi_logs['Marker'] != "":
			log_marker = db_audi_logs['Marker']
		else:
			more_logs_remaining = False

		for db_audi_log in db_audi_logs['DescribeDBLogFiles']:
			logger.info(f"Reviewing log file: {db_audi_log['LogFileName']} found and with LastWritten value of: {db_audi_log['LastWritten']}")
			if int(db_audi_log['LastWritten']) > last_written_time:
				logger.info(f'proceed to download {db_audi_log["LogFileName"]}')
				try: 
					more_logs_remaining_in_file = True
					file_log_marker = '0'
					while more_logs_remaining_in_file:
						log_file_response = rds_client.download_db_log_file_portion(DBInstanceIdentifier=db_instance_name, LogFileName=db_audi_log['LogFileName'], Marker=file_log_marker)
						db_audi_log.update({'LogFileData': log_file_response['LogFileData']})
						log_files.append(db_audi_log)
						new_last_written_time.append(db_audi_log['LastWritten'])
						logger.info (f"File download marker: {file_log_marker}")
						# update last written time
						if log_file_response['AdditionalDataPending']:
							file_log_marker = log_file_response['Marker']
						else:
							more_logs_remaining_in_file = False
				except Exception as e:
					logger.info (f"File download failed: {e}")
					continue
			else:
				logger.info(f'log {db_audi_log["LogFileName"]} is already downloaded')

	# log_files example
	'''
	{
		"LogFileName": "audit/server_audit.log",
		"LastWritten": 1712624678983,
		"Size": 205544,
		"LogFileData": "some chunk data"
	},
	'''

	return log_files, new_last_written_time
	

def upload_log_file_to_s3(log_file_as_bytes, s3_bucket_name, s3_object_name):
	s3_client = boto3.client('s3', region_name=REGION)
	try:
		s3_response = s3_client.put_object(Bucket=s3_bucket_name, Key=s3_object_name, Body=log_file_as_bytes)
	except botocore.exceptions.ClientError as e:
		logger.info ("Error writing object to S3 bucket, S3 ClientError: " + e.response['Error']['Message'])
		return
	logger.info(f"Uploaded log file {s3_object_name} to S3 bucket {s3_bucket_name}")


def batch_load_to_kinesis(log_records, batch_size, stream_name):
	try:
		kinesis_client = boto3.client('kinesis')
		if len(log_records) == 0:
			return 'no new record to load to kinesis stream'
		
		# records more than designed batch size, load in batch
		elif len(log_records) > batch_size:
			logger.info(f'len log records {len(log_records)} batch size {batch_size}')
			batches = [log_records[i:i+batch_size] for i in range(0, len(log_records), batch_size)]
			logger.info(f'len batches {len(batches)}')

			for batch in batches:
				logger.info(f'len batch {len(batch)}')
				records = [
					{
						'Data': json.dumps(record),
						'PartitionKey': record['timestamp']
					}
					for record in batch
				]
				response = kinesis_client.put_records(
					StreamName=stream_name,
					Records=records
				)
				logger.info(f"in batch response - failed count {response['FailedRecordCount']}, metadata {response['ResponseMetadata']}")
		
		# records less than designed batch size, load in batch
		else:
			records = [
				{
					'Data': json.dumps(record),
					'PartitionKey': record['timestamp']
				}
				for record in log_records
			]
			response = kinesis_client.put_records(
				StreamName=stream_name,
				Records=records
			)
			logger.info(f"not in batch response - failed count {response['FailedRecordCount']}, metadata {response['ResponseMetadata']}")

	except botocore.exceptions.ClientError as e:
		logger.info (f"Error writing object to kinesis stream, kinesis ClientError: {e.response['Error']['Message']}")
		return

	logger.info(f"Uploaded logs to kinesis stream %{stream_name}")
		
def lambda_handler(event, context):
	metadata=get_metadata_from_ddb(DDB_AUDIT_LOG_METADATA_TABLE)
	last_written_time = int(metadata['Item']['AuditLogLastWritten'])    # convert decimal to int
	last_written_log_file_name = metadata['Item']['LogFileName'] 

	log_files, new_last_written_time = download_latest_db_audit_file(DB_INSTANCE_NAME, LOG_NAME_PREFIX, last_written_time)

	# sort log data file in case multiple audit log file returned
	sorted_log_files = sorted(log_files, key=lambda x: x['LastWritten'])
	logger.info(f"sorted log files -- {[(each['LastWritten'], each['LogFileName'], each['Size']) for each in sorted_log_files]} stream_name")

	# run through each audit log file, size is controlled in the option group parameter
	for each_log_file in sorted_log_files:
		# collect latest messages to kinesis stream and s3
		log_records= each_log_file['LogFileData'].split('\n')
		log_file_name = each_log_file['LogFileName']
		to_s3_full_log_records = []

		logger.info(f'existing audit log file name -- {log_file_name}')
		if log_file_name == last_written_log_file_name:
			logger.info(f'same audit log file -- {log_file_name}')
			for each_log_record in log_records:
				try: 
					record_time_str = each_log_record.split(',')[0]
					record_time_obj = datetime.strptime(record_time_str+"+0000", '%Y%m%d %H:%M:%S%z') # time string in utc
					record_time_epoch = int(record_time_obj.timestamp())*1000
					# compare with last time record time
					if record_time_epoch > last_written_time:
						to_s3_full_log_records.append(each_log_record)
				except Exception as e:
					logger.info(f'Exception on unformatted line: {e}')
					pass
			logger.info(f'latest message number -- {len(to_s3_full_log_records)}' )
		else:
			logger.info(f'new audit log file name -- {log_file_name}')
			# to_s3_full_log_records.extend(log_records[:-1])
			to_s3_full_log_records.extend(log_records[:-1])

		# at last, update new last written time from this batch call
		update_metadata_in_ddb(DDB_AUDIT_LOG_METADATA_TABLE, max(new_last_written_time), log_file_name)

		# batch load latest messages s3 on each audit log file size maximum
		upload_log_file_to_s3("\n".join(to_s3_full_log_records).encode(errors='ignore'), BUCKET_NAME, LOG_BUCKET_PREFIX+'-'+str(each_log_file['LastWritten']))

		# filter rdsadmin out - internal operation, before load to kinesis stream
		audit_log_keys = ["timestamp", "serverhost", "username", "host", "connectionid", "queryid", "operation", "database", "object", "retcode"]
		def split_by_comma_ignore_quotes(str):
			return re.split(r",(?=(?:[^']*'[^']*')*[^']*$)", str)
		logger.info(f'new audit log records number -- {len(to_s3_full_log_records)}')

		to_kinesis_log_records =  [dict(zip(audit_log_keys, split_by_comma_ignore_quotes(x))) for x in to_s3_full_log_records if x.split(',')[2] != "rdsadmin"]
		logger.info(f'to kinesis log records number - {len(to_kinesis_log_records)}')
		if len(to_kinesis_log_records) > 1:
			logger.info(f'show last records -- {to_kinesis_log_records[-1]}')
		
		
		batch_size = 100
		batch_load_to_kinesis(to_kinesis_log_records, batch_size, KINESIS_STREAM_NAME)

		logger.info('done')
		return 'done'
