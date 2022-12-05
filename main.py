import yaml
import boto3
import tarfile
import time
import logging
from cloudwatch import cloudwatch


def load_data_from_s3(
        boto_session, bucket_name, data_filename):
    s3 = boto_session.client('s3')

    s3.download_file(bucket_name, data_filename, 'data/' + data_filename)


def bundle_directory(dir_name):
    with tarfile.open(f'{dir_name}.tar.gz', 'w:gz') as tar:
        tar.add(dir_name)


def upload_to_s3(boto_session, filename, bucket_name):
    s3 = boto_session.client('s3')
    s3.upload_file('data/' + filename, bucket_name, filename)


if __name__ == "__main__":
    # Get configuration
    CONFIG_FILE = 'config.yaml'
    with open(CONFIG_FILE, 'r') as f:
        config_data = yaml.safe_load(f)
    REGION = config_data['AWS']['REGION']
    S3_BUCKET = config_data['AWS']['S3']['S3_BUCKET']
    LOG_GROUP = config_data['AWS']['CLOUDWATCH']['LOG_GROUP']
    AWS_ACCESS_KEY_ID = config_data['AWS']['AUTH']['AWS_ACCESS_KEY_ID']
    AWS_SECRET_ACCESS_KEY = config_data['AWS']['AUTH']['AWS_SECRET_ACCESS_KEY']
    LOG_STREAM = str(int(time.time()))
    S3_FILENAME = config_data['DATA']['LOG']
    CLOUDWATCH_LOGS = config_data['OPTIONS']['LOG_TO_CLOUDWATCH']
    S3_UPLOAD = config_data['OPTIONS']['SAVE_TO_S3']

    # Create S3 Boto3 Session
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    if CLOUDWATCH_LOGS:
        # Set up CloudWatch
        handler = cloudwatch.CloudwatchHandler(
            log_group=LOG_GROUP,
            log_stream=LOG_STREAM,
            region=REGION,
            access_id=AWS_ACCESS_KEY_ID,
            access_key=AWS_SECRET_ACCESS_KEY
        )
        logger.addHandler(handler)

    # Load the dataset
    logger.info('Loading dataset...')
    load_data_from_s3(session, S3_BUCKET, S3_FILENAME)

    for count in range(5):
        logger.info(f'Logging {count+1}/5')
        time.sleep(3)

    logger.info('Updating local timestamp file...')
    with open('data/' + S3_FILENAME, 'a') as f:
        f.write(f"Updated on {time.strftime('%Y-%m-%d %H:%M:%S')}\n")

    if S3_UPLOAD:
        # Persist final model in S3
        logger.info('Uploading to data store...')
        upload_to_s3(session, S3_FILENAME, S3_BUCKET)
