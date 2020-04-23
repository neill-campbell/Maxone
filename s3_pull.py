import os
import boto3
import json
import re
from backend_tools.aws.s3 import S3Interface
from boto3.dynamodb.conditions import Key

# set location of credentials file
os.environ["AWS_CONFIG_FILE"] = "./aws_config.ini"

# globals
OUTPUT_DIR = './data/'
DIRECTORY = OUTPUT_DIR
BUCKET = 'smartcoach'
REGION = 'us-east-2'
MAX_DOWNLOAD_COUNT = 5000
FILE_TYPE = ".json"
TABLE_NAME = 'dev-ai-basketball-throw-videos'

# interfaces
dynamodb = boto3.resource('dynamodb', 'us-east-1')
interface = S3Interface(REGION)


def score_parse(json_name, json_score, file_dir):
    save_prefix = re.search('hpe(.*).json', json_name)
    save_prefix = save_prefix.group(1) + '_'
    save_names = json_score.split('\\"')
    i = 1
    while i < len(save_names) - 2:
        save_file = open(OUTPUT_DIR + file_dir + '/' + save_prefix + save_names[i] + ".txt", "w+")
        if save_names[i+2] == 'r':
            save_file.write("0")
            save_file.close()
            print('saved a negative for ' + save_prefix + save_names[i])
        elif save_names[i+2] == 'g':
            save_file.write("1")
            save_file.close()
            print('saved a positive for ' + save_prefix + save_names[i])
        i += 4


def download_dir():
    continuation_token, results = interface.list_objects(BUCKET)
    download_count = 0
    while continuation_token is not None:
        for item in results:

            if FILE_TYPE in item['Key']:
                base, dirname, file_name = item['Key'].split('/')

                # get hpe score information from dynamoDB
                try:
                    table_session = dynamodb.Table(TABLE_NAME)
                    dynamo_grab = table_session.query(
                        IndexName='hpe-index',
                        KeyConditionExpression=Key('hpe').eq(item['Key'])
                    )

                    # make the directory
                    try:
                        os.mkdir(OUTPUT_DIR + dirname)
                    except:
                        print('Dir exists')

                    # save
                    save_grab = json.dumps(dynamo_grab["Items"][0]["score"])
                    score_parse(file_name, save_grab, dirname)

                    # only download s3 .json files if score exists in dynamoDB
                    print('downloading item', item['Key'])
                    interface.download_object(BUCKET, item['Key'], OUTPUT_DIR + dirname + '/' + file_name)
                except:
                    print('No .json file available for ' + file_name)

                download_count += 1

            # break when we have hit our download limit
            if download_count > MAX_DOWNLOAD_COUNT:
                print('Max download cap reached')
                exit(0)

        # get next round of items
        continuation_token, results = interface.list_objects(BUCKET, continuation_token)


if __name__ == "__main__":
    download_dir()
