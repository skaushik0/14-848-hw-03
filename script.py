#! /usr/bin/env python3

'''
script.py:  Homework for 14-848 ("HW-03: NoSQL").
            Creates a DynamoDB table, loads it with movie
            test data and queires them.
'''
import os
import json
import zipfile
import tempfile
import traceback
import shutil
import decimal
import inspect

import boto3

AWS_DEFAULT_REGION = 'us-east-1'
AWS_S3_BUCKET_NAME = 'skaushik0-14-848-hw-03-s3'
AWS_S3_REMOTE_PATH = 'movies.json'
AWS_DYNAMO_DB_NAME = 'skaushik0-14-848-hw-03-db'
AWS_DYNAMO_DB_CONF = {
    'TableName': AWS_DYNAMO_DB_NAME,
    'KeySchema': [
        {
            'AttributeName': 'year',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'title',
            'KeyType': 'RANGE'
        }
    ],
    'AttributeDefinitions': [
        {
            'AttributeName': 'year',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'title',
            'AttributeType': 'S'
        }
    ],
    'ProvisionedThroughput': {
        'ReadCapacityUnits': 4,
        'WriteCapacityUnits': 4
    }
}

SRC_PATH = os.path.dirname(os.path.realpath(__file__))
DAT_PATH = os.path.join(SRC_PATH, 'data')
MOV_PATH = os.path.join(DAT_PATH, 'aws', 'moviedata.zip')


def s3_create(client):
    '''
    Create an S3 bucket.
    '''
    try:
        client.create_bucket(Bucket=AWS_S3_BUCKET_NAME)
    except: # pylint: disable=bare-except
        print('err: {0}: failed to create bucket: {1}'.format(
            inspect.stack()[0][3],
            AWS_S3_BUCKET_NAME
        ))
        traceback.print_exc()
        return False

    return True


def db_create(client):
    '''
    Create a DynamoDB table.
    '''
    try:
        client.create_table(**AWS_DYNAMO_DB_CONF)
    except Exception as db_exc: # pylint: disable=broad-except
        if db_exc.__class__.__name__ == 'ResourceInUseException':
            print('wrn: {0}: table already exists: {1}'.format(
                inspect.stack()[0][3],
                AWS_DYNAMO_DB_NAME
            ))
            return True

        print('err: {0}: failed to create table: {1}'.format(
            inspect.stack()[0][3],
            AWS_DYNAMO_DB_NAME
        ))
        traceback.print_exc()
        return False

    print('inf: {0}: waiting for table creation: {1}'.format(
        inspect.stack()[0][3],
        AWS_DYNAMO_DB_NAME
    ))
    waiter = client.get_waiter('table_exists')
    waiter.wait(TableName=AWS_DYNAMO_DB_NAME)

    return True


def s3_upload(client, path=MOV_PATH):
    '''
    Upload the movie data file to an S3 bucket.
    '''
    with zipfile.ZipFile(path) as zip_file:
        if zip_file.testzip() is not None:
            print("err: {0}: invalid archive: '{1}'".format(
                inspect.stack()[0][3], MOV_PATH
            ))
            return False

        tmp_dir = None
        for mem_file in zip_file.infolist():
            try:
                tmp_dir = tempfile.mkdtemp()
                print('inf: {0}: tmp_dir: {1}'.format(
                    inspect.stack()[0][3], tmp_dir
                ))

                zip_file.extract(mem_file, tmp_dir)
                print('inf: {0}: extract: {1}'.format(
                    inspect.stack()[0][3],
                    mem_file.filename
                ))

                ext_file = os.path.join(tmp_dir, mem_file.filename)
                client.upload_file(
                    ext_file, AWS_S3_BUCKET_NAME, AWS_S3_REMOTE_PATH
                )
                print('inf: {0}: upload: {1} to s3://{2}'.format(
                    inspect.stack()[0][3],
                    ext_file,
                    os.path.join(AWS_S3_BUCKET_NAME, AWS_S3_REMOTE_PATH)
                ))

            except: # pylint: disable=bare-except
                print('err: {0}: failed to extract or upload:'.format(
                    inspect.stack()[0][3]
                ))
                traceback.print_exc()
                shutil.rmtree(tmp_dir)
                return False

            if tmp_dir is not None:
                shutil.rmtree(tmp_dir)

    return True


def db_upload(client, path=MOV_PATH):
    '''
    Read the movie data file, and write them to the DynamoDB table.
    '''
    with zipfile.ZipFile(path) as zip_file:
        if zip_file.testzip() is not None:
            print("err: {0}: invalid archive: '{1}'".format(
                inspect.stack()[0][3],
                MOV_PATH,
            ))
            return False

        tmp_dir = None
        for mem_file in zip_file.infolist():
            try:
                tmp_dir = tempfile.mkdtemp()
                print('inf: {0}: tmp_dir: {1}'.format(
                    inspect.stack()[0][3],
                    tmp_dir
                ))

                zip_file.extract(mem_file, tmp_dir)
                print('inf: {0}: extract: {1}'.format(
                    inspect.stack()[0][3],
                    mem_file.filename
                ))

                ext_file = os.path.join(tmp_dir, mem_file.filename)
                table = client.Table(AWS_DYNAMO_DB_NAME)

                with open(ext_file, 'r') as mov_data:
                    movies = json.load(
                        mov_data, parse_float=decimal.Decimal
                    )

                    # Limiting this to 512 because it takes too long.
                    to_insert = movies[:512]
                    for movie in to_insert:
                        table.put_item(Item=movie)

                    print('inf: {0}: inserted {1} items into db: {2}'.format(
                        inspect.stack()[0][3],
                        len(to_insert),
                        AWS_DYNAMO_DB_NAME
                    ))

            except: # pylint: disable=bare-except
                print('err: {0}: failed to extract, parse or insert:'.format(
                    inspect.stack()[0][3]
                ))
                traceback.print_exc()
                shutil.rmtree(tmp_dir)
                return False

            if tmp_dir is not None:
                shutil.rmtree(tmp_dir)

    return True


def db_query(client, year):
    '''
    Run queries for movies in the database matching a given year.
    '''
    table = client.Table(AWS_DYNAMO_DB_NAME)
    response = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('year').eq(year)
    )

    print("inf: {0}: 'KeyConditionExpression=Key('year').eq({1})'".format(
        inspect.stack()[0][3],
        year
    ))

    for resp in response['Items']:
        print(' - {0:<64}{1}'.format(resp['title'], resp['year']))


def main():
    '''
    Create client interfaces, call all the functions to create
    a bucket, table and query data from the table.
    '''
    s3_client = boto3.client('s3', region_name=AWS_DEFAULT_REGION)
    db_client = boto3.client('dynamodb', region_name=AWS_DEFAULT_REGION)
    db_native = boto3.resource('dynamodb')

    s3_create(s3_client)
    print('inf: {0}: created S3 bucket: s3://{1}'.format(
        inspect.stack()[0][3],
        AWS_S3_BUCKET_NAME
    ))

    s3_upload(s3_client, MOV_PATH)
    print('inf: {0}: uploaded file to S3 bucket: s3://{1}'.format(
        inspect.stack()[0][3],
        AWS_S3_BUCKET_NAME
    ))

    db_create(db_client)
    print('inf: {0}: created DynamoDB table: {1}'.format(
        inspect.stack()[0][3],
        AWS_DYNAMO_DB_NAME
    ))

    db_upload(db_native)
    print('inf: {0}: uploaded data DynamoDB table: {1}'.format(
        inspect.stack()[0][3],
        AWS_DYNAMO_DB_NAME
    ))

    years = [1993, 1984, 2000, 2003]
    print('inf: {0}: running queries on DynamoDB table: {1}, years: {2}'.format(
        inspect.stack()[0][3],
        AWS_DYNAMO_DB_NAME,
        years
    ))
    for year in years:
        db_query(db_native, year)


if __name__ == '__main__':
    main()
