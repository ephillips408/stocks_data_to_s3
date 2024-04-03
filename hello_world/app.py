import os
import logging
import json
import boto3

from utils import (get_stock_data_from_ddb,
                   stocks_to_dataframe, clean_dataframe, upload_file)

logger = logging.getLogger()
logger.setLevel('INFO')


def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """

    # Initialize the AWS resource clients
    db_client = boto3.client('dynamodb')
    bucket_client = boto3.client('s3')

    # Get the data from DynamoDB
    db_data = get_stock_data_from_ddb(
        db_client=db_client,
        index_name='SymbolIndex',
        table_name=os.environ['TABLE_NAME'],
        symbols=['IBM', 'MSFT']
    )

    logger.info('Successfully obtained data from DynamoDB')

    stocks_df = stocks_to_dataframe(
        response_list=db_data
    )

    logger.info('Successfully converted data to DataFrame')

    clean_stocks_df = clean_dataframe(
        df=stocks_df
    )

    logger.info('Successfully cleaned the DataFrame')

    upload_result = upload_file(
        s3_client=bucket_client,
        bucket_name=os.environ['BUCKET_NAME'],
        file_name='viz-data.csv',
        clean_df=clean_stocks_df
    )

    logger.info('Attempted the upload to S3. See final message for status')

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": upload_result,
            # "location": ip.text.replace("\n", "")
        }),
    }
