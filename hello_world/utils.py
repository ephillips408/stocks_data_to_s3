import logging

from io import StringIO
from botocore.exceptions import ClientError
from pandas import (DataFrame, to_numeric)

logger = logging.getLogger()
logger.setLevel('INFO')


def get_stock_data_from_ddb(db_client, table_name: str, index_name: str, symbols: list) -> list:
    """
    Gets the stock data of choice from DynamoDB.

    Parameters
    ----------
    `db_client`:
        The DynamoDB client, aka `boto3.client('dynamodb')`

    `table_name`: `str`
        The name of the table to query

    `index_name`: `str`
        The name of the global secondary index on the table that we would like to query.

    `symbols`: `list`
        The stock symbols that we would like to compare on the visualization, i.e. `symbols = [ 'IBM', 'MSFT', 'AAPL' ]`.
        These are also the references to the global secondary index on the table.

    Returns
    -------
    A list of lists that contain the data obtained from DynamoDB. Each list contains the data for the specified symbols.

    """

    try:

        results = []

        for symbol in symbols:

            query_params = {
                'TableName': table_name,
                'IndexName': index_name,
                'KeyConditionExpression': 'symbol = :symbol',
                'ExpressionAttributeValues': {
                    ':symbol': {'S': symbol}
                }
            }

            response = db_client.query(**query_params)

            results.append(response['Items'])

        return results

    except ClientError as err:

        logger.error(
            "Couldn't retreive data from table %s. Here's why: %s: %s",
            table_name,
            err.response["Error"]["Code"],
            err.response["Error"]["Message"],
        )
        raise


def stocks_to_dataframe(response_list: list) -> DataFrame:
    """
    Cleans the data that we returned from the `get_stock_data_from_ddb` function. Note that the returned data is a list of lists.

    Parameters
    ----------
    `response_list`: `list`
        The resulting list from calling the `get_stock_data_from_ddb` function.

    Returns
    -------
    The data returned from DynamoDB as a pandas DataFrame.
    """
    data = []

    # Iterate over the data for each stock. # The data for each symbol is its own list in the response
    for symbol_data in response_list:

        # This is the data for each date for the stock.
        for stock_data in symbol_data:

            data.append(
                {
                    'pk': stock_data['pk']['S'],
                    'symbol': stock_data['symbol']['S'],
                    'date': stock_data['date']['S'],
                    'open_price': stock_data['open_price']['N'],
                    'close_price': stock_data['close_price']['N'],
                    'volume': stock_data['volume']['N']
                }
            )

    results = DataFrame(data)

    return results


def clean_dataframe(df: DataFrame) -> DataFrame:
    """
    Cleans the data that we obtained from the `stocks_to_dataframe` function. 
    The necessary transformations are simply data type conversions.

    Parameters
    ----------
    `df`: `DataFrame`
        The pandas DataFrame that is returned from the `stocks_to_dataframe` function.

    Returns
    -------
    A cleaned DataFrame that is ready to upload to S3.
    """
    df[['open_price', 'close_price', 'volume']] = df[[
        'open_price', 'close_price', 'volume']].apply(to_numeric)
    return df


def upload_file(s3_client, bucket_name: str, file_name: str, clean_df: DataFrame) -> str:
    """
    Uploads the cleaned pandas DataFrame returned from the `clean_dataframe` function to S3.

    Parameters
    ----------
    `s3_client`:
        The S3 client that will be used for the bucket, i.e. `boto3.client('s3')`

    `bucket_name`: `str`
        The name of the bucket where we will upload the data.

    `file_name`: `str`
        The name of the file for the uploaded DataFrame

    `clean_df`: `DataFrame`
        The pandas DataFrame that is returned from the `clean_dataframe` function.

    Returns
    -------
    The response from the upload to S3 process
    """

    try:
        # We are using StringIO to work with the DataFrame in memory
        with StringIO() as csv_buffer:

            clean_df.to_csv(csv_buffer, index=False)

            response = s3_client.put_object(
                Bucket=bucket_name,
                Key=file_name,
                Body=csv_buffer.getvalue()
            )

            status = response['ResponseMetadata']['HTTPStatusCode']

        if status == 200:
            return f"Successful S3 put_object response. Status - {status}"

    except Exception as e:
        return f"Failed to upload DataFrame to S3: {e}"
