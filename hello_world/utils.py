import logging

from botocore.exceptions import ClientError

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
    A list of the data that was obtained from DynamoDB

    """

    try:

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

            return response

    except ClientError as err:

        logger.error(
            "Couldn't retreive data from table %s. Here's why: %s: %s",
            table_name,
            err.response["Error"]["Code"],
            err.response["Error"]["Message"],
        )
        raise
