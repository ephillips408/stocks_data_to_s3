import logging

from botocore.exceptions import ClientError
from pandas import DataFrame

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
    A pandas DataFrame that is properly cleaned for the upload to S3.
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
