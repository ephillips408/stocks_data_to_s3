### stock-data-to-s3

#### This is still a work in progress.

This code takes data that was pushed to DynamoDB using [this process](https://github.com/ephillips408/lambda_get_stock_data), and pushes the results as a `.csv` file to an S3 bucket. We query only the stocks by utilizing the global secondary index of the table.

This project was created using the [AWS SAM CLI]('https://aws.amazon.com/serverless/sam/). To build the function prior to running, use the command
  * `sam build`
in the terminal, and to run the code, use the command
  * `sam local invoke`

In order to add environment variables, first, they must be created by running the command `export SECRET_NAME=value`, and then referenced in `template.yaml` by

```yaml
Resources:
  YourFunctionName:
    Properties:
      Environment:
        Variables:
          SECRET_NAME: ${SECRET_NAME}
```

The necessary environment variables can be seen in `template.yaml`.