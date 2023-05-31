from chalice import Chalice
import boto3
import uuid
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

# IMPORTANTE!!!!!!!!!
# Antes de dar deploy na aplicação executar os seguintes passos:
# sobe o docker do localstack e dentro da pasta .chalice rode o comando a baixo:
# awslocal cloudformation deploy --template-file dynamodb_cf_template.yaml --stack-name "my-stack"

app = Chalice(app_name='bank-payments-project')

@app.route('/{table_name}/account/{accountID}', methods=['GET'])
def read_dynamodb_table_item_by_accountid(table_name, accountID):
    try:
        dynamodb_client = boto3.client("dynamodb", region_name='us-east-1', endpoint_url='http://host.docker.internal:4566/')
        response = dynamodb_client.query(
        ExpressionAttributeValues={
            ':v1': {
                'S': accountID,
            },
        },
        KeyConditionExpression='AccountID = :v1',
        TableName=table_name,
        )
    except ClientError:
        raise
    else:
        return response['Items']

@app.route('/{table_name}/date/{schedualedDate}/status/{status}', methods=['GET'])
def read_dynamodb_table_item_case2(table_name, schedualedDate, status):
    try:
        dynamodb_client = boto3.resource("dynamodb", region_name='us-east-1', endpoint_url='http://host.docker.internal:4566/')
        table = dynamodb_client.Table(table_name)
        response = table.query(
            IndexName="date_by_status-index",
            KeyConditionExpression=Key('SchedualedDate').eq(schedualedDate) & Key('PaymentStatus').eq(status),
        )
    except ClientError:
        raise
    else:
        return response['Items']

@app.route('/{table_name}/schedualed_payments/account/{accountID}', methods=['GET'])
def read_dynamodb_table_item_case1(table_name, accountID):
    statusAccountID = f'schedualed#{accountID}'
    currentDay = '20230530'
    ninetyDaysLater = '20230828'
    try:
        dynamodb_client = boto3.resource("dynamodb", region_name='us-east-1', endpoint_url='http://host.docker.internal:4566/')
        table = dynamodb_client.Table(table_name)
        response = table.query(
            IndexName="schedualed_user_by_date-index",
            KeyConditionExpression=Key('StatusAccountID').eq(statusAccountID) & Key('SchedualedDate').between(currentDay, ninetyDaysLater),
        )
    except ClientError:
        raise
    else:
        return response['Items']


@app.route('/{table_name}', methods=['POST'])
def add_dynamodb_table_item(table_name):
    data = app.current_request.json_body
    accountID = data['AccountID']
    paymentID = str(uuid.uuid4())[:6]
    try:
        dynamodb_resource = boto3.resource("dynamodb", region_name='us-east-1', endpoint_url='http://host.docker.internal:4566/')
        table = dynamodb_resource.Table(table_name)
        response = table.put_item(
            Item={
                'AccountID': data['AccountID'],
                'PaymentID': paymentID,
                'SchedualedDate': data['SchedualedDate'],
                'PaymentStatus': 'schedualed',
                'DataBlob': 'some data',
                'StatusAccountID': f'schedualed#{accountID}'
            }
        )
    except ClientError:
        raise
    else:
        return {
            'Message': "Payment posted with success!",
            'PaymentID': paymentID,
            'HTTPStatusCode': response['ResponseMetadata']['HTTPStatusCode']
        }

def process_payment(table_name, accountID, paymentID, data):
    delete_dynamodb_table_item(table_name, accountID, paymentID)
    newPaymentID = str(uuid.uuid4())[:6]
    try:
        dynamodb_resource = boto3.resource("dynamodb", region_name='us-east-1', endpoint_url='http://host.docker.internal:4566/')
        table = dynamodb_resource.Table(table_name)
        response = table.put_item(
            Item={
                'AccountID': accountID,
                'PaymentID': newPaymentID,
                'SchedualedDate': data['SchedualedDate'],
                'PaymentStatus': 'processed',
                'DataBlob': 'some data',
                'StatusAccountID': f'processed#{accountID}'
            }
        )
    except ClientError:
        raise
    else:
        return {
            'Message': "Payment processed with success!",
            'HTTPStatusCode': response['ResponseMetadata']['HTTPStatusCode'],
        }

def update_payment_status(table_name, accountID, paymentID, newStatus):
    try:
        dynamodb_client = boto3.client("dynamodb", region_name='us-east-1', endpoint_url='http://host.docker.internal:4566/')
        response = dynamodb_client.update_item(
            TableName = table_name,
            ExpressionAttributeNames={
            '#S': 'PaymentStatus',
            '#SA': 'StatusAccountID'
            },
            ExpressionAttributeValues={
                ':s': {
                    'S': newStatus
                },
                ':sa': {
                    'S': f'{newStatus}#{accountID}'
                },
            },
            Key={
                'AccountID': {
                    'S': accountID,
                },
                'PaymentID': {
                    'S': paymentID,
                },
            },
            ReturnValues='ALL_NEW',
            UpdateExpression='SET #S = :s, #SA = :sa'
        )
    except ClientError:
        raise
    else:
        return response['Attributes']


@app.route('/{table_name}/account/{accountID}/payment/{paymentID}', methods=['PUT'])
def update_dynamodb_table_item(table_name, accountID, paymentID):
    data = app.current_request.json_body
    newStatus = data['NewStatus']
    try:
        if (newStatus == 'processed'):
            response = process_payment(table_name, accountID, paymentID, data)
        else:
            response = update_payment_status(table_name, accountID, paymentID, newStatus)
    except ClientError:
        raise
    else:
        return response


@app.route('/{table_name}/account/{accountID}/payment/{paymentID}', methods=['DELETE'])
def delete_dynamodb_table_item(table_name, accountID, paymentID):
    dynamodb_resource = boto3.resource("dynamodb", region_name='us-east-1', endpoint_url='http://host.docker.internal:4566/')
    try:
        table = dynamodb_resource.Table(table_name)
        response = table.delete_item(
            Key={
                'AccountID': accountID,
                'PaymentID': paymentID
            }
        )
    except ClientError:
        raise
    else:
        return {
            'Message' : "Payment deleted with success!",
            'HTTPStatusCode': response['ResponseMetadata']['HTTPStatusCode']
        }