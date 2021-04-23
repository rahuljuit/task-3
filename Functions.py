import boto3


class CrudOperations:

    def __init__(self):
        self.session_var = boto3.session.Session(profile_name='dynamodb_user')
        self.dynamo_client = self.session_var.resource('dynamodb')

    def create_table_(self, table_name):
        table = self.dynamo_client.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'RollNo',
                    'KeyType': 'HASH',
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'RollNo',
                    'AttributeType': 'N'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )

        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
        return table

    def insert_item(self, roll_no, age, first_name, last_name, sec, table_name):
        table = self.dynamo_client.Table(table_name).put_item(
            Item={
                'RollNo': roll_no,
                'Age': age,
                'First_name': first_name,
                'Last_name': last_name,
                'section': sec
            }
        )
        return table

    def insert_items(self, table_name):
        with self.dynamo_client.Table(table_name).batch_writer() as batch:
            batch.put_item(
                Item={
                       'RollNo': 501,
                       'Age': 10,
                       'First_name': 'Atharva',
                       'Last_name': 'Agarwal',
                       'section': 'A'
                      }
                )
            batch.put_item(
                Item={
                    'RollNo': 502,
                    'Age': 9,
                    'First_name': 'Bijoy',
                    'Last_name': 'Nambiar',
                    'section': 'B'
                }
            )
            batch.put_item(
                Item={
                    'RollNo': 503,
                    'Age': 11,
                    'First_name': 'Shivam',
                    'Last_name': 'Gupta',
                    'section': 'E'
                }
            )
            batch.put_item(
                Item={
                    'RollNo': 504,
                    'Age': 10,
                    'First_name': 'Tushar',
                    'Last_name': 'Gupta',
                    'section': 'D'
                }
            )

    def get_items(self, roll_no, table_name):
        table = self.dynamo_client.Table(table_name)
        response = table.get_item(
            Key={
                'RollNo': roll_no
            }
        )
        return response['Item']

    def update_item_(self, roll_no, value,table_name):
        table = self.dynamo_client.Table(table_name)
        response = table.update_item(
            Key={
                'RollNo': roll_no
            },
            UpdateExpression='SET Age = :val1',
            ExpressionAttributeValues={
                ':val1': value
            }
        )
        return response

    def delete_item_(self, roll_no,table_name):
        table = self.dynamo_client.Table(table_name)
        response = table.delete_item(
            Key={
                'RollNo': roll_no
            }
        )
        return response


MyObj = CrudOperations()
# MyObj.create_table_('Student')
# MyObj.insert_items('Student')
# item = MyObj.get_items(504, 'Student')
# print(item)
# MyObj.update_item_(504, 13, 'Student')
# result = MyObj.insert_item(505, 12, 'Rahul', 'gupta', 'C', 'Student')
# print(result)
# MyObj.delete_item_(505, 'Student')
