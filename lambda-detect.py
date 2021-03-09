import json
import boto3
import time
from datetime import datetime
client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')	
client_step = boto3.client('stepfunctions')

def invoke_sfn(create_clust,terminate_clust,partition_to_process,response_length):
  input_step = {
    "CreateCluster": create_clust,
    "TerminateCluster": terminate_clust,
    "last_partition": partition_to_process,
    "last_partition_full": partition_to_process,
    "response_length": response_length
  }
  try:
        # Comment below code when needed to stop execution for testing
    response_step = client_step.start_execution(

        stateMachineArn='arn:aws:states:us-east-1:XXXXXXXXXX:stateMachine:MyStateMachine',

        input= json.dumps(input_step)

    )
    print('Step success invoke')
    return response_step
  except Exception as e:
    return e


def lambda_handler():

  response = client.list_objects_v2(
  Bucket='kafka-dbstream',
  Delimiter='dbstream.public.orders+',
  Prefix='topics/dbstream.public.orders/',
  FetchOwner=False,
  )
  #  Fetch from DynamoDB
  response_length = len(response['CommonPrefixes'])
  table = dynamodb.Table('lambda-partition-detect')
  dynamo_fetch_part = table.get_item(Key={'partition': 'part'})
  
  part_proc = dynamo_fetch_part['Item']['value']
  print(part_proc)
  part_proc_full = part_proc + 'dbstream.public.orders+'
  current_index = response['CommonPrefixes'].index({'Prefix': part_proc_full})
  print(response_length)
  print(current_index)
  current_time= int(time.time())
  if current_index < response_length - 2:

    partition_to_process = response['CommonPrefixes'][current_index + 1]
    partition_to_process = partition_to_process['Prefix']
    partition_to_process = partition_to_process.replace('dbstream.public.orders+', '')
    
    table_etl_status = dynamodb.Table('etl-job-state')
    try:
      fetch_table_etl_status = table_etl_status.get_item(Key={'partition': partition_to_process})
      fetch_table_item = fetch_table_etl_status.get('Item')
      if fetch_table_item:
        print('etl-status')
        fetch_sfn_arn = fetch_table_etl_status['Item'].get('step_function')
        if fetch_sfn_arn and fetch_sfn_arn != 'null':
          get_sfn = client_step.describe_execution(executionArn=fetch_sfn_arn)
          if get_sfn and get_sfn.get('status') == 'SUCCEEDED' or 'RUNNING':
            return
          else:
            response_invoke = invoke_sfn(True,True,partition_to_process,response_length)
            update_table_job_state = table_etl_status.update_item(Key={'partition': partition_to_process}, UpdateExpression="set job_status=:s, job_timestamp=:t, step_function=:f",
            ExpressionAttributeValues={':s': 'queued',':t': current_time,':f': response_invoke['executionArn']},ReturnValues="UPDATED_NEW")
        else:
          print('inside if block')

          response_invoke = invoke_sfn(True,True,partition_to_process,response_length)
          fetch_sfn_arn = response_invoke.get('executionArn')
          update_table_job_state = table_etl_status.update_item(Key={'partition': partition_to_process}, UpdateExpression="set job_status=:s, job_timestamp=:t, step_function=:f",
          ExpressionAttributeValues={':s': 'queued',':t': current_time,':f': fetch_sfn_arn},ReturnValues="UPDATED_NEW")
          print('keyError-step-function')
      else:
        response_invoke = invoke_sfn(True,True,partition_to_process,response_length)
        fetch_sfn_arn = response_invoke.get('executionArn')
        add_new_partition_item_ddb = table_etl_status.put_item(Item={'partition': partition_to_process,'job_status': 'queued','job_timestamp': current_time, 'step_function': response_invoke.get('executionArn')})

    except Exception as e:
      print(e)
  else:
  	print('No Action')

if __name__ == '__main__':
  lambda_handler()
