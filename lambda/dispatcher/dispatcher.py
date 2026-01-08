import boto3, json, os, datetime, re

sqs = boto3.client('sqs')
sfn = boto3.client('stepfunctions')

# 環境変数
REQUEST_QUEUE_URL = os.environ['REQUEST_QUEUE_URL']
JOB_QUEUE_URL = os.environ['JOB_QUEUE_URL']
STATE_MACHINE_ARN = os.environ['STATE_MACHINE_ARN']

def lambda_handler(event, context):
    # 1. 自らメッセージを取りに行く
    response = sqs.receive_message(
        QueueUrl=REQUEST_QUEUE_URL,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=10
    )
    
    messages = response.get('Messages', [])
    if not messages:
        print("No messages to process.")
        return {'statusCode': 200, 'body': 'No messages'}

    for msg in messages:
        receipt_handle = msg['ReceiptHandle']
        body_str = msg['Body']
        
        try:
            body = json.loads(body_str)
            # 文字列として2重エンコードされている場合の対策
            if isinstance(body, str):
                body = json.loads(body)

            records = body.get('Records', [])
            print(f"DEBUG: records count: {len(records)}")
            
            for s3_record in records:
                print("--- Loop Start ---")
                
                # 1. 's3' キーの存在確認
                if 's3' not in s3_record:
                    print(f"DEBUG: 's3' not in record. Keys found: {list(s3_record.keys())}")
                    continue
                print("DEBUG: Step 1 Passed ('s3' key found)")

                # 2. size の取得と判定
                s3_data = s3_record['s3']
                obj_size = s3_data['object'].get('size', 0)
                print(f"DEBUG: Step 2 - Object size is {obj_size}")

                if int(obj_size) == 0:
                    print("DEBUG: Step 2 Skipped - Size is 0 (Folder or Empty)")
                    continue
                print("DEBUG: Step 2 Passed (Size > 0)")

                # 3. キー名の取得
                key = s3_data['object'].get('key')
                print(f"DEBUG: Step 3 - Target Key: {key}")

                # 4. SFN起動
                print("DEBUG: Step 4 - Attempting start_execution...")
                res = sfn.start_execution(
                    stateMachineArn=STATE_MACHINE_ARN,
                    input=json.dumps({
                        "bucket_name": s3_data['bucket']['name'],
                        "object_key": key
                    })
                )
                print(f"DEBUG: Step 5 SUCCESS! ARN: {res['executionArn']}")

            # 成功したらメッセージ削除
            sqs.delete_message(QueueUrl=REQUEST_QUEUE_URL, ReceiptHandle=receipt_handle)
            print("DEBUG: Message deleted from SQS")

        except Exception as e:
            # ここが足りなかった except ブロックです
            print(f"DEBUG: Error processing message: {e}")
            import traceback
            traceback.print_exc()

    return {'statusCode': 200, 'body': f"Processed {len(messages)} messages"}