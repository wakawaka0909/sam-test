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
        MaxNumberOfMessages=1, # 1回に処理するメッセージ数
        WaitTimeSeconds=10     # ロングポーリング（メッセージがなければ最大10秒待機）
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

            records = body.get('Records', [])
            print(f"DEBUG: records type: {type(records)}, count: {len(records)}")
            
            # 元のロジック：S3レコードを回す
            for s3_record in records:
                if 's3' not in s3_record:
                    continue
                if s3_record['s3']['object'].get('size', 0) == 0:
                    continue

                # --- ここから追加・修正処理 ---

                # A. 別のキューにメッセージを移動（コピー）
                sqs.send_message(
                    QueueUrl=JOB_QUEUE_URL,
                    MessageBody=body_str
                )

                # B. Step Functions 起動用の設定
                key = s3_record['s3']['object']['key']
                raw_name = os.path.basename(key).split('.')[0]
                safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', raw_name)[:50]
                now = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
                exec_name = f"{safe_name}_{now}"

                sfn_input = {
                    "bucket_name": s3_record['s3']['bucket']['name'],
                    "src_key": key,
                    "size_bytes": s3_record['s3']['object']['size'],
                    "original_message_id": msg['MessageId']
                }
                
                sfn.start_execution(
                    stateMachineArn=STATE_MACHINE_ARN,
                    name=exec_name,
                    input=json.dumps(sfn_input)
                )

            # D. 全ての処理が完了したら、送信元のキューから削除
            sqs.delete_message(
                QueueUrl=REQUEST_QUEUE_URL,
                ReceiptHandle=receipt_handle
            )
            print(f"Successfully processed and moved message: {msg['MessageId']}")

        except Exception as e:
            print(f"Error: {e}")
            # エラー時は delete_message を呼ばずに終了することで、
            # メッセージはソースキューに残り、再度取得可能になります。
            continue

    return {'statusCode': 200, 'body': f"Processed {len(messages)} messages"}