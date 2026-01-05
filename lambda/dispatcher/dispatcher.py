import boto3, json, os, datetime, re
sfn = boto3.client('stepfunctions')
def lambda_handler(event, context):
    for record in event['Records']:
        try:
            body = json.loads(record['body'])
            for s3_record in body.get('Records', []):
                if 's3' not in s3_record:
                    continue
                
                # サイズが0（フォルダ）の場合もここで弾くのがスマート
                if s3_record['s3']['object'].get('size', 0) == 0:
                    continue

                key = s3_record['s3']['object']['key']
                raw_name = os.path.basename(key).split('.')[0]
                safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', raw_name)[:50]
                now = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
                exec_name = f"{safe_name}_{now}"

                sfn_input = {
                    "bucket_name": s3_record['s3']['bucket']['name'],
                    "src_key": key,
                    "size_bytes": s3_record['s3']['object']['size']
                }
                sfn.start_execution(
                    stateMachineArn=os.environ['STATE_MACHINE_ARN'],
                    name=exec_name,
                    input=json.dumps(sfn_input)
                )
        except Exception as e:
            print(f"Error: {e}"); raise e