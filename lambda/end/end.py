import boto3

s3 = boto3.client('s3')

# ★ここを実際のバケット名に書き換えてください
SplitBucket = 'test-split-20260105'
ErrorBucket = 'test-error-20260107'

def lambda_handler(event, context):
    # Mapステートの出力（配列）が直接渡される想定
    results = event
    
    # 失敗したアイテムのみ抽出
    failed_items = [item for item in results if item.get('status') == 'FAILED']
    
    processed_files = []
    
    for item in failed_items:
        file_path = item.get('ファイルパス')
        if not file_path or file_path == 'unknown':
            continue
            
        try:
            # 1. コピー（キーを同じにすることで構造を維持）
            copy_source = {'Bucket': SplitBucket, 'Key': file_path}
            s3.copy_object(CopySource=SplitBucket, Bucket=ErrorBucket, Key=file_path)
            
            # 2. 元のファイルを削除
            s3.delete_object(Bucket=SplitBucket, Key=file_path)
            
            processed_files.append({"file": file_path, "result": "MOVED"})
        except Exception as e:
            processed_files.append({"file": file_path, "result": "FAILED", "error": str(e)})

    return {
        "status": "COMPLETED",
        "moved_count": len([f for f in processed_files if f['result'] == 'MOVED']),
        "failed_count": len([f for f in processed_files if f['result'] == 'FAILED']),
        "details": processed_files
    }