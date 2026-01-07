import boto3

s3 = boto3.client('s3')

SourceBucket = 'test-20260105'
SplitBucket = 'test-split-20260105'
ErrorBucket = 'test-error-20260107'

def lambda_handler(event, context):
    # Mapステートの出力（配列）が直接渡される想定
    results = event
    
    # 失敗したアイテムのみ抽出
    failed_items = [item for item in results if item.get('status') == 'FAILED']
    
    processed_files = []
    moved_originals = set()

    for item in failed_items:
        src_path = item.get('元ファイルパス')
        split_path = item.get('分割ファイルパス')

        # 分割ファイルの移動
        if not split_path and split_path == 'unknown':
            try:
                # 1. コピー
                copy_split = {'Bucket': SplitBucket, 'Key': split_path}
                s3.copy_object(CopySource=copy_split, Bucket=ErrorBucket, Key=split_path)
            
                # 2. 削除
                s3.delete_object(Bucket=SplitBucket, Key=file_path)
            
                processed_files.append({"file": split_path, "result": "MOVED"})
            except Exception as e:
                processed_files.append({"file": split_path, "result": "FAILED", "error": str(e)})

        # 元ファイルの移動
        if not src_path and src_path not in moved_originals:
            try:
                # 1. コピー
                copy_src = {'Bucket': SourceBucket, 'Key': src_path}
                s3.copy_object(CopySource=copy_src, Bucket=ErrorBucket, Key=src_path)
            
                # 2. 削除
                s3.delete_object(Bucket=SplitBucket, Key=file_path)
            
                processed_files.append({"file": split_path, "result": "MOVED"})
            except Exception as e:
                processed_files.append({"file": split_path, "result": "FAILED", "error": str(e)})

    return {
        "status": "COMPLETED",
        "details": processed_files
    }