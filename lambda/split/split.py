import boto3
import gzip
import io
import os

s3 = boto3.client('s3')

def lambda_handler(event, context):
    bucket = event['bucket_name']
    key = event['src_key']
    
    response = s3.get_object(Bucket=bucket, Key=key)
    
    # 拡張子を剥ぎ取るロジック
    filename = os.path.basename(key)
    table_name = filename
    for _ in range(2):
        table_name, ext = os.path.splitext(table_name)
        if ext not in ['.gz', '.csv']:
            break

    # ★ ここで初期化する必要があります
    uploaded_files = [] 
    
    TARGET_SIZE = 100 * 1024 * 1024 
    READ_CHUNK = 64 * 1024 * 1024
    part_num = 1
    
    with gzip.GzipFile(fileobj=response['Body']) as gz_in:
        current_buffer = []
        current_buffer_size = 0
        pending_line_fragment = b""
        
        while True:
            chunk = gz_in.read(READ_CHUNK)
            if not chunk:
                if pending_line_fragment:
                    current_buffer.append(pending_line_fragment)
                break
            
            chunk = pending_line_fragment + chunk
            last_newline = chunk.rfind(b'\n')
            
            if last_newline == -1:
                pending_line_fragment = chunk
                continue
            
            complete_lines = chunk[:last_newline + 1]
            pending_line_fragment = chunk[last_newline + 1:]
            
            current_buffer.append(complete_lines)
            current_buffer_size += len(complete_lines)
            
            if current_buffer_size >= TARGET_SIZE:
                # アップロードしてパスを取得
                dst_key = upload_part(bucket, table_name, part_num, current_buffer)
                uploaded_files.append(dst_key) # リストに追加
                part_num += 1
                current_buffer = []
                current_buffer_size = 0
        
        if current_buffer:
            dst_key = upload_part(bucket, table_name, part_num, current_buffer)
            uploaded_files.append(dst_key)

        return {
            "status": "SUCCESS",
            "items": [
                {
                    "file_path": f, 
                    "table_name": table_name
                } for f in uploaded_files
            ]
        }

def upload_part(bucket, table_name, part_num, byte_list):
    dst_key = f"{table_name}/{table_name}_part{part_num:03d}.csv.gz"
    gz_buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=gz_buffer, mode='wb', compresslevel=1) as gz_out:
        gz_out.writelines(byte_list)
    
    s3.put_object(Bucket='test-split-20260105', Key=dst_key, Body=gz_buffer.getvalue())
    print(f"Uploaded: {dst_key}")
    return dst_key # パスを返却する