import json
import boto3
import csv
import io
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Configuration
BUCKET_NAME = 'multi-source-data-sea'
ECOMMERCE_FILE = 'raw/ecommerce/Ecommerce_Consumer_Behavior_Analysis_Data.csv'
MANUFACTURING_FILE = 'raw/manufacturing/smart_manufacturing_data.csv'
ECOMMERCE_STREAM = 'ecommerce-data-stream'
MANUFACTURING_STREAM = 'manufacturing-data-stream'

# Performance Settings
MAX_THREADS = 50
RECORDS_PER_BATCH = 500
MAX_RETRIES = 2

kinesis = boto3.client('kinesis')
s3 = boto3.client('s3')

def process_file_to_kinesis(bucket, key, stream_name, pk_field='Customer_ID'):
    """Process entire CSV and send to Kinesis, detecting natural end of dataset"""
    response = s3.get_object(Bucket=bucket, Key=key)
    reader = csv.DictReader(io.TextIOWrapper(response['Body']))
    
    total_records = 0
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = []
        batch = []
        
        for row in reader:
            row['ingest_timestamp'] = datetime.now().isoformat()
            batch.append(row)
            
            if len(batch) >= RECORDS_PER_BATCH:
                futures.append(executor.submit(
                    send_kinesis_batch,
                    stream_name=stream_name,
                    records=batch.copy(),
                    pk_field=pk_field
                ))
                batch = []
                total_records += RECORDS_PER_BATCH
        
        # Send final partial batch
        if batch:
            futures.append(executor.submit(
                send_kinesis_batch,
                stream_name=stream_name,
                records=batch,
                pk_field=pk_field
            ))
            total_records += len(batch)
        
        # Wait for all batches to complete and count successes
        successful_records = 0
        for future in as_completed(futures):
            successful_records += future.result()
            
        print(f"Finished processing {stream_name}. Total records: {total_records}, Successful writes: {successful_records}")
        return successful_records

def send_kinesis_batch(stream_name, records, pk_field):
    """Send batch using PutRecords API"""
    entries = [{
        'Data': json.dumps(record),
        'PartitionKey': str(record.get(pk_field, 'default'))
    } for record in records]
    
    for attempt in range(MAX_RETRIES):
        try:
            response = kinesis.put_records(
                Records=entries,
                StreamName=stream_name
            )
            return len(records) - response['FailedRecordCount']
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                print(f"Failed batch after {MAX_RETRIES} attempts: {str(e)}")
                return 0
            time.sleep(2 ** attempt)

def lambda_handler(event, context):
    start_time = time.time()
    results = {}
    
    try:
        # Process both files in parallel
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(
                    process_file_to_kinesis,
                    BUCKET_NAME, ECOMMERCE_FILE, ECOMMERCE_STREAM, 'Customer_ID'
                ): 'ecommerce',
                executor.submit(
                    process_file_to_kinesis,
                    BUCKET_NAME, MANUFACTURING_FILE, MANUFACTURING_STREAM, 'machine_id'
                ): 'manufacturing'
            }
            
            # Wait for both streams to complete
            for future in as_completed(futures):
                stream_name = futures[future]
                results[stream_name] = future.result()
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'records_processed': results,
                'time_elapsed_sec': round(time.time() - start_time, 2),
                'message': 'Successfully processed all records from both datasets'
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'time_elapsed_sec': round(time.time() - start_time, 2),
                'partial_results': results
            })
        }
