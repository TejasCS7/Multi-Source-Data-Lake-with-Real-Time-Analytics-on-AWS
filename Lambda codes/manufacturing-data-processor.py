import json
import base64
import boto3
import time
import uuid
import datetime
import csv
import io

s3 = boto3.client('s3')
kinesis = boto3.client('kinesis')

BUCKET_NAME = 'multi-source-data-lake'
MANUFACTURING_PROCESSED_PREFIX = 'processed/manufacturing/'

def dict_to_csv_string(data_list):
    """Convert list of dictionaries to CSV string"""
    if not data_list:
        return ""
    
    fieldnames = set()
    for record in data_list:
        fieldnames.update(record.keys())
    fieldnames = sorted(fieldnames)
    
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(data_list)
    
    return output.getvalue()

def process_record(record):
    """Process individual record with error handling"""
    try:
        # Handle both direct invocation and Kinesis wrapped records
        if 'kinesis' in record and 'data' in record['kinesis']:
            payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        else:
            payload = json.dumps(record)  # For direct invocation
            
        data = json.loads(payload)
        
        # Add processing metadata
        data['processing_timestamp'] = datetime.datetime.now().isoformat()
        data['batch_id'] = str(uuid.uuid4())
        
        # Business logic for manufacturing data
        if all(k in data for k in ['temperature', 'vibration', 'anomaly_flag']):
            temp = float(data.get('temperature', 0))
            vibration = float(data.get('vibration', 0))
            anomaly = int(data.get('anomaly_flag', 0))
            downtime_risk = float(data.get('downtime_risk', 0))
            
            maintenance_priority = (
                (0.3 * temp/100) + 
                (0.25 * vibration/10) + 
                (0.15 * anomaly) + 
                (0.3 * downtime_risk)
            )
            
            data['maintenance_priority_score'] = round(maintenance_priority, 2)
            
            if maintenance_priority > 0.7:
                return data, {
                    'machine_id': data.get('machine_id'),
                    'alert_type': 'HIGH_MAINTENANCE_PRIORITY',
                    'score': maintenance_priority,
                    'timestamp': data.get('timestamp'),
                    'recommended_action': 'Schedule immediate maintenance'
                }
        
        return data, None
    
    except Exception as e:
        print(f"Error processing record: {str(e)}")
        return None, None

def lambda_handler(event, context):
    processed_records = []
    alerts = []
    
    # Handle different event types
    if 'Records' in event:  # Kinesis/SQS trigger
        records_to_process = event['Records']
    else:  # Direct invocation
        records_to_process = [event]
    
    for record in records_to_process:
        processed_data, alert = process_record(record)
        if processed_data:
            processed_records.append(processed_data)
        if alert:
            alerts.append(alert)
    
    # Batch write to S3
    if processed_records:
        timestamp = int(time.time())
        file_key = f"{MANUFACTURING_PROCESSED_PREFIX}batch_{timestamp}.csv"
        
        try:
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=file_key,
                Body=dict_to_csv_string(processed_records),
                ContentType='text/csv'
            )
            print(f"Processed {len(processed_records)} records to {file_key}")
        except Exception as e:
            print(f"Failed to write to S3: {str(e)}")
            raise
    
    # Store alerts
    if alerts:
        alert_key = f"{MANUFACTURING_PROCESSED_PREFIX}alerts/alerts_{timestamp}.json"
        try:
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=alert_key,
                Body=json.dumps(alerts),
                ContentType='application/json'
            )
            print(f"Generated {len(alerts)} alerts")
        except Exception as e:
            print(f"Failed to write alerts: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': {
            'processed_count': len(processed_records),
            'alert_count': len(alerts),
            's3_location': f"s3://{BUCKET_NAME}/{file_key}" if processed_records else None
        }
    }
