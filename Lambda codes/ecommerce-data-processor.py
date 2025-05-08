import json
import base64
import csv
import io
import boto3
from datetime import datetime

# Configuration
BUCKET_NAME = 'multi-source-data-lake'
PROCESSED_PREFIX = 'processed/ecommerce/'
s3 = boto3.client('s3')

def calculate_customer_metrics(record):
    """Calculate business metrics for ecommerce records"""
    try:
        print(f"Input record to calculate metrics: {record}")
        
        # Try multiple possible field names for Purchase_Amount
        purchase_amount_str = str(record.get('Purchase_Amount', 
                                          record.get('purchase_amount', 
                                                   record.get('PurchaseAmount', '0')))).replace('$', '').strip()
        try:
            purchase_amount = float(purchase_amount_str)
        except ValueError:
            print(f"Invalid Purchase_Amount format: {purchase_amount_str}")
            raise ValueError(f"Invalid Purchase_Amount format: {purchase_amount_str}")
        
        # Try multiple possible field names for Frequency_of_Purchase
        purchase_freq_str = str(record.get('Frequency_of_Purchase', 
                                        record.get('frequency_of_purchase', 
                                                 record.get('FrequencyOfPurchase', '0')))).strip()
        try:
            purchase_freq = int(purchase_freq_str)
        except ValueError:
            print(f"Invalid Frequency_of_Purchase format: {purchase_freq_str}")
            raise ValueError(f"Invalid Frequency_of_Purchase format: {purchase_freq_str}")
        
        # Try multiple possible field names for Brand_Loyalty
        brand_loyalty_str = str(record.get('Brand_Loyalty', 
                                         record.get('brand_loyalty', 
                                                  record.get('BrandLoyalty', '0')))).strip()
        try:
            brand_loyalty = int(brand_loyalty_str)
        except ValueError:
            print(f"Invalid Brand_Loyalty format: {brand_loyalty_str}")
            raise ValueError(f"Invalid Brand_Loyalty format: {brand_loyalty_str}")
        
        # Try multiple possible field names for Customer_Satisfaction
        satisfaction_str = str(record.get('Customer_Satisfaction', 
                                       record.get('customer_satisfaction', 
                                                record.get('CustomerSatisfaction', '0')))).strip()
        try:
            satisfaction = int(satisfaction_str)
        except ValueError:
            print(f"Invalid Customer_Satisfaction format: {satisfaction_str}")
            raise ValueError(f"Invalid Customer_Satisfaction format: {satisfaction_str}")
        
        # Calculate customer value score
        customer_value_score = (0.4 * purchase_amount / 1000) + \
                              (0.3 * purchase_freq / 10) + \
                              (0.15 * brand_loyalty / 10) + \
                              (0.15 * satisfaction / 10)
        
        # Determine customer segment
        customer_segment = 'HIGH_VALUE' if customer_value_score > 0.6 else \
                        'MEDIUM_VALUE' if customer_value_score > 0.4 else 'STANDARD'
        
        print(f"Calculated metrics: customer_value_score={customer_value_score}, customer_segment={customer_segment}")
        return {
            'customer_value_score': round(customer_value_score, 2),
            'customer_segment': customer_segment,
            'processing_timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        print(f"Error calculating metrics for record {record}: {str(e)}")
        return {
            'customer_value_score': None,
            'customer_segment': 'ERROR',
            'processing_timestamp': datetime.now().isoformat()
        }

def process_record(record):
    """Process a single Kinesis record"""
    try:
        # Log the raw Kinesis record
        print(f"Raw Kinesis record: {record}")
        
        # Decode base64 Kinesis data
        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        print(f"Decoded payload: {payload}")
        
        # Parse JSON
        data = json.loads(payload)
        print(f"Parsed JSON data: {data}")
        
        # Add calculated metrics
        metrics = calculate_customer_metrics(data)
        data.update(metrics)
        
        return data
    except Exception as e:
        print(f"Error processing record: {str(e)}")
        return None

def save_to_s3(records):
    """Save processed records to S3 as CSV"""
    if not records:
        return None
    
    try:
        # Generate CSV in memory
        csv_buffer = io.StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
        
        # Create S3 path with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_key = f"{PROCESSED_PREFIX}ecommerce_{timestamp}.csv"
        
        # Upload to S3
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=file_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        return file_key
    except Exception as e:
        print(f"Error saving to S3: {str(e)}")
        raise

def lambda_handler(event, context):
    print(f"Processing {len(event.get('Records', []))} records")
    
    processed_records = []
    for record in event.get('Records', []):
        processed = process_record(record)
        if processed:
            processed_records.append(processed)
    
    if processed_records:
        try:
            output_path = save_to_s3(processed_records)
            return {
                'statusCode': 200,
                'body': {
                    'message': 'Successfully processed records',
                    'processed_count': len(processed_records),
                    'output_location': f"s3://{BUCKET_NAME}/{output_path}"
                }
            }
        except Exception as e:
            return {
                'statusCode': 500,
                'body': {
                    'error': str(e),
                    'failed_records': len(event['Records']) - len(processed_records)
                }
            }
    else:
        return {
            'statusCode': 400,
            'body': {
                'error': 'No valid records processed',
                'total_records': len(event.get('Records', []))
            }
        }
