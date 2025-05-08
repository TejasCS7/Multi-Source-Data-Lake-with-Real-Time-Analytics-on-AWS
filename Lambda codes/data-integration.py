import json
import boto3
import time

s3 = boto3.client('s3')

BUCKET_NAME = 'multi-source-data-lake'
ANALYTICS_PREFIX = 'analytics/'
INTEGRATED_PREFIX = 'analytics/integrated/'

def lambda_handler(event, context):
    """
    Integrate manufacturing and ecommerce data for cross-domain analytics
    """
    try:
        # Read the latest manufacturing analytics summary
        manufacturing_summaries = s3.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=f"{ANALYTICS_PREFIX}manufacturing/summary/"
        )
        
        # Get the latest manufacturing summary
        latest_mfg_key = None
        latest_timestamp = 0
        
        if 'Contents' in manufacturing_summaries:
            for obj in manufacturing_summaries['Contents']:
                if obj['LastModified'].timestamp() > latest_timestamp:
                    latest_timestamp = obj['LastModified'].timestamp()
                    latest_mfg_key = obj['Key']
        
        # Read the latest ecommerce analytics summary
        ecommerce_summaries = s3.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=f"{ANALYTICS_PREFIX}ecommerce/summary/"
        )
        
        # Get the latest ecommerce summary
        latest_ecom_key = None
        latest_timestamp = 0
        
        if 'Contents' in ecommerce_summaries:
            for obj in ecommerce_summaries['Contents']:
                if obj['LastModified'].timestamp() > latest_timestamp:
                    latest_timestamp = obj['LastModified'].timestamp()
                    latest_ecom_key = obj['Key']
        
        # Load both summaries
        mfg_data = {}
        ecom_data = {}
        
        if latest_mfg_key:
            mfg_response = s3.get_object(Bucket=BUCKET_NAME, Key=latest_mfg_key)
            mfg_data = json.loads(mfg_response['Body'].read().decode('utf-8'))
        
        if latest_ecom_key:
            ecom_response = s3.get_object(Bucket=BUCKET_NAME, Key=latest_ecom_key)
            ecom_data = json.loads(ecom_response['Body'].read().decode('utf-8'))
        
        # Create integrated insights
        integrated_insights = {
            "timestamp": time.time(),
            "manufacturing_metrics": mfg_data,
            "ecommerce_metrics": ecom_data,
            # Create cross-domain insights
            "cross_domain_insights": {
                "operational_excellence_score": (
                    mfg_data.get("average_machine_efficiency", 0) * 0.6 + 
                    ecom_data.get("customer_satisfaction_index", 0) / 10 * 0.4
                ),
                "business_health_index": (
                    (1 - mfg_data.get("high_risk_machines", 0) / 
                     mfg_data.get("total_machines_analyzed", 100)) * 0.5 +
                    (ecom_data.get("high_value_customers", 0) / 
                     ecom_data.get("total_customers_analyzed", 1000)) * 0.5
                ),
                "data_quality_composite": (
                    mfg_data.get("data_quality_score", 0) * 0.5 +
                    ecom_data.get("data_quality_score", 0) * 0.5
                )
            }
        }
        
        # Store integrated insights
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{INTEGRATED_PREFIX}integrated_insights_{int(time.time())}.json",
            Body=json.dumps(integrated_insights),
            ContentType='application/json'
        )
        
        return {
            'statusCode': 200,
            'body': 'Data integration completed successfully'
        }
    
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': f'Error in data integration: {str(e)}'
        }
