import pandas as pd
import boto3
import json

s3 = boto3.client('s3')
sns_client = boto3.client('sns')
sns_arn = 'arn:aws:sns:ap-south-1:112207118291:doordash-data-processing-notification' #update this

def lambda_handler(event, context):
    try:
	
	    # Define the source bucket name and file name
        source_bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        source_file_name = event["Records"][0]["s3"]["object"]["key"]
		
		# Download the JSON file from the source bucket
        response = s3.get_object(Bucket=source_bucket_name, Key=source_file_name)
        json_data = response['Body'].read().decode('utf-8')
        
        # Convert the JSON data to a Pandas DataFrame
        df = pd.read_json(json_data)
        
        # Filter the records based on certain criteria
        filtered_df = df[df['status'] == 'delivered']
        
        # Convert the filtered DataFrame to JSON string
        json_string = filtered_df.to_json(orient='records')
        
        # Define the destination bucket name and file name in S3
        destination_bucket_name = 'doordash-target-zn'
        destination_file_name = 'delivered_records.json'
        
        # Upload the JSON string to the destination bucket
        s3.put_object(Bucket=destination_bucket_name, Key=destination_file_name, Body=json_string)
        message = "Filtered records have been written to {}/{}".format("s3://"+source_bucket_name+"/"+source_file_name)
		
        respone = sns_client.publish(Subject="SUCCESS - Daily Data Processing",TargetArn=sns_arn, Message=message, MessageStructure='text')
		
    except Exception as err:
        print(err)
        message = "Input S3 File {} processing Failed !!".format("s3://"+source_bucket_name+"/"+source_file_name)
        respone = sns_client.publish(Subject="FAILED - Daily Data Processing", TargetArn=sns_arn, Message=message, MessageStructure='text')