import boto3
import time
import os
import json

# Initialize the AWS Glue client
glue = boto3.client('glue')

# --- ENVIRONMENT VARIABLES (Highly Recommended) ---
# Define Glue resource names here, or set them as Environment Variables in the Lambda console
CRAWLER_NAME = "staging-food"      # Use the exact name of your Crawler
GLUE_JOB_NAME = "Si" # Use the exact name of your Glue Job

def lambda_handler(event, context):
    print("--- Pipeline Triggered by S3 Event ---")

    # 1. Start the Glue Crawler
    print(f"Starting Glue Crawler: {CRAWLER_NAME}")
    try:
        glue.start_crawler(Name=CRAWLER_NAME)
    except Exception as e:
        # Ignore if crawler is already running
        if "Crawler is already running" in str(e):
            print("Crawler is already running. Waiting for it to finish.")
        else:
            print(f"Error starting crawler: {e}")
            raise e
    
    # 2. Wait for the Crawler to complete (Crucial Step!)
    print("Waiting for crawler to complete...")
    max_wait_time = 300  # 5 minutes max wait time
    start_time = time.time()
    
    while True:
        if time.time() - start_time > max_wait_time:
            raise TimeoutError("Crawler wait time exceeded 5 minutes. Pipeline aborted.")

        # Check the crawler status
        status = glue.get_crawler(Name=CRAWLER_NAME)['Crawler']['State']

        if status == 'READY':
            # Check if the last run was successful
            last_run = glue.get_crawler(Name=CRAWLER_NAME)['Crawler']['LastCrawl']['Status']
            if last_run == 'SUCCEEDED':
                 print("Crawler finished successfully.")
                 break
            elif last_run == 'FAILED':
                 raise Exception("Crawler failed during execution. Check Glue logs.")
        
        print(f"Current crawler status: {status}. Sleeping 10s...")
        time.sleep(10) # Wait 10 seconds before checking again

    # 3. Start the Main Glue ETL Job (Raw -> Silver)
    print(f"Crawler finished. Starting Glue Job: {GLUE_JOB_NAME}")
    try:
        response = glue.start_job_run(JobName=GLUE_JOB_NAME)
        return {
            'statusCode': 200,
            'body': json.dumps({'message': f'Glue pipeline initiated. JobRunId: {response["JobRunId"]}'})
        }
    except Exception as e:
        print(f"Error starting Glue Job: {e}")
        raise e
