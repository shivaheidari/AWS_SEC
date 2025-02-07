import boto3
import json
import requests

# AWS Clients
s3 = boto3.client("s3")
kinesis_client = boto3.client("kinesis")

# Emotion Detection API (Replace with your actual API)
EMOTION_API_URL = "https://your-emotion-api.com/predict"

def lambda_handler(event, context):
    for record in event["Records"]:
        tweet_data = json.loads(record["kinesis"]["data"])
        tweet_text = tweet_data["text"]

        # Call Emotion API
        response = requests.post(EMOTION_API_URL, json={"text": tweet_text})
        emotion = response.json()["emotion"]

        # Append Emotion Tag
        tweet_data["emotion"] = emotion

        # Save to S3 in JSON format
        s3.put_object(
            Bucket="your-twitter-emotion-bucket",
            Key=f"tweets/{tweet_data['id']}.json",
            Body=json.dumps(tweet_data),
        )

    return {"statusCode": 200, "body": "Processed successfully"}


