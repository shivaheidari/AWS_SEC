import boto3
import json
from datetime import datetime
import tweepy


# Initialize S3 client
class TwitterClient:
    def __init__(self, bearer_token):
        self.client = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)
    
    def get_real_time_tweets(self, tag, limit=10):

        query = f"{tag} -is:retweet lang:en"
        tweets = self.client.search_recent_tweets(query=query, max_results=limit)
        if tweets.data:
            return [tweet.text for tweet in tweets.data]
        return []


    

class KinesisDataStream:

    def __init__(self, access_key, secret, region):
       
       self.session = boto3.Session(aws_access_key_id = access_key,
       aws_secret_access_key = secret,
       region_name = region)
       self.kinesis_client = boto3.client("kinesis", region_name="us-east-1")

    def create_stream(self, stream_name, shard_count=1):
        try:
            response = self.kinesis_client.create_stream(
            StreamName=stream_name,
            ShardCount=1)
            print(f"Kinesis stream '{stream_name}' created successfully!")
        except Exception as e:
           print(f"Error creating stream:{e}")


    def send(self, data, stream_name):


        try:
            json_data = json.dumps({"tweet": data})
            response = self.kinesis_client.put_record(
                StreamName=stream_name,
                Data=json_data,
                PartitionKey="partition-1"
            )
            print(f"Tweet sent to Kinesis: {data}")
        
        except Exception as e:
            print(f"Error sending data to Kinesis: {e}")



    def read_from_kinesis(self,stream_name):
        try:
                response = self.kinesis_client.describe_stream(StreamName=stream_name)
                shard_id = response["StreamDescription"]["Shards"][0]["ShardId"]

                iterator_response = self.kinesis_client.get_shard_iterator(
                    StreamName=stream_name,
                    ShardId=shard_id,
                    ShardIteratorType="LATEST"
                )
                shard_iterator = iterator_response["ShardIterator"]


                while True:
                        records_response = self.kinesis_client.get_records(ShardIterator=shard_iterator, Limit=10)
                        shard_iterator = records_response.get("NextShardIterator", None)

                        for record in records_response["Records"]:
                            tweet_data = json.loads(record["Data"])
                            print(f"Received tweet: {tweet_data['tweet']}")

                        if not shard_iterator:
                            break  # Exit loop if there are no more records
        except Exception as e:
            print(f"Error reading from Kinesis: {e}")

       