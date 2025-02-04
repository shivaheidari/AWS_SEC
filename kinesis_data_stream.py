
import boto3
import json
import time
from datetime import datetime

class KinesisDataStream:

    def __init__(self, access_key, secret, region):
       
       self.session = boto3.Session(aws_access_key_id = access_key,
       aws_secret_access_key = secret,
       region_name = region)
       self.kinesis_client = boto3.client("kinesis", region_name="ca-central-1")

    def get_list_streams(self):
        response = self.kinesis_client.list_streams()
        return response
    
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


    def send_tweets_from_file(self, stream_name, file_path, batch_size=5):
        """Reads tweets from a file and sends them to Kinesis in batches."""
        try:
            with open(file_path, "r") as file:
                batch = []
                for line in file:
                    tweet_data = json.loads(line.strip())  # Read each line
                    batch.append({
                        'Data': json.dumps(tweet_data),
                        'PartitionKey': 'partition-1'
                    })

                    # Send batch when batch size is met
                    if len(batch) >= batch_size:
                        self.kinesis_client.put_records(StreamName=stream_name, Records=batch)
                        print(f"Sent {len(batch)} tweets to Kinesis.")
                        batch = []  # Reset batch
                        time.sleep(2)  # Prevent AWS throttling

                # Send remaining tweets
                if batch:
                    self.kinesis_client.put_records(StreamName=stream_name, Records=batch)
                    print(f"Sent {len(batch)} remaining tweets to Kinesis.")

        except Exception as e:
            print(f"Error sending tweets: {e}")



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

       