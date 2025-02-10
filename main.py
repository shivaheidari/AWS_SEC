from twitter_client import TwitterClient 
from kinesis_data_stream import KinesisDataStream as kds




client = TwitterClient(BEARER_TOKEN)




ds_client = kds(aws_access_key_id,aws_secret_access_key, region_name)

# tweets = client.get_real_time_tweets("garden", 10)
# ds_client.send(tweets, "tweeter_stream")


#streams = ds_client.get_list_streams()

#print("Streams:", streams.get("StreamNames", []))

#ds_client.create_stream("twitter-stream-fetch")

#tweets = ds_client.send_tweets_from_file("tweeter_stream", "deep seek_tweets.json")
#ds_client.read_from_kinesis("tweeter_stream")





