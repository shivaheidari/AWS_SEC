from twitter_client import TwitterClient 
from kinesis_data_stream import KinesisDataStream as kds




#git ignore file
#add keyds here

client = TwitterClient(BEARER_TOKEN)

tweets = client.get_real_time_tweets("Samsung", 10)


ds_client = kds(aws_access_key_id,aws_secret_access_key, region_name)

streams = ds_client.get_list_streams()

for stream in streams:
    print(stream)

#ds_client.create_stream("twitter-stream-fetch")

#tweets = ds_client.send_tweets_from_file("twitter-stream-fetch", "deep seek_tweets.json")

ds_client.send(tweets, "twitter-stream-fetch")


