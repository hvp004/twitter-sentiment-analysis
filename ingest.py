import twitter
import configparser
import requests
import json
import datetime
import pandas as pd
from io import StringIO
import boto3
import time

class Ingest:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        print(kwargs.keys())

    def search_tweets_api(self, start_time, end_time, filename):
        kwargs = self.kwargs

        # session for aws assests
        session = boto3.Session( **kwargs['aws'])

        twitter_kwargs = kwargs['tweet-search']
        storage_kwargs = kwargs['storage']
        kinesis_kwargs = kwargs['kinesis']


        # keep count od tweets read
        tweets = 0

        # make headers
        headers = {"Authorization": "Bearer {}".format(kwargs['twitter-api']['bearer_token'])}

        # build search url
        url = "{}?query={}&{}&{}&expansions=author_id,referenced_tweets.id&max_results=100&start_time={}&end_time={}".format(
            twitter_kwargs['search_url'], twitter_kwargs['query'], twitter_kwargs['tweets_fields'], twitter_kwargs['user_fields'], start_time, end_time
        )

        # fetch response
        response = requests.request("GET", url, headers=headers)


        if response.status_code != 200: 
            # unsuccesfull response, raise exception
            raise Exception(response.status_code, response.text)

        # print(response.json())
        
        # process reponse packet
        tweet = response.json()

        # print(pd.DataFrame(tweet['data']).rename(columns={'id': 'tweet_id'}).columns, pd.DataFrame(tweet['includes']['users']).rename(columns={'id': 'user_id'}).columns)

        # print(pd.DataFrame(tweet['data']).rename(columns={'id': 'tweet_id'}).head(5))

        # join tweets with user information
        # sometimes twitter gives `withheld` as a field arbitarily
        # hence selecting fields of interest explicitly 
        data = pd.DataFrame(tweet['data']).rename(columns={'id': 'tweet_id'})[['tweet_id', 'created_at', 'author_id', 'text', 'referenced_tweets']].join(
            pd.DataFrame(tweet['includes']['users']).rename(columns={'id': 'user_id'})[['name', 'user_id', 'username', 'verified', 'location']])

        # rewteets, replies and quoted tweets comes out to be trunctcated under text field, have to fetch from ['includes']['tweets'] based on 'referenced_tweets' and 'id' 

        # replace referenced_tweets subjson with id strings
        mask = ~data['referenced_tweets'].isna()
        data.loc[mask, 'referenced_tweets'] = data.loc[mask]['referenced_tweets'].apply(lambda x: x[0]['id'])

        # make a dedicated dataframe for reference tweets
        ref_tweets = pd.DataFrame(tweet['includes']['tweets']).rename(columns={'text': 'ref_text'})

        # merge data with ref_tweets to get full text
        merged_data = data.merge(ref_tweets[['id', 'ref_text']], left_on='referenced_tweets', right_on='id', how='left')
        mask = ~merged_data['referenced_tweets'].isna()
        merged_data.loc[mask, 'text'] = merged_data.loc[mask, 'ref_text']

        # 'created_at' is a timestamp field
        merged_data['created_at'] = pd.to_datetime(merged_data['created_at'])

        # make it a human readable string since pd.to_json defaults it to ms 
        merged_data['created_at'] = merged_data['created_at'].dt.strftime('%Y/%m/%d-%H:%M:%S')

        # if one tweet has been retweeded multiple times, keep oldest
        merged_data = merged_data.drop_duplicates('referenced_tweets', keep='first')

        # drop unwanted columns
        merged_data = merged_data.drop(['id', 'author_id', 'ref_text'], 1)

        # check shape (tweets, 9)
        # print(merged_data.shape)

        # remove emojis and `"` from text
        merged_data['text'] = merged_data['text'].apply(lambda x: ''.join([" " if ord(i) < 32 or ord(i) > 126 or i == '"' else i for i in str(x)]))

        # number of tweets read
        tweets = merged_data.shape[0]

        # store the data
        if storage_kwargs['mode'] == 'local':
            filepath = '{}/r{}.json'.format(storage_kwargs['staging_local_dir'], filename)
            merged_data.to_json(filepath, orient='records')
        else:
            bucket = storage_kwargs['s3_bucket']
            key = storage_kwargs['staging_s3_key']
            buffer = StringIO()
            merged_data.to_json(buffer, orient='records')
            s3_resource = session.resource('s3')
            s3_object = s3_resource.Object(bucket, '{}/{}.json'.format(key, filename))
            s3_object.put(Body=buffer.getvalue())

        # pass (twitter_id, text) to kinesis for further processing
        kinesis = session.client('kinesis')
        kinesis.put_record(StreamName=kinesis_kwargs['streamname'], Data=merged_data[['tweet_id', 'text']].to_json(orient='records'), PartitionKey='key_tweet')
        
        print(start_time, tweets) 
        return tweets
        

    def search_tweets(self):
        kwargs = self.kwargs
        twitter_kwargs = kwargs['tweet-search']
        start_times = pd.date_range(start=twitter_kwargs['start_time'], end=twitter_kwargs['end_time'], freq=twitter_kwargs['freq'])
        end_times = start_times + pd.Timedelta(twitter_kwargs['freq'])

        # total tweets read for timespan
        count = 0 
        for i in range(len(start_times)):
            start_time = start_times[i].isoformat("T") + "Z"
            end_time = end_times[i].isoformat("T") + "Z"
            count += self.search_tweets_api(start_time, end_time, start_times[i].strftime('%Y%m%d%H%M'))

        print("total tweets: ", count)
            

if __name__ == "__main__":

    # read config and ini files
    config = configparser.ConfigParser(interpolation=None)
    config.read(['./config/secret.ini', './config/app.yaml'])
    

    # initiate ingest object
    ingest = Ingest(**config)

    # start ingesting to storage
    ingest.search_tweets()
