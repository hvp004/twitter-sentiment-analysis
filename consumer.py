import boto3
import time
import pandas as pd
import json
import configparser
from io import StringIO

class Consumer:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        print(kwargs)

    def start_consuming(self):
        kwargs = self.kwargs

        storage_kwargs = kwargs['storage']

        # session for aws assests
        session = boto3.Session( **kwargs['aws'])

        # get kinesis and comprehend aws clients
        kinesis = session.client('kinesis')
        self.comprehend = session.client('comprehend')

        # obtain shard iterator
        shard_iterator = kinesis.get_shard_iterator(StreamName='tweet_stream', ShardId='shardId-000000000000', ShardIteratorType='LATEST')['ShardIterator']

        # get reading to start listening
        record_response = kinesis.get_records(ShardIterator=shard_iterator, Limit=1)

        # start listening
        while 'NextShardIterator' in record_response:

            # get latest records from kinesis
            shard_iterator = record_response['NextShardIterator']
            record_response = kinesis.get_records(ShardIterator=shard_iterator)

            # filename for the json file
            filename = record_response['ResponseMetadata']['RequestId']

            # concate all batches that we have had since our last read
            if record_response['Records']:
                data = pd.concat([pd.DataFrame(json.loads(record['Data'])) for record in record_response['Records']]).reset_index(drop=True)

                # detect sentiment from text and store sentiment scores and label as columns
                data = data.join(data['text'].apply(lambda t: self.comprehend_tweet(t, True)).apply(pd.Series)).rename(columns=dict(zip(range(5), ['mixed', 'negative', 'neutral', 'positive', 'label'])))
                print(data)

                # store each item except text since we already have it in staging
                if storage_kwargs['mode'] == 'local':
                    filepath = '{}/r{}.json'.format(storage_kwargs['sentiment_local_dir'], filename)
                    data.to_json(filepath, orient='records')
                else:
                    bucket = storage_kwargs['s3_bucket']
                    key = storage_kwargs['sentiment_s3_key']
                    buffer = StringIO()
                    data.drop('text', 1).to_json(buffer, orient='records')
                    s3_resource = session.resource('s3')
                    s3_object = s3_resource.Object(bucket, '{}/{}.json'.format(key, filename))
                    s3_object.put(Body=buffer.getvalue())


            else:
                # no updates since last read
                print("No Update.")

            time.sleep(10)

    def comprehend_tweet(self, text, debug=False):
        if not debug:
            response = self.comprehend.detect_sentiment(Text=text, LanguageCode='en')
            return [round(float(response['SentimentScore'][x]), 2)  for x in ['Mixed', 'Negative', 'Neutral', 'Positive']] + [response['Sentiment']]
        return [0.25, 0.25, 0.25, 0.25, 'DEBUG']


if __name__ == "__main__":

    # read config and ini files
    config = configparser.ConfigParser(interpolation=None)
    config.read(['./config/secret.ini', './config/app.yaml'])
    
    # initiate ingest object
    consumer = Consumer(**config)

    # start ingesting to storage
    consumer.start_consuming()