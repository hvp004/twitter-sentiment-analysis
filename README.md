# Twitter Sentiment Analysis

### System Architecture

The project runs on AWS exposing some AWS services:

1. S3: For storage of staging data, sentiment data, tweet transactional data, aggregated data

2. EC2: To run `ingest.py` and `consumer.py`. You can also run them on your own laptop. This is optional. 

3. EMR Cluster: To process all data produced in batches. Merge data and run aggregation before storing them back to S3.

4. Redshift Cluster: Ultimately data would end up in the Redshift. 

5. AWS Comprehend: In order to get sentiment from tweet text data, AWS Comprehend is used. This can be replaced with local NLP model or an open source or enterprise version ML model with HTTP end point. Comprehend is used through `boto3` python API here. 

   ![System Architecture](./docs/architecture.png)



## Steps:

1. Run consumer.py: It will not show any output yet since we have not started fetching tweets from twitter api yet. 

2. Run ingest.py: Now we have started ingesting the tweets from `start_time` to `end_time` inclusive. You can see then in real time with 5sec delay on consumer.py 

   After ingesting stops, data is stored in either s3 bucket or local directory depending on the `[storage][mode]` specified in the `app.yaml` file. `staging` and  `sentiment` directories would hold json documents with raw tweets and their metadata and sentiments comprehended from AWS Comprehend NLP service.

3. EMR: ssh into the emr cluster with putty using `hadoop@<EMR-CLUSTER-DNS>` on port 22 using private key and copy `emr-script.py` from s3 bucket to emr master cluster instance. 

   Use `aws s3 cp s3://<S3-BUCKET-WITH-emr-script.py>/emr-script.py .`

4. `spark-submit`: Now we are ready to submit this task to the spark cluster. 

   Use `spark-submit ./emr-script.py`	

   `emr-script.py` provides two csv files into the specified buckets. 

   1. File containing entire dataset with all tweets and their individual results. 
   2. File containing aggregated results with 15 mins time window containing average sentiment score and percentage weight of each sentiment with total number of absolute tweets for that 15 mins window. 

5. Redshift: Now, we are ready to load the processed data into the AWS Redshift datawarehouse. 

   For this, you need to create two tables. 
   
   1. `tweet`: A table to have a historical transactional data for all tweets and their results.
   2. `sentiment`: Aggregated data for every 15 minute time window with avg scores, percentage number of tweets and total tweets in that timeframe. 
   
   Use `create-table.sql` and `create-sentiment.sql` from `redshift` to create these tables. 
   
   Now, it is time to fill data in these tables. 
   
   Use COPY command from AWS to load data from S3 into AWS Redshift Cluster.  Use `copy-tweet` and `copy-sentiment` to fill up `tweet` and `sentiment` tables into redshift. COPY commands are also in `redshift` directory of the project. 