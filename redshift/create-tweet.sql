create table sentiment(
  time_window timestamp not null,
  count_tweets integer,
  avg_positive decimal(5,4),
  avg_neutral decimal(5,4),
  avg_mixed decimal(5,4),
  avg_negative decimal(5,4),
  perc_tweet_positive decimal(5,4),
  perc_tweet_neutral decimal(5,4),
  perc_tweet_negative decimal(5,4),
  perc_tweet_mixed decimal(5,4),
  
  CONSTRAINT sentiment_pkey PRIMARY KEY (time_window)
  
)