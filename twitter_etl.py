import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs

def run_twitter_etl():

    access_key = "YOUR_ACCESS_KEY"
    access_secret = "YOUR_ACCESS_SECRET"
    consumer_key = "YOUR_CONSUMER_KEY"
    consumer_secret = "YOUR_CONSUMER_SECRET"


    # Twitter Authentication

    auth= tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key,consumer_secret)

    #creating an API object

    api= tweepy.API(auth)

    tweets = api.user_timeline(screen_name='@3laa7gag',
                            count=200,
                            include_rts=False,
                            tweet_mode='extended')


    tweets_list= []

    for tweet in tweets:
        text = tweet.full_text

        refined_tweet= {"user" : tweet.user.screen_name,
                        "text" : text,
                        "favorite_count" : tweet.favorite_count,
                        "retweet_count" : tweet.retweet_count,
                        "created_at" : tweet.created_at}
        
        tweets_list.append(refined_tweet)

    df = pd.DataFrame(tweets_list)

    df.to_csv("s3://twitter-airflow-pipeline-results/my_timeline_tweets.csv")
