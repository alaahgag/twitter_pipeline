import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs

def run_twitter_etl():

    access_key= "CtJdkjY5OcUVEA3dkjTo1hHxE"
    access_secret= "xNly7VjRsk4IBuGpwrTT8ua0LCTM6uGC0VxdMPfz9WRN8pdjg4"
    consumer_key= "1248241054459088896-O4LM7dPa6Eb0DEXHY3i1XsT31V3GJO"
    consumer_secret= "RPU2ofSIzam9MEewVvQAOlONxLbX2M7x19PY4iEObBKPx"


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