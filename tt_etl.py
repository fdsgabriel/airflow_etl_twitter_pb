import tweepy as tp
import pandas as pd
import json
from datetime import datetime
import numpy as np
from pandasql import sqldf
import s3fs


def run_twitter_etl():
    
    #API Key
    access_key = YOUR_KEY

    #API Secret Key
    access_secret = YOUR_KEY
    
    #Access Token
    consumer_key = YOUR_KEY

    #Access Token Secret
    consumer_secret = YOUR_KEY

    #Creating connection
    #Authentication
    auth =  tp.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)

    #Object
    api = tp.API(auth)

    #Get info from medias
    all_tweets = []
    for user in ['@folha','@estadao','@uol','@jornalextra','@valoreconomico']:
        tweets = api.user_timeline(screen_name=user,
                                    count=100,
                                    include_rts = False,
                                    tweet_mode = 'extended'

                                    )
        all_tweets.append(tweets)

    tweet_list = []
    for i in range(len(all_tweets)):
        for each_json in all_tweets[i]:
            text = each_json._json["full_text"]
            
            refined_text = {
                            "user":each_json.user.screen_name,
                            "text":text,
                            "favorite_count":each_json.favorite_count,
                            "retweet_count":each_json.retweet_count,
                            "created_at":each_json.created_at,
                            "url":"https://twitter.com/"+each_json.user.screen_name+"/status/"+str(each_json.id)
                            }

            tweet_list.append(refined_text)

    df = pd.DataFrame(tweet_list)

    #Convert to srttime
    df['created_at'] = df['created_at'].apply(lambda x: x.strftime('%Y-%m-%d'))

    #Today value
    today_is = datetime.today().strftime('%Y-%m-%d')

    #Add is_today column
    df.loc[df.created_at == today_is, 'Is_today'] = 'Yes'
    df.Is_today.fillna("No", inplace=True)

    #Get week number and add a new column
    df['week_number'] = pd.to_datetime(df['created_at']).dt.week

    #This week value
    week_is = datetime.today().isocalendar()[1]

    #Add is_this_week column
    df.loc[df.week_number == week_is, 'Is_week'] = 'Yes'
    df.Is_week.fillna("No", inplace=True)

    #Set full text dataframe
    pd.set_option('display.max_colwidth', None)

    #Save in S3
    df.to_csv("s3://tt-airflow-gfds/tt_brazil_midia.csv")

