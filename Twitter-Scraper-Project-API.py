# lambda_function.py
import json
import boto3
from botocore.vendored import requests
import API_Functions
import pandas as pd
import os

def lambda_handler(event, context):
    ticker = event['pathParameters']['symbol'] 
    StartDate = event['pathParameters']['startdate']
    EndDate = event['pathParameters']['enddate']
   
    Window_Sentiment = API_Functions.GetSentimentWindow(ticker=ticker, StartDate=StartDate, EndDate=EndDate)
    Window_Tweets = API_Functions.GetTweetsWindow(ticker=ticker, StartDate=StartDate, EndDate=EndDate)
    
    return {
        "sentiment_score": Window_Sentiment,
        "tweets": Window_Tweets
    }


# API_Functions.py

import boto3
from botocore.exceptions import ClientError
import pandas as pd
from datetime import datetime
import math
import json

bucket = "my-bucket-name"

def GetSentimentWindow(ticker: str, StartDate: str, EndDate: str):
    sentiment_list = []
    datelist = pd.date_range(start=StartDate,end=EndDate).strftime('%Y-%m-%d').tolist()
    for date in datelist:
        Daily_Sentiment = GetCompanySentiment(Ticker=ticker, Date=date)
        sentiment_list.append(Daily_Sentiment)
    sentiment_df = pd.DataFrame(sentiment_list)
    sentiment_df.columns = ['sentiment']
    SL_dropzero = sentiment_df[sentiment_df['sentiment'] != 2].mean()
    
    return json.loads(json.dumps(SL_dropzero['sentiment'], default=str))
    
def GetTweetsWindow(ticker: str, StartDate: str, EndDate: str):
    tweets_df_window = pd.DataFrame()
    datelist = pd.date_range(start=StartDate,end=EndDate).strftime('%Y-%m-%d').tolist()
    for date in datelist:
        Daily_Tweets = GetCompanyTweets(Ticker=ticker, Date=date)
        tweets_df_window = pd.concat([tweets_df_window, Daily_Tweets], axis=0)
    tweets_df_window.columns = ['id', 'datetime', 'text', 'label', 'url']
    tweets_df_window = tweets_df_window.to_dict(orient='records')
    
    return tweets_df_window


def GetCompanySentiment(Date: str, Ticker: str):
    #download csv from s3
    key = 'Twitter/' + Date[0:4] + '/tweets_' + Date + '.csv'
    
    s3 = boto3.client('s3')
    s3_obj = s3.get_object(Bucket=bucket, Key=key)

    #print('Data for this day are not available')
    s3_data = s3_obj['Body']
    tweets_csv = pd.read_csv(s3_data, sep='\t')
    tweets_csv = tweets_csv.dropna(subset = ['Ticker'])
    data_for_company = tweets_csv[tweets_csv['Ticker'] == Ticker]
    daily_mean_sentiment = data_for_company.Label.mean()
    if math.isnan(daily_mean_sentiment):
        return 2
    
    return daily_mean_sentiment
    
    
def GetCompanyTweets(Date: str, Ticker: str):
    #download csv from s3
    key = 'Twitter/' + Date[0:4] + '/tweets_' + Date + '.csv'

    s3 = boto3.client('s3')
    try:
        s3_obj = s3.get_object(Bucket=bucket, Key=key)
    except:
        print('Data for this day are not available')
        return []
    else:
        s3_data = s3_obj['Body']
        tweets_csv = pd.read_csv(s3_data, sep='\t')
        
        tweets_csv = tweets_csv.dropna(subset = ['Ticker'])
        data_for_company = tweets_csv[tweets_csv['Ticker'] == Ticker]
        text_display = data_for_company[['time', 'text_og','Label', 'ID']]#.tail(10)
        text_display = text_display.reset_index()
        text_display.columns = ['id', 'datetime', 'text','label', 'ID']
        text_display['url'] = text_display['ID'].apply(lambda x: f"https://twitter.com/twitter/status/{x}")
        text_display = text_display.drop(columns=['ID'])
        #text_display = text_display.to_dict(orient='records')
        return text_display