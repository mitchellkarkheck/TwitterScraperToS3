#lambda_function.py

import json
import pandas as pd
import tweepy
import vaderSentiment
import configparser
import time
import nltk
nltk.data.path.append("/tmp")
nltk.download("punkt", download_dir="/tmp")
nltk.download("stopwords", download_dir="/tmp")
nltk.data.path.append("/tmp")
nltk.download("vader_lexicon", download_dir="/tmp")
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.sentiment.util import *
import re
import string
import datetime
import os
import Twitter_Functions
from io import StringIO
import boto3
from botocore.vendored import requests

today=datetime.date.today()
yesterday = today - datetime.timedelta(days = 1)
today = today.strftime('%Y-%m-%d')
yesterday = yesterday.strftime('%Y-%m-%d')

def lambda_handler(event, context):
   
    todays_tweets = Twitter_Functions.GetTheTweets(yesterday, yesterday + '.csv')
    
    return 'Done!Tweets sent to s3 bucket Bucketname/Twitter'


# Twitter_Function.py

import json
import pandas as pd
import tweepy
import vaderSentiment
import configparser
import time
import nltk
nltk.data.path.append("/tmp")
nltk.download("punkt", download_dir="/tmp")
nltk.download("stopwords", download_dir="/tmp")
nltk.data.path.append("/tmp")
nltk.download("vader_lexicon", download_dir="/tmp")
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.sentiment.util import *
import re
import string
import datetime
import os
from io import StringIO
import boto3
from botocore.vendored import requests


def GetTheTweets(Todays_date, Output_Path):
    # TSX stocks is a spreadsheet of 300 stocks from the TSX and S&P with names, twitter handles, and other info
    dfcompanylist = pd.read_csv('TSXstocks.csv')
    
    tweets_list=[]

    client = tweepy.Client(bearer_token=os.environ['BEARER_TOKEN_MITCH'])

    for j in dfcompanylist.Account.index:
        # Search query
        query = dfcompanylist.Account[j]+' -is:retweet lang:en'
        
        tweets = client.search_recent_tweets(query=query, tweet_fields =['created_at', 'referenced_tweets', 'source', 'lang', 'entities', 'public_metrics'], user_fields=['username', 'public_metrics'], expansions='author_id', max_results=100)

        # Get users list from the includes object
        if tweets.data is None: 
              continue
        users = {u["id"]: u for u in tweets.includes['users']}

        for tweet in tweets.data:
            if users[tweet.author_id]:
                user = users[tweet.author_id]
                tweets_list.append([tweet.created_at, tweet.lang, tweet.public_metrics['retweet_count'], tweet.public_metrics['like_count'], tweet.public_metrics['reply_count'], dfcompanylist.Account[j], user.username, user.public_metrics['followers_count'], tweet.entities.get('hashtags'),  dfcompanylist.Sector[j], dfcompanylist.Industry[j], dfcompanylist.Ticker[j], tweet.id, tweet.text])
    tweets_full_sample = pd.DataFrame(tweets_list)
    tweets_full_sample.columns = ['time','language', 'retweets', 'likes', 'replies', 'about', 'user', 'Followers', 'Data', 'Sector', 'Industry', 'Ticker', 'ID', 'text']

    tweets_combined = tweets_full_sample.drop_duplicates(subset=['text'])
    tweets_combined = tweets_combined.reset_index() 
    
    def extract_tags(tags_str):
        if (tags_str is None) or (tags_str == "") or (tags_str == "[]") or (tags_str == "NaN") or (tags_str == "nan") or (tags_str == "None"):
            return []
        return [x['tag'] for x in json.loads(tags_str.replace("'", '"')) if "tag" in x]

    def clean_data(text):
        #Use sumple regex statemnents to clean tweet text by removing links and special characters, and strip all non-ASCII characters to remove emoji characters.
        text = text.encode('ascii', 'ignore').decode('ascii') #remove emoji
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) \
                                  |(\w+:\/\/\S+)", " ", text).split())
    def tokenlize(text):
        #create tokens of words in text using nltk
        tokens = nltk.word_tokenize(text) 
        # convert to lower case
        tokens = [w.lower() for w in tokens] 
        # remove punctuation from each word
        table = str.maketrans('', '', string.punctuation)
        stripped = [w.translate(table) for w in tokens]
        # remove remaining tokens that are not alphabetic
        tokens = [w for w in stripped if w.isalpha()]
        # filter out stop words
        stop_words = set(nltk.corpus.stopwords.words('english'))
        tokens = [w for w in tokens if not w in stop_words]
        return tokens

    # combined tokens to string for vader
    def clean_all(data):
        df = data.copy()
        df['length'] = 0
        for row in range(len(df)):
          if isinstance(df.iloc[row]['text'], str):
            #df.at[row,'Headline'] = tokenlize(clean_data(df.loc[row,'Headline']))
            tokens = tokenlize(clean_data(df.loc[row,'text']))
            df.at[row,'text'] = ' '.join(tokens)
            df.loc[row,'length'] = len(tokens)
        data_1 = data.rename(columns={'text':'text_og'})
        df = pd.concat([df, data_1['text_og']], axis=1) 
        return df

    
    tweets_combined = clean_all(tweets_combined)
    
    #Sentiment Analysis
    
    SIA = SentimentIntensityAnalyzer()
    tweets_combined["text"]= tweets_combined["text"].astype(str)

    # Applying Model, Variable Creation
    tweets_combined['Polarity Score']=tweets_combined["text"].apply(lambda x:SIA.polarity_scores(x)['compound'])

    # Labelling with 1,0,-1, and adjusting threshold to be slightly more picky for positive tweets based on imbalances in testing
    
    score = tweets_combined["Polarity Score"].values
    sentiment = []
    for i in score:
        if i >= 0.3 :
            sentiment.append('1')
        elif i <= -0.05 :
            sentiment.append('-1')
        else:
            sentiment.append('0')
    tweets_combined["Label"] = sentiment
    tweets_combined = tweets_combined[['time','language', 'retweets', 'likes', 'replies', 'about', 'user', 'Followers', 'Data', 'Sector', 'Industry', 'Ticker', 'ID', 'length', 'Polarity Score', 'Label', 'text', 'text_og']]
    
    tweets_combined['Data'] = tweets_combined['Data'].astype('str')
    tweets_combined["Tags"] = tweets_combined.Data.apply(extract_tags)
    tweets_combined = tweets_combined.drop(columns='Data')
    
    # Extracting Todays Tweets from up to 7 days, depending on volume
    tweets_combined['Date'] = pd.to_datetime(tweets_combined['time']).dt.date
    tweets_combined['Date'] = tweets_combined['Date'].astype('str')
    tweets_grouped = tweets_combined.groupby('Date')
    todays_tweets = tweets_grouped.get_group(Todays_date)
    todays_tweets = todays_tweets[['time','language', 'retweets', 'likes', 'replies', 'about', 'user', 'Followers', 'Tags', 'Sector', 'Industry', 'Ticker', 'ID', 'length', 'Polarity Score', 'Date', 'Label', 'text', 'text_og']]
    
    # Output
    
    #save df as csv file, in S3 bucket
    bucket = "your-bucket-name"
    csv_name = Output_Path
    s3_path = "Twitter/2023/tweets_" + csv_name

    csv_buffer = StringIO()
    todays_tweets.to_csv(csv_buffer, sep='\t', line_terminator='\n', index=False)

    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, s3_path).put(Body=csv_buffer.getvalue())

    
    return todays_tweets