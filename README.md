# Stock Twitter Scraper and Sentiment Analyzer

This project consists of an automated Twitter scraper that collects daily tweets about stocks, performs sentiment analysis to classify them as positive, negative, or neutral, and stores the resulting data on Amazon S3. It also includes an API script for retrieving the average sentiment of a given stock and tweets over a specified period from the stored data.

## Features

- Scrape daily tweets about 250+ stock twitter handles from excel sheet using Twitter API.
- Perform sentiment analysis on tweets using VaderSentiment to classify them as positive, negative, or neutral.
- Store the resulting data on Amazon S3 in CSV format.
- Automated daily scraping through AWS Lambda and CloudWatch.
- An API script to fetch the average sentiment and tweets for a particular stock over any available time range.

## Architecture

- `twitter_scraper.py`: Main script that scrapes tweets and performs sentiment analysis.
- `api_script.py`: Script for querying average sentiment data.
- `aws_lambda_function.py`: Configuration for AWS Lambda.
- `data/companies.csv`: Spreadsheet with information about 250+ companies (Twitter handles, tickers, etc.)

## Set-Up and Configuration

### Prerequisites

- Python 3.8
- Twitter Developer Account
- AWS Account

### Installation

1. Clone this repository to your local machine.
2. Install the required Python packages: `pip install -r requirements.txt`

### Twitter API Keys

You'll need to set up a Twitter Developer Account and create an application to obtain API keys.

Place your Twitter API keys in a configuration file or environment variable.

### AWS Configuration

- Set up AWS S3 to store the resulting CSV files.
- Configure AWS Lambda to run the script.
- Set up AWS CloudWatch to trigger the Lambda function daily with a cron job.

## Usage

### Scraping and Analyzing Tweets

Run the twitter scraper script. This will scrape tweets, analyze them, and store the data on S3. If running on AWS Lambda, this process is automated. Code can also be tweaked to just run and store everything locally. 

### Querying Sentiment Data

Use the API script to query average sentiment data for a specific stock and time range. Adjust inputs manually. This code is set up to configure with a UX platform and will  need to be tweaked for personal use. 

## Disclaimer

This code is provided for educational and demonstration purposes. Make sure to comply with Twitter's policies and terms of use, as well as any legal obligations concerning the data you are handling.

## License

CC0 1.0 Universal
