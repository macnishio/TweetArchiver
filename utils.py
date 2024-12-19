import pandas as pd
from datetime import datetime
import re

def parse_tweet_line(line):
    try:
        parts = line.strip().split(",")
        if len(parts) < 8:
            return None

        # Extract timestamp
        timestamp_str = parts[0].strip('"')  # Remove quotes if present
        created_at = None
        
        if timestamp_str and timestamp_str.lower() != "nat":
            try:
                if "T" in timestamp_str:
                    created_at = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S%z")
                else:
                    created_at = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
            except:
                created_at = None

        # Extract tweet data
        tweet = {
            'created_at': created_at,
            'text': parts[3] if len(parts) > 3 else "",
            'tweet_id': parts[4] if len(parts) > 4 else None,
            'url': parts[5] if len(parts) > 5 else None,
            'conversation_id': parts[7] if len(parts) > 7 else None,
            'author_id': parts[8] if len(parts) > 8 else None,
            'author_username': parts[9] if len(parts) > 9 else None,
            'author_name': parts[10] if len(parts) > 10 else None
        }
        
        return tweet
    except Exception as e:
        print(f"Error parsing line: {e}")
        return None

def process_file_content(content):
    lines = content.split('\n')
    tweets = []
    
    for line in lines:
        if not line.strip():
            continue
        
        tweet = parse_tweet_line(line)
        if tweet:
            tweets.append(tweet)
    
    return pd.DataFrame(tweets)

def clean_tweet_data(df):
    # Create a copy of the dataframe first
    df = df.copy()
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['tweet_id'], keep='first')
    
    # Clean text using loc
    df.loc[:, 'text'] = df['text'].fillna('')
    
    # Ensure datetime and remove invalid timestamps
    df.loc[:, 'created_at'] = pd.to_datetime(df['created_at'], utc=True, errors='coerce')
    df = df.dropna(subset=['created_at'])
    
    return df

