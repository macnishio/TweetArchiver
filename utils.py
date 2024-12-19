import pandas as pd
from datetime import datetime
import re

def parse_tweet_line(line):
    try:
        # Split preserving content with commas
        parts = []
        current_part = []
        in_quotes = False
        
        for char in line.strip():
            if char == '"':
                in_quotes = not in_quotes
            elif char == ',' and not in_quotes:
                parts.append(''.join(current_part).strip('"'))
                current_part = []
            else:
                current_part.append(char)
        
        parts.append(''.join(current_part).strip('"'))
        
        # Ensure minimum required parts
        if len(parts) < 2:
            return None
            
        # Parse timestamp
        timestamp_str = parts[0].strip('"')
        created_at = None
        
        if timestamp_str and timestamp_str.lower() != "nat":
            try:
                if "T" in timestamp_str:
                    created_at = datetime.strptime(timestamp_str.split("+")[0], "%Y-%m-%dT%H:%M:%S")
                else:
                    # Try different time formats
                    formats = [
                        "%Y-%m-%d %H:%M:%S",
                        "%b %d %Y, %I:%M %p",
                        "%I:%M %p",
                        "%B %d %Y, %I:%M %p"
                    ]
                    
                    for fmt in formats:
                        try:
                            created_at = datetime.strptime(timestamp_str, fmt)
                            break
                        except ValueError:
                            continue

            except Exception as e:
                print(f"Warning: Could not parse timestamp '{timestamp_str}': {e}")
                created_at = None

        # Extract engagement counts
        try:
            engagement = int(parts[2]) if len(parts) > 2 and parts[2].strip() else 0
        except (ValueError, IndexError):
            engagement = 0

        # Extract tweet data
        tweet = {
            'created_at': created_at,
            'text': parts[1] if len(parts) > 1 else "",
            'tweet_id': parts[3] if len(parts) > 3 else None,
            'url': parts[4] if len(parts) > 4 else None,
            'author_id': parts[6] if len(parts) > 6 else None,
            'author_username': parts[7] if len(parts) > 7 else None,
            'author_name': parts[7] if len(parts) > 7 else None,  # Using username as name if not provided
            'like_count': engagement
        }
        
        # Skip if no tweet_id (required field)
        if not tweet['tweet_id']:
            return None
            
        return tweet
    except Exception as e:
        print(f"Error parsing line: {line}\nError: {e}")
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