import pandas as pd
from datetime import datetime
import re
import logging

# ロギングの設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def clean_text(text):
    """テキストのクリーニング"""
    if not isinstance(text, str):
        return ""
    # 特殊文字の削除と空白の正規化
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def parse_tweet_line(line):
    try:
        # 行全体をログに記録
        logger.debug(f"Processing line: {line}")
        
        # カンマで分割（JSONやURLを保持）
        parts = []
        current_part = []
        in_quotes = False
        in_json = False
        
        for char in line.strip():
            if char == '"' and not in_json:
                in_quotes = not in_quotes
            elif char == '{':
                in_json = True
            elif char == '}':
                in_json = False
            
            if char == ',' and not (in_quotes or in_json):
                parts.append(''.join(current_part))
                current_part = []
            else:
                current_part.append(char)
        
        parts.append(''.join(current_part))
        parts = [clean_text(p.strip('"')) for p in parts]

        if not parts[0]:
            logger.warning("Skipping line: No timestamp found")
            return None

        # タイムスタンプの解析
        timestamp_str = parts[0]
        created_at = None
        
        if timestamp_str and timestamp_str.lower() != "nat":
            # ユーザー名とURLが含まれている場合は除去
            if " http" in timestamp_str:
                timestamp_str = timestamp_str.split(" http")[0]
            
            # ユーザー名が含まれている場合は除去
            if " " in timestamp_str and "@" in timestamp_str:
                timestamp_str = " ".join(timestamp_str.split(" ")[1:])
            
            try:
                # 様々なフォーマットを試行
                formats = [
                    "%Y-%m-%dT%H:%M:%S",  # ISO format
                    "%Y-%m-%d %H:%M:%S",
                    "%b %d %Y, %I:%M %p",
                    "%B %d %Y, %I:%M %p",
                    "%Y/%m/%d %H:%M:%S",
                    "%d/%m/%Y %H:%M:%S",
                    "%I:%M %p"
                ]
                
                for fmt in formats:
                    try:
                        if "T" in timestamp_str and fmt == "%Y-%m-%dT%H:%M:%S":
                            created_at = datetime.strptime(timestamp_str.split("+")[0], fmt)
                        else:
                            created_at = datetime.strptime(timestamp_str, fmt)
                        logger.debug(f"Successfully parsed timestamp: {timestamp_str} with format {fmt}")
                        break
                    except ValueError:
                        continue

                if not created_at:
                    logger.warning(f"Could not parse timestamp with any format: {timestamp_str}")
                    
            except Exception as e:
                logger.error(f"Error parsing timestamp '{timestamp_str}': {e}")
                created_at = None

        # Extract engagement counts (assuming it's still at index 2)
        try:
            engagement = int(parts[2]) if len(parts) > 2 and parts[2].strip() else 0
        except (ValueError, IndexError):
            engagement = 0
            logger.warning("Could not parse engagement count.")

        # ツイートデータの抽出
        tweet = {
            'created_at': created_at,
            'text': clean_text(parts[3]) if len(parts) > 3 else "",
            'tweet_id': parts[4].split('/')[-1] if len(parts) > 4 and '/' in parts[4] else parts[4] if len(parts) > 4 else None,
            'url': parts[4] if len(parts) > 4 else None,
            'like_count': engagement,
            'author_id': parts[7] if len(parts) > 7 else None,
            'author_username': parts[8].replace('@', '') if len(parts) > 8 else None,
            'author_name': parts[8].replace('@', '') if len(parts) > 8 else None
        }
        
        # URLの正規化
        if tweet['url'] and 'twitter.com' in tweet['url']:
            tweet['url'] = tweet['url'].split(' ')[0]  # 余分な部分を削除
            
        # ツイートIDの正規化
        if tweet['tweet_id']:
            tweet['tweet_id'] = str(tweet['tweet_id']).split('/')[-1]  # URLからIDを抽出
            tweet['tweet_id'] = re.sub(r'\D', '', tweet['tweet_id'])  # 数字以外を削除
        
        # Skip if no tweet_id (required field)
        if not tweet['tweet_id']:
            logger.warning("Skipping tweet: No tweet_id found.")
            return None
            
        return tweet
    except Exception as e:
        logger.error(f"Error parsing line: {line}\nError: {e}")
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