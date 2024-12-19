import pandas as pd
from datetime import datetime
import re
import logging
import json

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

def extract_tweet_id_from_url(url):
    """URLからツイートIDを抽出"""
    if not url:
        return None
    try:
        # URLの最後の部分からIDを抽出
        tweet_id = url.split('/')[-1]
        # 数字以外を削除
        tweet_id = re.sub(r'\D', '', tweet_id)
        return tweet_id if tweet_id else None
    except:
        return None

def parse_json_field(json_str):
    """JSON文字列をパース"""
    if not json_str:
        return None
    try:
        return json.loads(json_str)
    except:
        return None

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
        parts = [p.strip().strip('"') for p in parts]

        # 最低限必要なフィールド数をチェック
        if len(parts) < 5:
            logger.warning(f"Insufficient fields in line: {len(parts)} fields")
            return None

        # タイムスタンプの解析
        timestamp_str = parts[0]
        created_at = None
        
        if timestamp_str and timestamp_str.lower() != "nat":
            try:
                if "T" in timestamp_str:
                    # ISO 8601形式のパース
                    created_at = datetime.strptime(timestamp_str.split("+")[0], "%Y-%m-%dT%H:%M:%S")
            except Exception as e:
                logger.error(f"Error parsing timestamp '{timestamp_str}': {e}")

        # エンゲージメント数の解析
        try:
            engagement = int(parts[2]) if parts[2].strip() else 0
        except (ValueError, IndexError):
            engagement = 0
            logger.warning("Could not parse engagement count")

        # URLとツイートIDの解析
        url = parts[4] if len(parts) > 4 else None
        tweet_id = extract_tweet_id_from_url(url)

        # JSONメタデータの解析
        metadata = parse_json_field(parts[6]) if len(parts) > 6 else None
        
        # ツイートデータの構築
        tweet = {
            'created_at': created_at,
            'text': clean_text(parts[3]),
            'tweet_id': tweet_id,
            'url': url.split(' ')[0] if url else None,  # 余分な部分を削除
            'like_count': engagement,
            'author_id': parts[7] if len(parts) > 7 else None,
            'author_username': parts[8].replace('@', '') if len(parts) > 8 else None,
            'author_name': parts[9] if len(parts) > 9 else None
        }
        
        # tweet_idが必須
        if not tweet['tweet_id']:
            logger.warning("Skipping tweet: No tweet_id found")
            return None
            
        return tweet
    except Exception as e:
        logger.error(f"Error parsing line: {line}\nError: {e}")
        return None

def process_file_content(content):
    """ファイル内容を処理してデータフレームを返す"""
    lines = content.split('\n')
    tweets = []
    
    for line in lines:
        if not line.strip():
            continue
        
        tweet = parse_tweet_line(line)
        if tweet:
            tweets.append(tweet)
    
    logger.info(f"Successfully parsed {len(tweets)} tweets")
    return pd.DataFrame(tweets)

def clean_tweet_data(df):
    """データフレームのクリーニングと正規化"""
    if df.empty:
        return df
        
    # Create a copy of the dataframe
    df = df.copy()
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['tweet_id'], keep='first')
    
    # Clean text
    df.loc[:, 'text'] = df['text'].fillna('')
    
    # Ensure datetime and remove invalid timestamps
    df.loc[:, 'created_at'] = pd.to_datetime(df['created_at'], utc=True, errors='coerce')
    
    # Remove rows with missing required fields
    df = df.dropna(subset=['tweet_id', 'created_at'])
    
    logger.info(f"After cleaning: {len(df)} tweets remain")
    return df
