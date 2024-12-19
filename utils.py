import pandas as pd
from datetime import datetime
import re
import logging
import json
from dateutil.parser import parse

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
        # URLからIDを抽出して数字のみを取得
        matches = re.findall(r'/status/(\d+)', url)
        return matches[0] if matches else None
    except Exception as e:
        logger.error(f"Error extracting tweet ID from URL {url}: {e}")
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
    """1行のツイートデータをパース"""
    try:
        # 行全体をログに記録（デバッグ用）
        logger.debug(f"Processing line: {line}")
        
        # 基本的なパターンをチェック
        if 'twitter.com' not in line:
            logger.debug(f"Line does not contain twitter.com URL: {line[:100]}")
            return None
            
        # スペースで分割してフィールドを抽出
        parts = line.strip().split(' ')
        
        # 必要なフィールドを抽出
        author_username = parts[0]  # 最初の部分はユーザー名
        
        # タイムスタンプを探す（ISO 8601形式）
        timestamp_str = None
        for part in parts:
            if 'T' in part and '+' in part:
                timestamp_str = part
                break
        
        # タイムスタンプの解析
        created_at = None
        if timestamp_str:
            try:
                created_at = datetime.strptime(timestamp_str.split("+")[0], "%Y-%m-%dT%H:%M:%S")
            except Exception as e:
                logger.error(f"Error parsing timestamp '{timestamp_str}': {e}")
                return None

        # URLを探してtweet_idを抽出
        tweet_url = None
        for part in parts:
            if 'twitter.com' in part:
                tweet_url = part
                break
        
        if not tweet_url:
            logger.warning("No Twitter URL found in line")
            return None
            
        tweet_id = extract_tweet_id_from_url(tweet_url)
        if not tweet_id:
            logger.warning(f"Could not extract tweet ID from URL: {tweet_url}")
            return None

        # テキストの抽出（URLの前の部分）
        text = ""
        for part in parts:
            if 'twitter.com' in part:
                break
            if part != author_username and 'T' not in part:  # ユーザー名とタイムスタンプ以外
                text += part + " "
        text = clean_text(text)
        
        # エンゲージメント数を探す（数字のみの部分）
        engagement = 0
        for part in parts:
            if part.isdigit():
                engagement = int(part)
                break
        
        # ツイートデータの構築
        tweet = {
            'created_at': created_at,
            'text': text,
            'tweet_id': tweet_id,
            'url': tweet_url,
            'like_count': engagement,
            'author_id': None,  # 現在のデータ形式では利用不可
            'author_username': author_username.replace('@', ''),
            'author_name': author_username.replace('@', '')  # ユーザー名を名前としても使用
        }
        
        # 必須フィールドのチェック
        if not tweet['tweet_id']:
            logger.warning(f"Skipping tweet: No tweet_id found in URL: {url}")
            return None
            
        if not tweet['created_at']:
            logger.warning(f"Skipping tweet: Invalid timestamp: {timestamp_str}")
            return None

        return tweet
    except Exception as e:
        logger.error(f"Error parsing line: {line}\nError: {e}")
        return None

def process_file_content(content):
    """ファイル内容を処理してデータフレームを返す"""
    lines = content.split('\n')
    tweets = []
    skipped = 0
    i = 0
    
    while i < len(lines):
        line = lines[i].strip()
        
        # 空行をスキップ
        if not line:
            i += 1
            continue
            
        # 時刻のみの行をスキップ（例: "07:28 PM"）
        if re.match(r'^\d{2}:\d{2}\s+[AP]M$', line):
            i += 1
            continue
            
        # ツイートデータを含む行を処理
        if 'twitter.com' in line:
            tweet = parse_tweet_line(line)
            if tweet:
                tweets.append(tweet)
            else:
                skipped += 1
                logger.warning(f"Failed to parse tweet at line {i+1}: {line[:100]}...")
        else:
            skipped += 1
            logger.debug(f"Skipped non-tweet line {i+1}: {line[:100]}...")
            
        i += 1
    
    logger.info(f"Successfully parsed {len(tweets)} tweets, skipped {skipped} lines")
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
    
    # Convert timestamps to UTC datetime
    df = df.assign(
        created_at=lambda x: pd.to_datetime(x['created_at'])
        .dt.tz_localize('UTC')
        .dt.tz_convert('UTC')
    )
    
    # Remove rows with missing required fields
    df = df.dropna(subset=['tweet_id', 'created_at'])
    
    logger.info(f"After cleaning: {len(df)} tweets remain")
    return df
