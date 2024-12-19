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
        
        # カンマで分割（JSONやURLを保持）
        parts = []
        current_part = []
        in_quotes = False
        in_json = False
        json_brace_count = 0
        
        for char in line.strip():
            if char == '"' and not in_json:
                in_quotes = not in_quotes
            elif char == '{':
                in_json = True
                json_brace_count += 1
            elif char == '}':
                json_brace_count -= 1
                if json_brace_count == 0:
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
                else:
                    # 他の形式を試行
                    created_at = parse(timestamp_str)
            except Exception as e:
                logger.error(f"Error parsing timestamp '{timestamp_str}': {e}")

        # エンゲージメント数の解析
        try:
            engagement = int(parts[2]) if parts[2].strip() else 0
        except (ValueError, IndexError):
            engagement = 0
            logger.warning(f"Could not parse engagement count from: {parts[2] if len(parts) > 2 else 'N/A'}")

        # テキスト内容の取得（3番目のフィールド）
        text = clean_text(parts[3]) if len(parts) > 3 else ""

        # URLとtweet_idの抽出（4番目のフィールド）
        url = parts[4] if len(parts) > 4 else None
        if url:
            url = url.split(' ')[0]  # 余分な部分を削除
        tweet_id = extract_tweet_id_from_url(url)

        # JSONメタデータの解析（7番目のフィールド）
        metadata = parse_json_field(parts[6]) if len(parts) > 6 else None
        
        # ツイートデータの構築
        tweet = {
            'created_at': created_at,
            'text': text,
            'tweet_id': tweet_id,
            'url': url,
            'like_count': engagement,
            'author_id': parts[7] if len(parts) > 7 else None,
            'author_username': parts[8].replace('@', '') if len(parts) > 8 else None,
            'author_name': parts[8].replace('@', '') if len(parts) > 8 else None  # 同じフィールドを使用
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
    
    for i, line in enumerate(lines, 1):
        if not line.strip():
            continue
            
        tweet = parse_tweet_line(line)
        if tweet:
            tweets.append(tweet)
        else:
            skipped += 1
            logger.warning(f"Skipped line {i}: {line[:100]}...")  # 最初の100文字のみログ出力
    
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
    
    # Ensure datetime and remove invalid timestamps
    df.loc[:, 'created_at'] = pd.to_datetime(df['created_at'], utc=True, errors='coerce')
    
    # Remove rows with missing required fields
    df = df.dropna(subset=['tweet_id', 'created_at'])
    
    logger.info(f"After cleaning: {len(df)} tweets remain")
    return df
