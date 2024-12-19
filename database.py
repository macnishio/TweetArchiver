import os
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

class Database:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname=os.environ.get('PGDATABASE'),
            user=os.environ.get('PGUSER'),
            password=os.environ.get('PGPASSWORD'),
            host=os.environ.get('PGHOST'),
            port=os.environ.get('PGPORT')
        )
        self.engine = create_engine(
            f"postgresql://{os.environ.get('PGUSER')}:{os.environ.get('PGPASSWORD')}@"
            f"{os.environ.get('PGHOST')}:{os.environ.get('PGPORT')}/{os.environ.get('PGDATABASE')}"
        )
        self._ensure_tables_exist()

    def _ensure_tables_exist(self):
        """テーブルが存在しない場合のみ作成"""
        with self.conn.cursor() as cur:
            # テーブルの存在確認
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'tweets'
                );
            """)
            table_exists = cur.fetchone()[0]
            
            if not table_exists:
                with open('schema.sql', 'r') as f:
                    cur.execute(f.read())
                self.conn.commit()
                logger.info("Tweets table created successfully")

    def insert_tweets(self, df):
        if df.empty:
            return 0
        
        # Prepare data for insertion
        data = []
        for _, row in df.iterrows():
            # Skip rows without tweet_id (required field)
            tweet_id = row.get('tweet_id')
            if pd.isna(tweet_id):
                continue

            # Ensure numeric fields are within PostgreSQL integer limits
            like_count = row.get('like_count', 0)
            if isinstance(like_count, (int, float)):
                like_count = min(like_count, 2147483647)  # PostgreSQL INTEGER最大値
            else:
                like_count = 0
                    
            data.append((
                str(tweet_id),  # 確実に文字列として扱う
                row.get('created_at'),
                str(row.get('author_id')) if row.get('author_id') else None,
                str(row.get('author_username')) if row.get('author_username') else None,
                str(row.get('author_username')) if row.get('author_username') else None,  # Using username as name
                str(row.get('text', '')),
                0,  # reply_count
                0,  # retweet_count
                like_count,
                str(row.get('url')) if row.get('url') else None,
                None,  # conversation_id
                None   # in_reply_to_user_id
            ))

        if not data:  # If no valid data after filtering
            return 0

        with self.conn.cursor() as cur:
            query = """
                INSERT INTO tweets (
                    tweet_id, created_at, author_id, author_username, author_name,
                    text, reply_count, retweet_count, like_count, url,
                    conversation_id, in_reply_to_user_id
                ) VALUES %s
                ON CONFLICT (tweet_id) DO NOTHING
            """
            execute_values(cur, query, data)
            self.conn.commit()
            return len(data)

    def get_tweets(self, limit=1000, offset=0):
        query = """
            SELECT * FROM tweets 
            ORDER BY created_at DESC 
            LIMIT %s OFFSET %s
        """
        return pd.read_sql(query, self.conn, params=(limit, offset))

    def search_tweets(self, keyword=None, username=None, start_date=None, end_date=None):
        conditions = ["1=1"]  # Always true condition to simplify query building
        params = []

        if keyword:
            # Split keywords and search for each word
            keywords = keyword.split()
            keyword_conditions = []
            for kw in keywords:
                if kw.lower() == "no":  # Skip "no" as a search term
                    continue
                keyword_conditions.append("text ILIKE %s")
                params.append(f"%{kw}%")
            if keyword_conditions:
                conditions.append("(" + " OR ".join(keyword_conditions) + ")")

        if username:
            conditions.append("author_username ILIKE %s")
            params.append(f"%{username}%")
            
        # Add date condition for tweets on or before 2024-04-30
        conditions.append("created_at <= %s")
        params.append(pd.Timestamp('2024-04-30').tz_localize('UTC'))

        if start_date:
            start_datetime = pd.Timestamp(start_date).tz_localize('UTC')
            conditions.append("created_at >= %s")
            params.append(start_datetime)

        if end_date:
            end_datetime = pd.Timestamp(end_date).tz_localize('UTC')
            conditions.append("DATE(created_at) <= DATE(%s)")
            params.append(end_datetime)

        query = """
            SELECT 
                id,
                tweet_id,
                created_at AT TIME ZONE 'UTC' as created_at,
                author_id,
                author_username,
                author_name,
                text,
                reply_count,
                retweet_count,
                like_count,
                url,
                conversation_id,
                in_reply_to_user_id
            FROM tweets
        """
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY created_at DESC LIMIT 1000"

        df = pd.read_sql_query(query, self.engine, params=tuple(params))
        if not df.empty:
            df['created_at'] = pd.to_datetime(df['created_at'])
            # Check if timestamps are already tz-aware
            if df['created_at'].dt.tz is None:
                df['created_at'] = df['created_at'].dt.tz_localize('UTC')
            else:
                df['created_at'] = df['created_at'].dt.tz_convert('UTC')
        return df

    def get_stats(self):
        query = """
            SELECT 
                COUNT(*) as total_tweets,
                COUNT(DISTINCT author_username) as unique_authors,
                MAX(created_at) as latest_tweet,
                MIN(created_at) as oldest_tweet
            FROM tweets
        """
        return pd.read_sql(query, self.conn)