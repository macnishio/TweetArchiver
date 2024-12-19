import os
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from datetime import datetime

class Database:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname=os.environ.get('PGDATABASE'),
            user=os.environ.get('PGUSER'),
            password=os.environ.get('PGPASSWORD'),
            host=os.environ.get('PGHOST'),
            port=os.environ.get('PGPORT')
        )
        self.create_tables()

    def create_tables(self):
        with self.conn.cursor() as cur:
            with open('schema.sql', 'r') as f:
                cur.execute(f.read())
            self.conn.commit()

    def insert_tweets(self, df):
        if df.empty:
            return 0
        
        # Prepare data for insertion
        data = []
        for _, row in df.iterrows():
            # Skip rows without tweet_id (required field)
            if pd.isna(row.get('tweet_id')):
                continue
                
            data.append((
                str(row.get('tweet_id')),  # Ensure tweet_id is string
                row.get('created_at'),
                row.get('author_id'),
                row.get('author_username'),
                row.get('author_username'),  # Using username as name
                row.get('text', ''),
                0,  # reply_count
                0,  # retweet_count
                row.get('like_count', 0),
                row.get('url'),
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
        conditions = []
        params = []

        if keyword:
            conditions.append("text ILIKE %s")
            params.append(f"%{keyword}%")

        if username:
            conditions.append("author_username ILIKE %s")
            params.append(f"%{username}%")

        if start_date:
            conditions.append("created_at >= %s")
            params.append(start_date)

        if end_date:
            conditions.append("created_at <= %s")
            params.append(end_date)

        query = "SELECT * FROM tweets"
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY created_at DESC LIMIT 1000"

        return pd.read_sql(query, self.conn, params=params)

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

