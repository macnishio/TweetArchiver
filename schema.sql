CREATE TABLE IF NOT EXISTS tweets (
    id SERIAL PRIMARY KEY,
    tweet_id VARCHAR(255) UNIQUE,
    created_at TIMESTAMP,
    author_id VARCHAR(255),
    author_username VARCHAR(255),
    author_name VARCHAR(255),
    text TEXT,
    reply_count INTEGER DEFAULT 0,
    retweet_count INTEGER DEFAULT 0,
    like_count INTEGER DEFAULT 0,
    url VARCHAR(512),
    conversation_id VARCHAR(255),
    in_reply_to_user_id VARCHAR(255),
    created_at_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tweets_author_username ON tweets(author_username);
CREATE INDEX IF NOT EXISTS idx_tweets_created_at ON tweets(created_at);
CREATE INDEX IF NOT EXISTS idx_tweets_tweet_id ON tweets(tweet_id);
