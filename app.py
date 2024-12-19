import streamlit as st
import pandas as pd
import plotly.express as px
from database import Database
from utils import process_file_content, clean_tweet_data
from datetime import datetime, timedelta
import io

# Initialize database
db = Database()

# Page config
st.set_page_config(
    page_title="Tweet Analytics Dashboard",
    page_icon="🐦",
    layout="wide"
)

# Sidebar
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Upload", "Search & Analysis", "Statistics"])

if page == "Upload":
    st.title("Tweet Data Upload")
    
    uploaded_file = st.file_uploader("Choose a file", type=['txt', 'csv'])
    
    if uploaded_file is not None:
        try:
            content = uploaded_file.getvalue().decode()
            
            with st.spinner('Processing data...'):
                df = process_file_content(content)
                df = clean_tweet_data(df)
                
                if not df.empty:
                    st.write(f"Found {len(df)} tweets")
                    st.dataframe(df.head())
                    
                    if st.button("Upload to Database"):
                        inserted = db.insert_tweets(df)
                        st.success(f"Successfully uploaded {inserted} tweets to database!")
                else:
                    st.error("No valid tweets found in the file")
                    
        except Exception as e:
            st.error(f"Error processing file: {str(e)}")

elif page == "Search & Analysis":
    st.title("Tweet Search & Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        keyword = st.text_input("Search by keyword")
        username = st.text_input("Search by username")
    
    with col2:
        start_date = st.date_input("Start date", datetime.now() - timedelta(days=7))
        end_date = st.date_input("End date", datetime.now())
    
    if st.button("Search"):
        tweets = db.search_tweets(
            keyword=keyword,
            username=username,
            start_date=start_date,
            end_date=end_date
        )
        
        if not tweets.empty:
            st.write(f"Found {len(tweets)} tweets")
            
            # Timeline visualization
            fig = px.histogram(tweets, x='created_at', title='Tweet Timeline')
            st.plotly_chart(fig)
            
            # Top users visualization
            user_counts = tweets['author_username'].value_counts().head(10)
            fig2 = px.bar(
                x=user_counts.index, 
                y=user_counts.values,
                title='Top Users by Tweet Count'
            )
            st.plotly_chart(fig2)
            
            # Show tweets
            st.dataframe(tweets)
            
            # Export option
            if st.button("Export to CSV"):
                csv = tweets.to_csv(index=False)
                st.download_button(
                    "Download CSV",
                    csv,
                    "tweets.csv",
                    "text/csv",
                    key='download-csv'
                )
        else:
            st.info("No tweets found matching your criteria")

elif page == "Statistics":
    st.title("Tweet Statistics")
    
    stats = db.get_stats()
    
    if not stats.empty:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Tweets", stats['total_tweets'].iloc[0])
        
        with col2:
            st.metric("Unique Authors", stats['unique_authors'].iloc[0])
        
        with col3:
            st.metric("Latest Tweet", stats['latest_tweet'].iloc[0].strftime('%Y-%m-%d'))
        
        with col4:
            st.metric("Oldest Tweet", stats['oldest_tweet'].iloc[0].strftime('%Y-%m-%d'))
        
        # Get tweets for time series
        tweets = db.get_tweets(limit=10000)
        if not tweets.empty:
            # Daily tweet count
            daily_tweets = tweets.set_index('created_at').resample('D').size()
            fig = px.line(
                daily_tweets,
                title='Daily Tweet Count',
                labels={'value': 'Tweet Count', 'created_at': 'Date'}
            )
            st.plotly_chart(fig)
            
            # User activity
            user_activity = tweets['author_username'].value_counts().head(10)
            fig2 = px.bar(
                x=user_activity.index,
                y=user_activity.values,
                title='Most Active Users',
                labels={'x': 'Username', 'y': 'Tweet Count'}
            )
            st.plotly_chart(fig2)
    else:
        st.info("No data available in the database")

