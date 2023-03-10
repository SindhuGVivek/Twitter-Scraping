import pandas as pd
import streamlit as st
import pymongo
import snscrape.modules.twitter as sntwitter
import json, base64
from dotenv.main import load_dotenv
import os
from datetime import datetime

load_dotenv()

# define the function to scrape Twitter data
def scrape_twitter_data(query, start_date, end_date, max_tweets):
    scraped_data = []
    for i, tweet in enumerate(
        sntwitter.TwitterSearchScraper(
            f"{query} since:{start_date} until:{end_date}"
        ).get_items()
    ):
        if i >= max_tweets:
            break
        scraped_data.append(
            {
                "date": tweet.date.strftime("%Y-%m-%d %H:%M:%S"),
                "id": tweet.id,
                "url": tweet.url,
                "content": tweet.content,
                "user": tweet.user.username,
                "reply_count": tweet.replyCount,
                "retweet_count": tweet.retweetCount,
                "language": tweet.lang,
                "source": tweet.sourceLabel,
                "like_count": tweet.likeCount,
            }
        )
    return scraped_data


# define the function to store data in MongoDB
def store_data_in_mongodb(query, start_date, end_date, scraped_data):
    # connect to MongoDB
    client = pymongo.MongoClient(os.environ["MONGO_URI"])
    # select the database and collection
    db = client["twitter"]
    collection = db["tweets"]
    # create the document to insert into the collection
    SD = datetime.combine(start_date, datetime.min.time())
    ED = datetime.combine(end_date, datetime.min.time())
    document = {
        "scraped_word": query,
        "scraped_date_range": f"{SD} - {ED}",
        "scraped_date": f"{datetime.today()}",
        "scraped_data": scraped_data,
    }
    # insert the document into the collection
    collection.insert_one(document)


# set the sidebar width
st.markdown(
    "<style>.sidebar .sidebar-content { width: 300px; }</style>", unsafe_allow_html=True
)

# add a title and description to the sidebar
st.sidebar.title("Twitter Scraper")
st.sidebar.write(
    "This app allows you to scrape Twitter data and store it in a MongoDB database."
)

# add input fields to the sidebar
query = st.sidebar.text_input("Search Query", "Coimbatore")
start_date = st.sidebar.date_input("Start Date")
end_date = st.sidebar.date_input("End Date")
max_tweets = st.sidebar.number_input(
    "Max Tweets", min_value=1, max_value=1000, value=100
)

# connect to MongoDB
client = pymongo.MongoClient(os.environ["MONGO_URI"])
# select the database and collection
db = client["twitter"]
collection = db["tweets"]

# tweet_count = st.slider("Number of tweets to scrape:", 100, 10000, 1000)

# # add a button to scrape the data
# if st.sidebar.button("Scrape Data"):
#     # scrape the data using the input parameters
#     scraped_data = scrape_twitter_data(query, start_date, end_date, max_tweets)
#     # store the data in MongoDB
#     # store_data_in_mongodb(query, start_date, end_date, scraped_data)
#     # display the data in a table
#     df = pd.DataFrame(scraped_data)
#     st.write(df)

# add a button to download the data in CSV format
if st.button("Download CSV"):
    # retrieve the data from the collection
    scraped_data = scrape_twitter_data(query, start_date, end_date, max_tweets)
    df = pd.DataFrame(scraped_data)
    st.write(df)

    # Insert the scraped data into MongoDB
    SD = datetime.combine(start_date, datetime.min.time())
    ED = datetime.combine(end_date, datetime.min.time())
    collection.insert_one(
        {
            "scraped_word": query,
            "scraped_date_range": f"{SD} - {ED}",
            "scraped_date": f"{datetime.today()}",
            "scraped_data": scraped_data,
        }
    )
    st.download_button(
        "Download",
        df.to_csv(index=False),
        file_name=f"{query}_{start_date}_{end_date}.csv",
        mime="text/csv",
    )

# add a button to download the data in JSON format
@st.cache_data
def download_json(data):
    json_data = json.dumps(data, indent=4)
    b64 = base64.b64encode(json_data.encode()).decode()
    href = f'<a href="data:file/json;base64,{b64}" download="scraped_data.json">Download JSON</a>'
    return href


if st.button("Download JSON"):
    scraped_data = scrape_twitter_data(query, start_date, end_date, max_tweets)
    df = pd.DataFrame(scraped_data)
    st.write(df)

    # Insert the scraped data into MongoDB
    SD = datetime.combine(start_date, datetime.min.time())
    ED = datetime.combine(end_date, datetime.min.time())
    collection.insert_one(
        {
            "keyword": query,
            "start_date": SD,
            "end_date": ED,
            "data": scraped_data,
        }
    )

    # Display the scraped data in a table
    df = pd.DataFrame(scraped_data)
    st.markdown(download_json(scraped_data), unsafe_allow_html=True)
