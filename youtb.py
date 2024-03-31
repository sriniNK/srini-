import os
import json
import psycopg2
import pandas as pd
import googleapiclient.discovery
from googleapiclient.discovery import build
import streamlit as st


def api_connection():
    api="AIzaSyDFPSns-KmH_c1wNxxVVIqGQfmnXkI0p6o"
    
    api_service_name="youtube"
    api_version="v3"
    
    youtube=build(api_service_name,api_version,developerKey=api)
    
    return youtube
youtube=api_connection()

#get_channel_details

def channel_details(youtube, channel_id):
    request = youtube.channels().list(
        part=["snippet", "contentDetails", "statistics"],
        id=channel_id
    )
    response = request.execute()
    channel_data = []
    for item in response['items']:
        data = {
            "channel_name": item["snippet"]["title"],
            "channel_id": item["id"],
            "Subscribers": item['statistics']['subscriberCount'],
            "Views": item['statistics']['viewCount'],
            "total_vd": item['statistics']['videoCount'],
            "Description": item['snippet']['description'],
            "Playlist_Id": item['contentDetails']['relatedPlaylists']['uploads']
        }
        channel_data.append(data)
    return channel_data

#get videos_id

def get_video_ids(youtube, channel_id):
    video_ids = []
    response = youtube.channels().list(id=channel_id, part="contentDetails").execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    while True:
        response = youtube.playlistItems().list(part="snippet", playlistId=playlist_id, maxResults=50, pageToken=next_page_token).execute()
        for item in response['items']:
            video_ids.append(item['snippet']['resourceId']['videoId'])
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return video_ids

#get_video_info

def video_details(youtube, video_ids):
    video_details_list = []
    for video_id in video_ids:
        request = youtube.videos().list(part="snippet,contentDetails,statistics", id=video_id)
        response = request.execute()
        for item in response["items"]:
            data = {
                "channel_name": item["snippet"]["channelTitle"],
                "channel_id": item["snippet"]["channelId"],
                "video_id": item['id'],
                "Title": item["snippet"]["title"],
                "Thumbnail": item["snippet"]["thumbnails"],
                "Des": item["snippet"]["description"],
                "Vcount": item["statistics"]["viewCount"],
                "publish_date": item["snippet"]["publishedAt"],
                "likes": item["statistics"]["likeCount"],
                "duration": item["contentDetails"]["duration"]
            }
            video_details_list.append(data)
    return video_details_list


#getting _videos_comments

def video_comments(youtube, video_ids):
    comment_info = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(part="snippet", videoId=video_id, maxResults=50)
            response = request.execute()
            for item in response["items"]:
                data = {
                    "cmt_id": item["snippet"]["topLevelComment"]["id"],
                    "Video_id": item["snippet"]["topLevelComment"]["snippet"]["videoId"],
                    "cmt_text": item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                    "cmt_auth": item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"]
                }
                comment_info.append(data)
    except Exception as e:
        print("An error occurred:", e)
    return comment_info

#getting playlist details

def playlist_info(youtube, channel_id):
    next_page_token = None
    playlist_data = []
    while True:
        request = youtube.playlists().list(part="snippet,contentDetails", channelId=channel_id, maxResults=50, pageToken=next_page_token)
        response = request.execute()
        for item in response['items']:
            data = {
                "Playlist_Id": item['id'],
                "Title": item['snippet']['title'],
                "Channel_Id": item['snippet']['channelId'],
                "Channel_Name": item['snippet']['channelTitle'],
                "Video_count": item['contentDetails']['itemCount']
            }
            playlist_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return playlist_data

#insertion data

def insert_into_postgres(data):
    mydb = psycopg2.connect(host='localhost', user="postgres", password="pulsar220", database="youtubedata", port="5432")
    cursor = mydb.cursor()
    try:
        for row in data:
            insert_query = "INSERT INTO channels (channel_name, channel_id, Subscribers, Views, total_vd, Description, Playlist_Id) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(insert_query, (row["channel_name"], row["channel_id"], row["Subscribers"], row["Views"], row["total_vd"], row["Description"], row["Playlist_Id"]))
        mydb.commit()
        st.success("Data inserted into PostgreSQL successfully!")
    except Exception as e:
        st.error(f"An error occurred: {e}")
    finally:
        cursor.close()
        mydb.close()

#databse_connection
def create_postgres_table():
        
    mydb=psycopg2.connect(host="localhost",user="postgres",password="pulsar220",database="youtube_data",port="5432")
    cursor=mydb.cursor()

    try:
        create_query="create table if not exists channels(channel_name varchar(100),channel_id varchar(100) primary key,Subscribers int,Views int,total_vd int,Description text,Playlist_Id varchar(100))"
        cursor.execute(create_query)
        mydb.commit()
        
    except:
        print("Channel Table Created already")
    
def insert_into_postgres(data):
    mydb = psycopg2.connect(host='localhost', user="postgres", password="pulsar220", database="youtube_data", port="5432")
    cursor = mydb.cursor()
    try:
        for row in data:
            insert_query = "INSERT INTO channels (channel_name, channel_id, Subscribers, Views, total_vd, Description, Playlist_Id) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(insert_query, (row["channel_name"], row["channel_id"], row["Subscribers"], row["Views"], row["total_vd"], row["Description"], row["Playlist_Id"]))
        mydb.commit()
        st.success("Data inserted into PostgreSQL successfully!")
    except Exception as e:
        st.error(f"An error occurred: {e}")
    finally:
        cursor.close()
        mydb.close()

#sql queries

def execute_sql_query(query):
    mydb = psycopg2.connect(host='localhost', user="postgres", password="pulsar220", database="youtube_data", port="5432")
    df = pd.read_sql_query(query, mydb)
    mydb.close()
    return df


def main():
    st.title("YouTube Data Analysis App")

    # Sidebar for user input
    st.sidebar.title("User Input")
    channel_ids = st.sidebar.text_area("Enter 10 channel IDs separated by commas", "")
    channel_ids = [ch.strip() for ch in channel_ids.split(",") if ch.strip()]
    
    if st.sidebar.button("submit"):
            
        if len(channel_ids) != 10:
            st.error("Please enter exactly 10 channel IDs.")
            return

    youtube = api_connection()
    
    st.header("Inserting Data into PostgreSQL")
    create_postgres_table()
    channel_data = []
    for ch_id in channel_ids:
        st.write(f"Inserting data for channel ID: {ch_id}")
        channel_data.extend(channel_details(youtube, ch_id))
    insert_button = st.button("Insert Data")
    
    if insert_button:
            insert_into_postgres(channel_data)
            st.success("Data inserted into PostgreSQL successfully.")
        

    insert_into_postgres(channel_data)
    
    st.header("SQL Queries and Results")
    queries = [
        "SELECT Title, channel_name FROM videos;",
        "SELECT channel_name, COUNT(*) AS video_count FROM videos GROUP BY channel_name ORDER BY video_count DESC LIMIT 1;",
        "SELECT Title, channel_name FROM videos ORDER BY Vcount DESC LIMIT 10;",
        "SELECT video_id, COUNT(*) AS comment_count FROM comments GROUP BY video_id;",
        "SELECT Title, channel_name FROM videos ORDER BY likes DESC LIMIT 10;",
        "SELECT video_id, SUM(likes) AS total_likes, SUM(dislikes) AS total_dislikes FROM videos GROUP BY video_id;",
        "SELECT channel_name, SUM(Views) AS total_views FROM videos GROUP BY channel_name;",
        "SELECT DISTINCT channel_name FROM videos WHERE publish_date BETWEEN '2022-01-01' AND '2022-12-31';",
        "SELECT channel_name, AVG(duration) AS avg_duration FROM videos GROUP BY channel_name;",
        "SELECT Title, channel_name FROM videos ORDER BY comment_count DESC LIMIT 10;"
    ]

    for idx, query in enumerate(queries, start=1):
        st.subheader(f"Query {idx}:")
        st.code(query)
        df = execute_sql_query(query)
        st.write(df)

if __name__ == "__main__":
    main()
