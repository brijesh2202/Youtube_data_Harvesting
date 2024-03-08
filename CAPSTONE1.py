from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st
from datetime import datetime

def Api_connection():
    
    Api_id = "AIzaSyD9dI1k-q0r9Y_7NneVtkUcGMkvJP90KJY"

    api_servicename = "youtube"

    
    api_version = "v3"

    
    youtube = build(api_servicename, api_version, developerKey=Api_id)
    return youtube
youtube=Api_connection()

#THE BELOW FUNCTION IS USED TO GET CHANNEL INFO FROM YOUTUBE API
def get_channel_creds(channel_id):
    request = youtube.channels().list(
        part="snippet, contentDetails, statistics",
        id=channel_id
    )
    response = request.execute()

    # THIS CODE EXTRACTS ONLY THE GIVEN RELAVANT INFO
    for item in response['items']:
        channel_info = {
            "Channel Name": item["snippet"]["title"],
            "Custom url for the channel": item["snippet"]["customUrl"],
            "Channel ID": item["id"],
            "Subscriber Count": item["statistics"]["subscriberCount"],
            "Total Views": item["statistics"]["viewCount"],
            "Total Video Count": item["statistics"]["videoCount"],
            "Channel Description": item["snippet"]["description"],
            "Playlist ID of the channel":item["contentDetails"]["relatedPlaylists"]["uploads"]
            
        }
        return channel_info
    
def get_total_videoid(channel_id,playlist_id):
    response1 = youtube.playlistItems().list(
        part='snippet',
        playlistId=playlist_id,
        maxResults=50
    ).execute()

    video_ids = []

    for item in response1['items']:
        video_ids.append(item['snippet']['resourceId']['videoId'])

    
    
    return video_ids

def get_video_single_info(Video_ids):
    video_data = []  # Initialize an empty list to store video data

   

    for video_id in Video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response1 = request.execute()

        for item in response1["items"]:
            data = {
                'Channel_Name': item['snippet']['channelTitle'],
                'Channel_Id': item['snippet']['channelId'],
                'Video_Id': item['id'],
                'Title': item['snippet']['title'],
                'Tags': item['snippet'].get('tags', []),
                'Thumbnail': item['snippet']['thumbnails']['default']['url'],
                'Description': item['snippet']['description'],
                'Published_Date': item['snippet']['publishedAt'],
                'Duration': item['contentDetails']['duration'],
                'Views': item['statistics']['viewCount'],
                'Likes': item['statistics']['likeCount'],
                'Comments': item['statistics']['commentCount'],
                'Favorite_Count': item['statistics']['favoriteCount'],
                'Definition': item['contentDetails']['definition'],
                'Caption_Status': item['contentDetails']['caption']
            }
            video_data.append(data)
    
    return video_data


def get_comment_user_info(Video_ids):
        Comment_data=[]
        try:
                for video_id in Video_ids:
                        request = youtube.commentThreads().list(
                                part="snippet",
                                videoId=video_id,
                                maxResults=50
                        )
                        response1=request.execute()
                        for item in response1['items']:
                                data={
                                        'Comment_id':item['snippet']['topLevelComment']['id'],
                                        'Video_ID':item['snippet']['topLevelComment']['snippet']['videoId'],
                                        'Comment_Text':item['snippet']['topLevelComment']['snippet']['textDisplay'],
                                        'Comment_Author':item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                        'Comment_Published':item['snippet']['topLevelComment']['snippet']['publishedAt']
                                        

                                }
                                Comment_data.append(data)
        
        except:
                pass
        return Comment_data

def get_total_playlist_details(channel_id):
    next_page_token = None
    Total_data = []

    while True:
        request = youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,  # Add a comma here
            maxResults=50,
            pageToken=next_page_token
        )
        response1 = request.execute()

        for item in response1['items']:
            data = {
                'Playlist_Id': item['id'],
                'Title': item['snippet']['title'],
                'Channel_Id': item['snippet']['channelId'],
                'Channel_Name': item['snippet']['channelTitle'],
                'PublishedAt': item['snippet']['publishedAt'],
                'Video_Count': item['contentDetails']['itemCount']
            }
            Total_data.append(data)

        next_page_token = response1.get('nextPageToken')
        if next_page_token is None:
            break

    return Total_data

#ESATBLISH CONNECTION WITH MONGODB
    #client=pymongo.MongoClient("mongodb://localhost:27017/")
#db=client["Youtube_data"]

import pymongo
client=pymongo.MongoClient("mongodb://localhost:27017")
db=client["Youtube_data"] 


def channel_details(channel_id):
    ch_creds = get_channel_creds(channel_id)
    pl_details = get_total_playlist_details(channel_id)
    video_id = get_total_videoid(channel_id)
    video_details = get_video_single_info(video_id)
    com_details = get_comment_user_info(video_id)

    coll1 = db["channel_details2"]  # CREATED A NEW TABLE BCUZ THE OLD TABLE WAS NOT ACCESSIBLE 
    coll1.insert_one({
        "channel_info": ch_creds,
        "playlist_info": pl_details,
        "video_information": video_details,
        "comment_information": com_details
    })
    return "document uploaded"

#CHANNELS TABLE
def channels_table():
    # Define the connection parameters
    connection_params = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 'Lolptw@123',
        'database': 'youtube_details'
    }

    try:
        # Connect to the MySQL server
        connection = mysql.connector.connect(**connection_params)

        # Create a cursor object to execute SQL queries
        cursor = connection.cursor()

        # Drop the 'channels' table if it exists
        drop_query = '''DROP TABLE IF EXISTS channels'''
        cursor.execute(drop_query)
        connection.commit()

        # Create the 'channels' table
        create_query = '''
            CREATE TABLE IF NOT EXISTS channels (
                Channel_Name VARCHAR(100),
                Channel_Id VARCHAR(80) PRIMARY KEY,
                Subscribers BIGINT,
                Views BIGINT,
                Total_Videos INT,
                Channel_Description TEXT,
                Playlist_Id VARCHAR(80)
            )
        '''
        cursor.execute(create_query)
        connection.commit()

        print("Table 'channels' created successfully")

        # Close the cursor
        cursor.close()

    except mysql.connector.Error as error:
        print("MySQL Error:", error)
    except Exception as e:
        print("Error:", e)

    finally:
        # Close the connection
        if 'connection' in locals() and connection.is_connected():
            connection.close()


    # Fetch data from MongoDB and insert into MySQL
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client["Youtube_data"]
    coll1 = db["channel_details2"]
    ch_list = []

    for ch_creds in coll1.find({}, {"_id": 0, "channel_info": 1}):
        ch_list.append(ch_creds["channel_info"])

    df = pd.DataFrame(ch_list)

    try:
        # Reconnect to MySQL server
        connection = mysql.connector.connect(**connection_params)
        cursor = connection.cursor()

        for index, row in df.iterrows():
            insert_query = '''INSERT INTO channels(Channel_Name,
                                                    Channel_Id,
                                                    Subscribers,
                                                    Views,
                                                    Total_Videos,
                                                    Channel_Description,
                                                    Playlist_Id)
                                                VALUES (%s, %s, %s, %s, %s, %s, %s)'''
            values = (row['Channel Name'],
                    row['Channel ID'],
                    row['Subscriber Count'],
                    row['Total Views'],
                    row['Total Video Count'],
                    row['Channel Description'],
                    row['Playlist ID of the channel'])
            cursor.execute(insert_query, values)
            connection.commit()

        print("Data inserted successfully")

    except mysql.connector.Error as error:
        print("MySQL Error:", error)
    except Exception as e:
        print("Error:", e)

    finally:
        # Close the cursor and connection
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

#PLAYLIST TABLE
from datetime import datetime

mongo_client = pymongo.MongoClient('mongodb://localhost:27017/')
db = mongo_client["Youtube_data"]
coll1 = db["channel_details2"]


pl_list = []
for pl_data in coll1.find({}, {"_id": 0, "playlist_info": 1}): # RETRIVE DATA FROM MONGO DB TO ESTABLISH DF
    for playlist in pl_data["playlist_info"]:
        pl_list.append(playlist)

df1 = pd.DataFrame(pl_list)
def playlist_table():
    
    connection_params = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 'Lolptw@123',
        'database': 'youtube_details'
    }

    try:
        connection = mysql.connector.connect(**connection_params)
        cursor = connection.cursor()

        # Drop the 'playlists' table if it exists
        drop_query = '''DROP TABLE IF EXISTS playlists'''
        cursor.execute(drop_query)
        connection.commit()

        
        create_query = '''
            CREATE TABLE IF NOT EXISTS playlists (
                Playlist_Id varchar(100) PRIMARY KEY,
                Title varchar(100),
                Channel_Id varchar(100),
                Channel_Name varchar(100),
                PublishedAt TIMESTAMP,
                Video_Count INT
            )'''
        cursor.execute(create_query)
        connection.commit()

        print("Table 'playlists' created successfully")

        # Insert data into MySQL table
        for index, row in df1.iterrows():
            published_at = datetime.strptime(row['PublishedAt'], '%Y-%m-%dT%H:%M:%SZ')
            published_at_formatted = published_at.strftime('%Y-%m-%d %H:%M:%S')

            insert_query = '''INSERT INTO playlists(Playlist_id,
                                                Title,
                                                Channel_Id,
                                                Channel_Name,
                                                PublishedAt,
                                                Video_Count)
                            VALUES(%s, %s, %s, %s, %s, %s)'''

            values = (row['Playlist_Id'],
                    row['Title'],
                    row['Channel_Id'],
                    row['Channel_Name'],
                    published_at_formatted,
                    row['Video_Count'])

            cursor.execute(insert_query, values)
            connection.commit()

    except mysql.connector.Error as error:
        print("Failed to create table or insert data:", error)

    finally:
        # Close the cursor and connection
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

# VIDEO DETAILS TABLE
from datetime import datetime
def video_details():
    # MongoDB connection
    mongo_client = pymongo.MongoClient('mongodb://localhost:27017/')
    db = mongo_client["Youtube_data"]
    coll1 = db["channel_details2"]

    # Retrieve data from MongoDB and create DataFrame
    vi_list = []
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for video_info in vi_data["video_information"]:
            vi_list.append(video_info)

    df2 = pd.DataFrame(vi_list)

    # MySQL connection
    connection_params = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 'Lolptw@123',
        'database': 'youtube_details'
    }

    try:
        connection = mysql.connector.connect(**connection_params)
        cursor = connection.cursor()

        
        create_query = '''CREATE TABLE IF NOT EXISTS videos(
            Channel_Name varchar(100),
            Channel_Id varchar(100),
            Video_Id varchar(30) PRIMARY KEY,
            Title varchar(150),
            Tags TEXT,
            Thumbnail varchar(200),
            Description TEXT,
            Published_Date TIMESTAMP,
            Duration VARCHAR(20),
            Views BIGINT,
            Comments INT,
            Favorite_Count INT,
            Definition VARCHAR(10),
            Caption_Status VARCHAR(50)
        )'''
        cursor.execute(create_query)
        
        for index, row in df2.iterrows():
            published_Date = datetime.strptime(row['Published_Date'], '%Y-%m-%dT%H:%M:%SZ')
            published_Date_formatted = published_Date.strftime('%Y-%m-%d %H:%M:%S')

            # Convert list of tags to a comma-separated string
            tags_str = ','.join(row['Tags'])

            insert_query = '''INSERT INTO videos(
                Channel_Name,
                Channel_Id,
                Video_Id,
                Title,
                Tags,
                Thumbnail,
                Description,
                Published_Date,
                Duration,
                Views,
                Comments,
                Favorite_Count,
                Definition,
                Caption_Status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

            tags_str = ','.join(row['Tags'])
            thumbnail_str = str(row['Thumbnail'])
            thumbnail_str = thumbnail_str[:255]

            values = (
                row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Title'],
                tags_str,  # Use the string of tags instead of the list
                thumbnail_str,
                row['Description'],
                published_Date_formatted,
                row['Duration'],
                row['Views'],
                row['Comments'],
                row['Favorite_Count'],
                row['Definition'],
                row['Caption_Status']
            )
            print("Values tuple:", values)

            cursor.execute(insert_query, values)
            connection.commit()

    except mysql.connector.Error as error:
        print("Failed to insert data into MySQL:", error)

    finally:
        # Close the cursor and connection
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

# Call the function
video_details() 


#COMMENTS TABLE
from datetime import datetime
def comment_details():
    
    mongo_client = pymongo.MongoClient('mongodb://localhost:27017/')
    db = mongo_client["Youtube_data"]
    coll1 = db["channel_details2"]

    
    comment_list = []
    for comment_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        for comment_info in comment_data["comment_information"]:
            comment_list.append(comment_info)

    df_comments = pd.DataFrame(comment_list)

    
    connection_params = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 'Lolptw@123',
        'database': 'youtube_details' 
    }

    try:
        connection = mysql.connector.connect(**connection_params)
        cursor = connection.cursor()

        
        create_query = '''CREATE TABLE IF NOT EXISTS comments (
            Comment_id varchar(100) PRIMARY KEY,
            Video_Id varchar(30),
            Comment_Text TEXT,
            Comment_Author varchar(100),
            Comment_Published TIMESTAMP
        )'''
        cursor.execute(create_query)

        for index, row in df_comments.iterrows():
            comment_published = datetime.strptime(row['Comment_Published'], '%Y-%m-%dT%H:%M:%SZ')
            comment_published_formatted = comment_published.strftime('%Y-%m-%d %H:%M:%S')

            insert_query = '''INSERT INTO comments (
                Comment_id,
                Video_ID,
                Comment_Text,
                Comment_Author,
                Comment_Published
            ) VALUES (%s, %s, %s, %s, %s)'''

            values = (
                row['Comment_id'],
                row['Video_ID'],
                row['Comment_Text'],
                row['Comment_Author'],
                comment_published_formatted
            )

            cursor.execute(insert_query, values)
            connection.commit()

    except mysql.connector.Error as error:
        print("Failed to insert data into MySQL:", error)

    finally:
        # Close the cursor and connection
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()


comment_details()

#STREAMLIT PART
def tables():
    channels_table()
    playlist_table()
    video_details()
    comment_details()

    return "Tables are created"

def show_channel_table():
    ch_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_info": 1}):
        ch_list.append(ch_data["channel_info"])
    df = pd.DataFrame(ch_list)
    return df

def show_playlists_table():
    pl_list = []
    for pl_data in coll1.find({}, {"_id": 0, "playlist_info": 1}):
        for playlist in pl_data["playlist_info"]:
            pl_list.append(playlist)

    df1 = pd.DataFrame(pl_list)
    return df1

def show_video_details():
    vi_list = []
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for video_info in vi_data["video_information"]:
            vi_list.append(video_info)

    df2 = pd.DataFrame(vi_list)
    return df2

def show_comment_list():
    comment_list = []
    for comment_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        for comment_info in comment_data["comment_information"]:
            comment_list.append(comment_info)

    df_comments = pd.DataFrame(comment_list)
    return df_comments

with st.sidebar:
    st.title(":green[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("CONTEXT PAGE")
    st.caption("Python Programming")
    st.caption("Data Collection")
    st.caption("API INTEGRATION")
    st.caption("Data manipulation using MongoDB and MySql")

channel_id=st.text_input("Enter the Channel ID")

if st.button("collect_and_store_data"):
    ch_ids=[]
    db=client["Youtube_data"]
    coll1=db["channel_details2"]
    for ch_data in coll1.find({},{"_id":0,"channel_info":1}):
            ch_ids.append(ch_data["channel_info"]["Channel ID"])

    if channel_id in ch_ids:
         st.success("Channel details of the given channel id already exists")
    else:
         insert=channel_details(channel_id)
         st.success(insert)

    if st.button("Migrate to SQL"):
         Table=tables()
         st.sucess(Table)

    show_table=st.radio("SELECT THE TABLE FOR ANALYSIS",("CHANNELS","PLAYLIST","VIDEOS","COMMENTS"))

    if show_table=="CHANNELS":
         show_channel_table()
    
    elif show_table=="PLAYLIST":
         show_channel_table()

    elif show_table=="VIDEOS":
         show_channel_table()

    elif show_table=="COMMENTS":
         show_channel_table()




# Streamlit app
question = st.selectbox("Select your question", (
    "1. All the videos and the channel name",
    "2. channels with most number of videos",
    "3. 10 most viewed videos",
    "4. comments in each videos",
    
    "5. views of each channel",
    "6. videos published in the year of 2022",
    "7. average duration of all videos in each channel",
    "8. videos with highest number of comments"
))

#QUESTIONS
import mysql.connector
import pandas as pd
import streamlit as st  # Import Streamlit


connection_params = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'Lolptw@123',
    'database': 'youtube_details'
}


#question = "1. All the videos and the channel name"

try:
    # Connect to MySQL server
    connection = mysql.connector.connect(**connection_params)
    
    # Create cursor object to execute SQL queries
    cursor = connection.cursor()

    
    if question == "1. All the videos and the channel name":
        query1 = '''SELECT title AS videos, channel_name AS channelname FROM videos'''
        cursor.execute(query1)
        t1 = cursor.fetchall()
        df = pd.DataFrame(t1, columns=["Video Title", "Channel Name"])
        st.write(df)

    elif question == "2. Channels with most number of videos":
        query2 = '''SELECT channel_name AS channelname, total_videos as no_videos FROM channels
                    ORDER BY total_videos DESC'''
        cursor.execute(query2)
        t2 = cursor.fetchall()
        df2 = pd.DataFrame(t2, columns=["Channel name", "No of videos"])
        st.write(df2)

    elif question == "3. 10 most viewed videos":
        
        query3 = '''SELECT views AS views, channel_name AS channelname, title AS videotitle 
                    FROM videos
                    WHERE views IS NOT NULL
                    ORDER BY views DESC
                    LIMIT 10'''
        cursor.execute(query3)
        t3 = cursor.fetchall()
        df3 = pd.DataFrame(t3, columns=["views", "channel name", "videotitle"])
        st.write(df3)

    elif question == "4. comments in each videos":
        
        query4 = '''SELECT comments AS no_comments, title AS videotitle 
                    FROM videos 
                    WHERE comments IS NOT NULL'''
        cursor.execute(query4)
        t4 = cursor.fetchall()
        df4 = pd.DataFrame(t4, columns=["no of comments", "videotitle"])
        st.write(df4)

    elif question == "5. views of each channel":
        query5 = '''SELECT channel_name AS channelname, views AS totalviews 
                    FROM channels'''
        cursor.execute(query5)
        t5 = cursor.fetchall()
        df5 = pd.DataFrame(t5, columns=["channel name", "totalviews"])
        st.write(df5)

    elif question == "6.videos punlished in the year of 2022":
        query6 = '''SELECT title AS video_title, published_date AS videorelease, channel_name AS channelname
                    FROM videos
                    WHERE extract(YEAR from published_date) = 2022'''
                    
        cursor.execute(query6)
        t6 = cursor.fetchall()
        df6 = pd.DataFrame(t6, columns=["videotitle", "published_date", "channelname"])
        st.write(df6)

    elif question == "7.average duration of all videos in each channel":
        query7 = '''SELECT Channel_Name AS channelname, AVG(Duration) AS averageduration
                    FROM videos
                    GROUP BY channel_name'''
                
        cursor.execute(query7)
        t7 = cursor.fetchall()
        df7 = pd.DataFrame(t7, columns=["channelname", "averageduration"])

        T7=[]
        for index,row in df7.iterrows():
            channel_title=row["channelname"]
            average_duration=row["averageduration"]
            average_duration_str=str(average_duration)
            T7.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
        df1=pd.DataFrame(T7) 
        st.write(df1)

    elif question == "8.videos with the highest number of comments":
        query8 = '''SELECT Title AS videotitle, Channel_Name AS channelname, Comments AS comments 
                    FROM videos
                    WHERE Comments IS NOT NULL
                    ORDER BY Comments DESC'''
                    
        cursor.execute(query8)
        t8 = cursor.fetchall()
        df8 = pd.DataFrame(t8, columns=["Video Title", "Channel Name", "Comments"])
        st.write(df8)

except mysql.connector.Error as error:
    st.error("Error executing query:", error)

finally:
    # Close the cursor and connection
    if 'cursor' in locals():
        cursor.close()
    if 'connection' in locals() and connection.is_connected():
        connection.close()







    
