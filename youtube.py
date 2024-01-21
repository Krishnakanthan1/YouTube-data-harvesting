import streamlit as st
import pymongo
import mysql.connector
from datetime import datetime
import pandas as pd

mongo_client = pymongo.MongoClient("mongodb+srv://krishna:1234@cluster0.ljre9m3.mongodb.net/?retryWrites=true&w=majority")
mongo_db = mongo_client["youtubedata"]

# Create a dictionary to map channel names to channel IDs
channel_id_mapping = {
    "Cheeky Cheeka": "UCUDk4lEtfnLEN9rDlj82YKg",
    "Abhinav Mukund": "UCBOVdxZA3qtdIYQ8SRQOYmQ",
    "Runner Awesome": "UC9S6NEIUCF46jnsmXAiuywA",
    "Mic Set":"UC5EQWvy59VeHPJz8mDALPxg",
    "Parithabangal":"UCueYcgdqos0_PzNOq81zAFg",
    "Gen Next Cricket Institute":"UCqyV7Bgn87c9QhNRKOI_eUw",
    "Hussain Manimegalai":"UCQS9wN4pNfQ0VNHJRPqj2OQ",
    "Vanakkam SAGO with Ramesh":"UCfOS0j5ynMCSXN9eX92f4MA",
    "Vj Siddhu Vlogs":"UCJcCB-QYPIBcbKcBQOTwhiA",
    "Sound":"UC5w2_DSNzUBBuXg32DCwEyA",
    # Add more channel name to ID mappings here
}

# Create a MySQL database connection
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Krishna@123",
    database="krishna"
)
mycursor = mydb.cursor()

# Streamlit UI
st.title("YouTube Channel Data Migration")
channel_name = st.selectbox("Select Channel Name", list(channel_id_mapping.keys()))
display_info = st.empty()  # Create a placeholder for displaying the channel information

if st.button("Migrate"):
    selected_channel_id = channel_id_mapping.get(channel_name)

    if selected_channel_id:
        # Retrieve data from MongoDB based on the selected channel ID
        channel_data = mongo_db.channel_info.find_one({"Channel_Id": selected_channel_id})

        channel_info = {
            'Channel_Id': selected_channel_id,
            'Channel_Name': channel_data['Channel_Name'],
            'Channel_Description': channel_data['Channel_Description'],
            'Subscription_Count': int(channel_data['Subscription_Count']),
            'Channel_Type': channel_data['Channel_Type'],
            'Channel_Status': channel_data['Channel_Status'],
            'Channel_Views': int(channel_data['Channel_Views'])
        }

        # Insert channel data into the channel_info table
        columns = ', '.join(channel_info.keys())
        placeholders = ', '.join(['%s'] * len(channel_info))
        query = "INSERT INTO channel_info ({}) VALUES ({})".format(columns, placeholders)
        mycursor.execute(query, tuple(channel_info.values()))

        # Migrate playlist data to MySQL
        playlist_collection = mongo_db.playlist_info
        playlists = playlist_collection.find({'channel_id': selected_channel_id})

        for playlist_data in playlists:
            playlist_info = {
                'Playlist_Id': playlist_data['playlist_id'],  # Correct the key name
                'Playlist_Name': playlist_data['playlist_name'],  # Correct the key name
                'Channel_Id': selected_channel_id
            }

            # Insert playlist data into the playlist_info table
            columns = ', '.join(playlist_info.keys())
            placeholders = ', '.join(['%s'] * len(playlist_info))
            query = "INSERT INTO playlist_info ({}) VALUES ({})".format(columns, placeholders)
            mycursor.execute(query, tuple(playlist_info.values()))

            #st.success(f"Playlists data migrated to MySQL successfully.")

            # Retrieve video information from MongoDB
            video_collection = mongo_db.video_info
            videos = video_collection.find({'playlist_id': playlist_data['playlist_id']})

            for video_data in videos:
                # Parse and format the Published_Date
                date_string = video_data['Published_Date']
                datetime_obj = datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%SZ')
                formatted_date = datetime_obj.strftime('%Y-%m-%d %H:%M:%S')

                video_info = {
                    'Video_Id': video_data['Video_Id'],
                    'Video_Name': video_data['Video_Name'],
                    'Video_Description': video_data['Video_Description'],
                    'Like_Count': int(video_data['Like_Count']),
                    'Dislike_Count': int(video_data['Dislike_Count']),
                    'Published_Date': formatted_date,  # Use the formatted date
                    'View_Count': int(video_data['View_Count']),
                    'Favorite_Count': int(video_data['Favorite_Count']),
                    'Comment_Count': int(video_data['Comment_Count']),
                    'Duration': int(video_data['Duration']),
                    'Playlist_Id': playlist_info['Playlist_Id'],  # Use the correct key name
                    'Thumbnail_URL': video_data['Thumbnail_URL'],
                    'Caption_Status': video_data['Caption_Status']
                }

                # Insert video data into the video_info table
                columns = ', '.join(video_info.keys())
                placeholders = ', '.join(['%s'] * len(video_info))
                update_values = ', '.join(f"{key} = VALUES({key})" for key in video_info.keys())
                query = f"INSERT INTO video_info ({columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_values}"
                mycursor.execute(query, tuple(video_info.values()))

                mydb.commit()



                # Retrieve comment information from MongoDB
                comment_collection = mongo_db.comment_info
                comments = comment_collection.find({'Video_Id': video_data['Video_Id']})

                for comment_data in comments:
                    # Convert the datetime format from MongoDB to MySQL
                    comment_published_at_mongodb = comment_data['Comment_Published_At']
                    comment_published_at_mysql = datetime.strptime(comment_published_at_mongodb, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")

                    comment_info = {
                        'Comment_Id': comment_data['Comment_Id'],
                        'Video_Id': video_data['Video_Id'],
                        'Comment_Author': comment_data['Comment_Author'],
                        'Comment_Text': comment_data['Comment_Text'],
                        'Comment_Published_At': comment_published_at_mysql  # Use the converted datetime
                    }

                    # Insert comment data into the comment_info table
                    columns = ', '.join(comment_info.keys())
                    placeholders = ', '.join(['%s'] * len(comment_info))
                    update_values = ', '.join(f"{key} = VALUES({key})" for key in comment_info.keys())
                    query = f"INSERT INTO comment_info ({columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_values}"

                    mycursor.execute(query, tuple(comment_info.values()))

        st.success(f"channel data migrated to MySQL successfully.")
    else:
        display_info.error("Channel not found in the database.")



mongo_client.close()
st.title("YouTube Data Analysis")

# Query to find the channels with the most number of videos
channels_with_most_videos_query ="""SELECT c.channel_name, count(v.video_id) as count_videos
FROM video_info v
LEFT JOIN playlist_info p ON v.playlist_id = p.playlist_id
LEFT JOIN channel_info c ON p.channel_id = c.channel_id
GROUP BY c.channel_name
ORDER BY count_videos Desc
Limit 1;
"""

videos_and_their_corresponding_channels = """
SELECT c.channel_name, v.Video_Name 
FROM video_info v
LEFT JOIN playlist_info p ON v.playlist_id = p.playlist_id
LEFT JOIN channel_info c ON p.channel_id = c.channel_id
GROUP BY c.channel_name , V.Video_Name
Limit 10;
"""
# Query to find the top 10 most viewed videos and their channels
top_10_most_viewed_videos_query = """
SELECT c.channel_name, v.View_count, v.Video_Name
FROM video_info v
LEFT JOIN playlist_info p ON v.playlist_id = p.playlist_id
LEFT JOIN channel_info c ON p.channel_id = c.channel_id
ORDER BY v.View_count DESC
LIMIT 10;
"""

Count_comments_and_thier_video_count= """
SELECT v.Video_Name,v.Comment_count
FROM video_info v
Limit 20;
"""
video_with_max_like="""
SELECT c.channel_name, v.Video_name,MAX(v.Like_Count) AS max_like_count
FROM video_info v
LEFT JOIN playlist_info p ON v.playlist_id = p.playlist_id
LEFT JOIN channel_info c ON p.channel_id = c.channel_id
GROUP BY c.channel_name,v.Video_name
ORDER BY max_like_count DESC
LIMIT 1;
"""
total_number_of_likes_dislikes_each_video="""
SELECT v.Video_name,v.Like_Count , Dislike_count
FROM video_info v
ORDER BY Like_Count Desc
Limit 10;
"""
channel_and_their_total_view_count="""
SELECT c.channel_name, sum(v.View_Count) AS total_view_count
FROM video_info v
LEFT JOIN playlist_info p ON v.playlist_id = p.playlist_id
LEFT JOIN channel_info c ON p.channel_id = c.channel_id
GROUP BY c.channel_name
ORDER BY total_view_count ;
"""
channels_published_video_in_2022="""
SELECT c.channel_name
FROM video_info v
LEFT JOIN playlist_info p ON v.playlist_id = p.playlist_id
LEFT JOIN channel_info c ON p.channel_id = c.channel_id
WHERE v.Published_Date BETWEEN '2022-01-01 00:00:00' AND '2023-01-01 00:00:00'
GROUP BY c.channel_name;
"""
channels_and_there_avg_durations="""
SELECT c.channel_name, Avg(v.Duration) AS avg_duration
FROM video_info v
LEFT JOIN playlist_info p ON v.playlist_id = p.playlist_id
LEFT JOIN channel_info c ON p.channel_id = c.channel_id
GROUP BY c.channel_name
ORDER BY avg_duration ;
"""
video_with_highest_comment= """
SELECT c.channel_name ,v.Video_Name, Max(Comment_Count) as max_comment_count
FROM video_info v
LEFT JOIN playlist_info p ON v.playlist_id = p.playlist_id
LEFT JOIN channel_info c ON p.channel_id = c.channel_id
GROUP BY c.channel_name,v.Video_name
ORDER BY max_comment_count DESC
LIMIT 1;
"""


# Display the channels with the most videos
if st.button("Find Channels with Most Videos"):
    st.header("Channels with the Most Videos")
    mycursor.execute(channels_with_most_videos_query)
    out = mycursor.fetchall()

    # Convert the query result to a Pandas DataFrame
    df = pd.DataFrame(out, columns=[i[0] for i in mycursor.description])

    # Display the DataFrame using Streamlit
    st.dataframe(df)

# Display the top 10 most viewed videos
if st.button("Find Top 10 Most Viewed Videos"):
    st.header("Top 10 Most Viewed Videos and Their Channels")
    mycursor.execute(top_10_most_viewed_videos_query)
    out = mycursor.fetchall()

    # Convert the query result to a Pandas DataFrame
    df = pd.DataFrame(out, columns=[i[0] for i in mycursor.description])

    # Display the DataFrame using Streamlit
    st.dataframe(df)
if st.button("videos and their corresponding channels"):
    st.header("What are the names of all the videos and their corresponding channels?")
    mycursor.execute(videos_and_their_corresponding_channels)
    out = mycursor.fetchall()

    # Convert the query result to a Pandas DataFrame
    df = pd.DataFrame(out, columns=[i[0] for i in mycursor.description])

    # Display the DataFrame using Streamlit
    st.dataframe(df)
if st.button("Count comments and thier video count"):
    st.header("How many comments were made on each video and what are their corresponding video names?")

    mycursor.execute(Count_comments_and_thier_video_count)
    out = mycursor.fetchall()

    # Convert the query result to a Pandas DataFrame
    df = pd.DataFrame(out, columns=[i[0] for i in mycursor.description])

    # Display the DataFrame using Streamlit
    st.dataframe(df)

if st.button("videos have the highest number of likes"):
    st.header("Which videos have the highest number of likes, and what are their corresponding channel names?")

    mycursor.execute(video_with_max_like)
    out = mycursor.fetchall()

    # Convert the query result to a Pandas DataFrame
    df = pd.DataFrame(out, columns=[i[0] for i in mycursor.description])

    # Display the DataFrame using Streamlit
    st.dataframe(df)

if st.button("total number of likes and dislikes for each video"):
    st.header(
        "What is the total number of likes and dislikes for each video, and what are their corresponding video names?")

    mycursor.execute(total_number_of_likes_dislikes_each_video)
    out = mycursor.fetchall()

    # Convert the query result to a Pandas DataFrame
    df = pd.DataFrame(out, columns=[i[0] for i in mycursor.description])

    # Display the DataFrame using Streamlit
    st.dataframe(df)

if st.button("total number of views for each channel"):
    st.header("What is the total number of views for each channel, and what are their corresponding channel names?")

    mycursor.execute(channel_and_their_total_view_count)
    out = mycursor.fetchall()

    # Convert the query result to a Pandas DataFrame
    df = pd.DataFrame(out, columns=[i[0] for i in mycursor.description])

    # Display the DataFrame using Streamlit
    st.dataframe(df)

if st.button("videos in the year 2022"):
    st.header("What are the names of all the channels that have published videos in the year 2022?")

    mycursor.execute(channels_published_video_in_2022)
    out = mycursor.fetchall()

    # Convert the query result to a Pandas DataFrame
    df = pd.DataFrame(out, columns=[i[0] for i in mycursor.description])

    # Display the DataFrame using Streamlit
    st.dataframe(df)
if st.button("average duration of all videos in each channel"):
    st.header(
        "What is the average duration of all videos in each channel, and what are their corresponding channel names?")

    mycursor.execute(channels_and_there_avg_durations)
    out = mycursor.fetchall()

    # Convert the query result to a Pandas DataFrame
    df = pd.DataFrame(out, columns=[i[0] for i in mycursor.description])

    # Display the DataFrame using Streamlit
    st.dataframe(df)

if st.button("videos with the highest number of comments"):
    st.header("Which videos have the highest number of comments, and what are their corresponding channel names?")

    mycursor.execute(video_with_highest_comment)
    out = mycursor.fetchall()

    # Convert the query result to a Pandas DataFrame
    df = pd.DataFrame(out, columns=[i[0] for i in mycursor.description])

    # Display the DataFrame using Streamlit
    st.dataframe(df)



# Close the MySQL connection
mydb.close()
