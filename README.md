# YouTube-data-harvesting


YouTube Data Harvesting and Warehousing

Problem Statement:
      The problem statement is to create a Streamlit application that allows users to access and analyse data from multiple YouTube channels.

Steps Approached:
•	Created a personalised YouTube API key, to extract data from YouTube.
•	Understood the data structure of YouTube. i.e. every channel has a unique channel id. By taking the channel id we can get the details of that particular channel, playlist, video details
•	Channel Information:
Each YouTube channel has a unique channel ID.
You can retrieve details about a specific channel by using its channel ID.
•	Playlist Information:
From the channel information, you can obtain the playlist ID(s).
Playlists are collections of videos grouped together.
•	Video Information:
By iterating through the channel's videos or playlists, you can obtain video IDs.
With the video ID, you can retrieve detailed information about a specific video, including:
Number of likes and dislikes,
Comment count,
Video duration,
Posting date.
•	After extracting data and storing it in MongoDB. Then migrating the data from MongoDB to MySQL workbench.
•	Finally, doing EDA in data and displaying the insights via Streamlit. 

 
