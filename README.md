from googleapiclient.discovery import build
import googleapiclient.discovery
import pymongo as pm
import mysql.connector
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import mysql.connector  
import streamlit as st


def Api_connect():
    Api_Id="AIzaSyAvmjuS-2GfU8AWKAqYjB9h6LWEYJkj6c0"

    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name,api_version,developerKey=Api_Id)

    return youtube

youtube=Api_connect()

def get_channel_info(channel_id):
    
    request = youtube.channels().list(
            part="snippet,ContentDetails,statistics",
            id=channel_id
        )
    response = request.execute()
    
    for i in response['items']:
        data=dict(Channel_Name=i['snippet']['title'],
        Channel_Id=i['id'], 
        Subscriber=i['statistics']['subscriberCount'],
        Views=i['statistics']['viewCount'],
        Total_videos=i['statistics']['videoCount'],
        Channel_Description=i['snippet']['description'],
        Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'])

    return data
    
def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                     part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids
    
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()
    
        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                     Channel_Id=item['snippet']['channelId'],
                     Video_Id=item['id'],
                     Title=item['snippet']['title'],
                     Tags=item['snippet'].get('tags'),
                     Thumbnail=item['snippet']['thumbnails']['default']['url'],
                     Description=item['snippet'].get('description'),
                     Published_Date=item['snippet']['publishedAt'],
                     Duration=item['contentDetails']['duration'],
                     View=item.get('viewCount'),
                     Likes=item['statistics'].get('likeCount'), 
                     Comments=item['statistics'].get('commentCount'),
                     Favorite_Count=item['statistics']['favoriteCount'],
                     Definition=item['contentDetails']['definition'],
                     Caption_Status=item['contentDetails']['caption'],
                     )
            video_data.append(data)
    return video_data 


def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                         Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                         Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                         Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                         Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])

                Comment_data.append(data)

    except:
        pass

    return Comment_data
    


def get_playlist_details(channel_id):

        next_page_token=None
        All_data=[]
        while True:
        
            request=youtube.playlists().list(
                    part='snippet,contentDetails',
                    channelId=channel_id,
                    maxResults=50,
                    pageToken=next_page_token
            )
            response=request.execute()
            
            for item in response['items']:
                    data=dict(Playlist_Id=item['id'],
                             Title=item['snippet']['title'],
                             Channel_Id=item['snippet']['channelId'],
                             Channel_Name=item['snippet']['channelTitle'],
                             PublishedAt=item['snippet']['publishedAt'],
                             Video_Count=item['contentDetails']['itemCount'])
                    All_data.append(data)  
        
            next_page_token=response.get('nextPageToken')
            if next_page_token is None:
                break
            
        return All_data


myclient=pm.MongoClient("mongodb://localhost:27017/")

def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    vi_ids=get_videos_ids(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    coll1=db['channel_details']
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                      "video_information":vi_details,"comment_information":com_details})

    return "upload done"

mycon = mysql.connector.connect(
    host = "localhost",
    user = "root",
    password = "12345678"
)
mycursor = mycon.cursor()


def channels_table():

    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="12345678",
        database="Guvi_Youtube_data")
    
    mycursor = mydb.cursor()
    
    drop_query = '''drop table if exists channels'''
    mycursor.execute(drop_query)
    mydb.commit()
    
    
    try:
        sql_query = '''create table if not exists channels(Channel_Name varchar(100),
                                                    Channel_Id varchar(70) primary key,
                                                    Subscriber bigint,
                                                    Views bigint,
                                                    Total_videos int,
                                                    Channel_Description text,
                                                    Playlist_Id varchar(70)
                                                    )'''
    
        mycursor.execute(sql_query)
        mydb.commit()
    
    except:
        print("Channels table created")
    
    ch_list = []
    myclient = MongoClient("mongodb://localhost:27017/")
    db = myclient["Guvi_Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data['channel_information'])
    df = pd.DataFrame(ch_list)
    
    for index, row in df.iterrows():
            insert_query = '''insert into channels(Channel_name, Channel_Id,
                                                    Subscriber,
                                                    Views,
                                                    Total_videos,
                                                    Channel_Description,
                                                    Playlist_Id)
                                                    values(%s, %s, %s, %s, %s, %s, %s)'''
    
            values = (row['Channel_Name'],
                      row['Channel_Id'],
                      row['Subscriber'],
                      row['Views'],
                      row['Total_videos'],
                      row['Channel_Description'],
                      row['Playlist_Id'])
    
            try:
                mycursor.execute(insert_query, values)
                mydb.commit()
    
            except:
                print("Channel values are alrady inserted")

def playlist_table():

    mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="12345678",
            database="Guvi_Youtube_data")
    mycursor = mydb.cursor()
    
    
    drop_query = '''drop table if exists playlists'''
    mycursor.execute(drop_query)
    mydb.commit()
    
    
    sql_query = '''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                        Title varchar(100),
                                                        Channel_Id varchar(100),
                                                        Channel_Name varchar(100),
                                                        PublishedAt timestamp,
                                                        Video_Count int)'''
    
    mycursor.execute(sql_query)
    mydb.commit()
    
    pl_list = []
    db = myclient["Guvi_Youtube_data"]
    coll1 = db["channel_details"]
    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1 = pd.DataFrame(pl_list)
    
    for index, row in df1.iterrows():
        insert_query = '''insert into playlists (Playlist_Id, 
                                                Title, 
                                                Channel_Id,
                                                Channel_Name,
                                                PublishedAt,
                                                Video_Count)
                                                
                                                values (%s, %s, %s, %s, %s, %s)'''
    
        datetime_string = row['PublishedAt']
    
        try:
            datetime_object = datetime.strptime(datetime_string, '%Y-%m-%dT%H:%M:%SZ')
        except ValueError as e:
            print(f"Error parsing datetime string: {e}")
            continue
    
        formatted_datetime = datetime_object.strftime('%Y-%m-%d %H:%M:%S')
    
        values = (
            row['Playlist_Id'],
            row['Title'],
            row['Channel_Id'],
            row['Channel_Name'],
            formatted_datetime,
            row['Video_Count']
        )
    
      
        mycursor.execute(insert_query, values)
        mydb.commit()

def video_table():


    mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="12345678",
            database="Guvi_Youtube_data")
    mycursor = mydb.cursor()
    
    drop_query = '''drop table if exists videos'''
    mycursor.execute(drop_query)
    mydb.commit()
    
    sql_query = ''' create table if not exists videos (Channel_Name varchar(100),
                                                        Channel_Id varchar(100),
                                                        Video_Id varchar(50),
                                                        Title varchar(100),
                                                        Tags text,
                                                        Thumbnail varchar(200),
                                                        Description text,
                                                        Published_Date timestamp,
                                                        Duration varchar(100),
                                                        View bigint,
                                                        Likes bigint,
                                                        Comments int,
                                                        Favorite_Count int,
                                                        Definition varchar(10),
                                                        Caption_Status varchar(50))'''
    
    mycursor.execute(sql_query)
    mydb.commit()
    
    
    vi_list = []
    db = myclient["Guvi_Youtube_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = pd.DataFrame(vi_list)
    
    
    for index, row in df2.iterrows():
            insert_query = '''insert into videos(Channel_Name,
                                                    Channel_Id,
                                                    Video_Id,
                                                    Title,
                                                    Tags,
                                                    Thumbnail,
                                                    Description,
                                                    Published_Date,
                                                    Duration,
                                                    View,
                                                    Likes,
                                                    Comments,
                                                    Favorite_Count,
                                                    Definition,
                                                    Caption_Status
                                                )
                                                    
                                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        
            datetime_string = row['Published_Date']
            
            try:
                datetime_object = datetime.strptime(datetime_string, '%Y-%m-%dT%H:%M:%SZ')
            except ValueError as e:
                print(f"Error parsing datetime string: {e}")
                continue
        
            formatted_datetime = datetime_object.strftime('%Y-%m-%d %H:%M:%S')
    
            tags_string = ', '.join(map(str, row['Tags'])) if row['Tags'] is not None else None
    
            
            values = (row['Channel_Name'],
                      row['Channel_Id'],
                      row['Video_Id'],
                      row['Title'],
                      tags_string,
                      row['Thumbnail'],
                      row['Description'],
                      formatted_datetime,
                      row['Duration'],
                      row['View'],
                      row['Likes'],
                      row['Comments'],
                      row['Favorite_Count'],
                      row['Definition'],
                      row['Caption_Status'])
        
            mycursor.execute(insert_query, values)
            mydb.commit()


def comments_table():

    mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="12345678",
            database="Guvi_Youtube_data")
    mycursor = mydb.cursor()
    
    drop_query = '''drop table if exists comments'''
    mycursor.execute(drop_query)
    mydb.commit()
    
    sql_query = ''' create table if not exists comments (Comment_Id varchar(100) primary key,
                                                        Video_Id varchar(50),
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published timestamp
                                                        )'''
    
    mycursor.execute(sql_query)
    mydb.commit()
    
    com_list = []
    db = myclient["Guvi_Youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = pd.DataFrame(com_list)
    
    for index, row in df3.iterrows():
            insert_query = '''insert into comments (Comment_Id, 
                                                Video_Id, 
                                                Comment_Text,
                                                Comment_Author,
                                                Comment_Published)
                                                
                                                values (%s,%s,%s,%s,%s)'''
        
            datetime_string = row['Comment_Published']
        
            try:
                datetime_object = datetime.strptime(datetime_string, '%Y-%m-%dT%H:%M:%SZ')
            except ValueError as e:
                print(f"Error parsing datetime string: {e}")
                continue
            
            formatted_datetime = datetime_object.strftime('%Y-%m-%d %H:%M:%S')
            
            values = (
                row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                formatted_datetime,)
            
        
            mycursor.execute(insert_query, values)
            mydb.commit()

def tables():
    channels_table()
    playlist_table()
    video_table()
    comments_table()

    return "tables created successfully"

def show_channels_table():
    ch_list = []
    db = myclient["Guvi_Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data['channel_information'])
    df = st.dataframe(ch_list)

    return df

def show_playlist_table():
    pl_list = []
    db = myclient["Guvi_Youtube_data"]
    coll1 = db["channel_details"]
    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1 = st.dataframe(pl_list)

    return df1

def show_video_table():
    vi_list = []
    db = myclient["Guvi_Youtube_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = st.dataframe(vi_list)
    
    return df2

def show_comment_table():
    com_list = []
    db = myclient["Guvi_Youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = st.dataframe(com_list)
    
    return df3

with st.sidebar:
    st.title(":blue[Sara-Guvi-YouTube Data Harvesting and Warehousing]")
    st.header("Skill Learned ")
    st.caption("Pythob Scripting")
    st.caption("API Integration")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("MSQL")
    st.caption("Data Management using MongoDB and SQL")
    st.caption("Streamlit")

channel_id=st.text_input("Enter the channel ID")

if st.button("collect and store data"):
    ch_ids=[]
    db=myclient["Guvi_Youtube_data"]
    coll1=db['channel_details']
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
      ch_ids.append(ch_data["channel_information"]["Channel_Id"])

    if channel_id in ch_ids:
      st.success("Channel Details of the given channel id already exists")

    else:
      insert=channel_details(channel_id)
      st.success(insert)

if st.button("Migrate to sql"):
    Table=tables()
    st.success(Table)

show_table=st.radio("Select The Tablle for view",("CHANNEL","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNEL":
 show_channels_table()
 
elif show_table=="PLAYLISTS":
 show_playlist_table()  
                                               
elif show_table=="VIDEOS":
 show_video_table()  
                                               
elif show_table=="COMMENTS":
 show_comment_table()  


mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="12345678",
        database="Guvi_Youtube_data")
        
mycursor = mydb.cursor()

question=st.selectbox("Select your question",("1.What are the names of all the videos and their corresponding channels?",
                                            "2.Which channels have the most number of videos, and how many videos do they have?",
                                            "3.What are the top 10 most viewed videos and their respective channels?",
                                            "4.How many comments were made on each video, and what are their corresponding video names?",
                                            "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                            "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                            "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                            "8.What are the names of all the channels that have published videos in the year 2022?",
                                            "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                            "10.Which videos have the highest number of comments, and what are their corresponding channel names?",))

if question=="1.What are the names of all the videos and their corresponding channels?":
    query1 = '''select title as videos, channel_name as channelname from videos'''
    mycursor.execute(query1)
    
    table1 = mycursor.fetchall()
    
    mydb.commit()
    
    df = pd.DataFrame(table1, columns=["video title", "channel name"])
    st.write(df)

elif question=="2.Which channels have the most number of videos, and how many videos do they have?":
    query2 = '''select channel_name as channelname, total_videos as no_videos from channels order by total_videos desc'''
    
    mycursor.execute(query2)
    table2 = mycursor.fetchall()
    mydb.commit()
    
    df2 = pd.DataFrame(table2, columns=["channel name", "no of videos"])
    st.write(df2)

elif question=="3.What are the top 10 most viewed videos and their respective channels?":
    query3 = '''select view as view, channel_name as channelname, title as videotitle 
                from videos 
                where view is not null 
                order by view desc 
                limit 10'''
    
    mycursor.execute(query3)
    table3 = mycursor.fetchall()
    mydb.commit()
    
    df3 = pd.DataFrame(table3, columns=["view", "channel name", "video title"])
    st.write(df3)

elif question=="4.How many comments were made on each video, and what are their corresponding video names?":
    query4 = '''select comments as no_comments, title as videostitle from videos where comments is not null'''
    
    mycursor.execute(query4)
    table4 = mycursor.fetchall()
    mydb.commit()
    
    df4 = pd.DataFrame(table4, columns=["no of comments", "videotitle"])
    st.write(df4)

elif question=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5 = '''select title as videotitle,channel_name as channelname, likes as likecount from 
                videos where likes is not null order by likes desc'''
    
    mycursor.execute(query5)
    table5 = mycursor.fetchall()
    mydb.commit()
    
    df5 = pd.DataFrame(table5, columns=["videotitle",'channelname',"likecount"])
    st.write(df5)

elif question=="6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    query6 = '''select likes as likecount,title as videotitle from videos'''
    
    mycursor.execute(query6)
    table6 = mycursor.fetchall()
    mydb.commit()
    
    df6 = pd.DataFrame(table6, columns=["likecount","videotitle"])
    st.write(df6)

elif question=="7.What is the total number of views for each channel, and what are their corresponding channel names?":
    query7 = '''select channel_name as channelname, views as totalviews from channels'''
    
    mycursor.execute(query7)
    table7 = mycursor.fetchall()
    mydb.commit()
    
    df7 = pd.DataFrame(table7, columns=["channel name", "totalviews"])
    st.write(df7)

elif question=="8.What are the names of all the channels that have published videos in the year 2022?":
    query8 = '''select title as video_title,published_date as videorelease,channel_name as channelname from videos
                where extract(year from published_date)=2022'''
    
    mycursor.execute(query8)
    table8 = mycursor.fetchall()
    mydb.commit()
    
    df8 = pd.DataFrame(table8, columns=["videotitle","published_date","channelname"])
    st.write(df8)

elif question=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9 = '''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
    
    mycursor.execute(query9)
    table9 = mycursor.fetchall()
    mydb.commit()
    
    df9 = pd.DataFrame(table9, columns=["channelname","averageduration"])
      
    
    table9=[]
    for index, row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        table9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df11=pd.DataFrame(table9)
    st.write(df11)

elif question=="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10 = '''select title as videotitle,channel_name as channelname,comments from videos where comments is
                not null order by comments desc'''
    
    mycursor.execute(query10)
    table10 = mycursor.fetchall()
    mydb.commit()
    
    df10 = pd.DataFrame(table10, columns=["videotitle","channel name","comments"])
    st.write(df10)

