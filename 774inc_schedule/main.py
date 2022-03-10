from apiclient.discovery import build
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from apscheduler.schedulers.blocking import BlockingScheduler
import datetime

import all_livers
import constants


# 初期化済みかを判定する
if not firebase_admin._apps:
    # 初期済みでない場合は初期化処理を行う
    cred = credentials.Certificate("./inc-scheduler-firebase-adminsdk-xbkws-fef985c11e.json")
    firebase_admin.initialize_app(cred)

db = firestore.client()
youtube = build('youtube', 'v3', developerKey=constants.YOUTUBE_API_KEY)
scheduler = BlockingScheduler()

    

def creat_publishedAfter():
    now = datetime.datetime.now() # 2019-02-04 21:04:15.412854
    midnight = now - datetime.timedelta(days=1)
    month = '0'+str(midnight.month)+'-' if midnight.month < 10 else str(midnight.month)+'-'
    day = '0'+str(midnight.day) if midnight.day < 10 else str(midnight.day)
    hour = '0'+str(midnight.hour) if midnight.hour < 10 else str(midnight.hour)
    publishedAfter = str(midnight.year) + '-' + month + day + 'T' + hour + ':00:00Z'
    print(publishedAfter)
    return publishedAfter


def get_search():
    search_response = youtube.search().list(
        part= 'snippet',
        eventType= 'none',
        maxResults= '50',
        publishedAfter= creat_publishedAfter(),
        q= '774inc',
        type= 'video').execute()
    
    video_ids = []
    channel_ids = []
    for item in search_response['items']:
        video_ids.append(item['id']['videoId'])
        channel_ids.append(item['snippet']['channelId'])
    return video_ids, channel_ids


def get_search_upcoming():
    search_response = youtube.search().list(
        part= 'snippet',
        eventType= 'upcoming',
        maxResults= '50',
        q= '774inc',
        type= 'video').execute()
    
    video_ids = []
    channel_ids = []
    for item in search_response['items']:
        video_ids.append(item['id']['videoId'])
        channel_ids.append(item['snippet']['channelId'])
    return video_ids, channel_ids


def get_video(video_id):
    video = youtube.videos().list(
        part = 'snippet,liveStreamingDetails',
        id = video_id).execute()['items'][0]
    return video


def iso_format(scheduledStartTime):
    dt = datetime.datetime.fromisoformat(scheduledStartTime[:-1])
    return dt


print('This job is main.')
video_ids, channel_ids = get_search()
upcoming_video_ids, upcoming_channel_ids = get_search_upcoming()
video_ids += upcoming_video_ids
channel_ids += upcoming_channel_ids


for i in range(len(video_ids)):
    #チャンネルアイコンの取得と774inc所属以外のチャンネルなら以降の処理をしない
    try:
        channel_icon = all_livers.livers_list[channel_ids[i]]
    except KeyError as e:
        print('catch KeyError:', e)
        continue
    
    video = get_video(video_ids[i])
    dt = iso_format(video['liveStreamingDetails']['scheduledStartTime'])
    
    print('scheduledStartTime', dt)
    print('title', video['snippet']['title'])
    print('videoStatus', video['snippet']['liveBroadcastContent'])
    print('------------------------------------------------------------------')
    print('')
    #firestoreにデータを格納
    doc_ref = db.collection('videos').document(video_ids[i])
    doc = doc_ref.get()
    if doc.exists:
        doc_ref.update({
            'iconUrl': channel_icon,
            'channelTitle': video['snippet']['channelTitle'],
            'scheduledStartTime': dt,
            'thumbnailUrl': video['snippet']['thumbnails']['medium']['url'],
            'title': video['snippet']['title'],
            'videoStatus': video['snippet']['liveBroadcastContent'],
            'videoUrl': 'https://www.youtube.com/watch?v=' + video_ids[i],
            'channelUrl': 'https://www.youtube.com/channel/' + video['snippet']['channelId']
            })
    else:
        doc_ref.set({
            'iconUrl': channel_icon,
            'channelTitle': video['snippet']['channelTitle'],
            'scheduledStartTime': dt,
            'thumbnailUrl': video['snippet']['thumbnails']['medium']['url'],
            'title': video['snippet']['title'],
            'videoStatus': video['snippet']['liveBroadcastContent'],
            'videoUrl': 'https://www.youtube.com/watch?v=' + video_ids[i],
            'channelUrl': 'https://www.youtube.com/channel/' + video['snippet']['channelId']
            })


@scheduler.scheduled_job('interval', minutes=5)
def live_status():
    print('excute live_status')
    for video_doc in db.collection('videos').where('videoStatus', '==', 'live').get():
        try:
            video = get_video(video_doc.id)
        except IndexError as e:
            print(e)
            print('アーカイブ消された')
            #firestoreからdocumentを削除
            db.collection('videos').document(video_doc.id).delete()
            continue
        #余分にfirestoreの書き込みを行いたくないので更新があった場合だけupdateするようにしたい
        dt = iso_format(video['liveStreamingDetails']['scheduledStartTime'])
        thumbnailUrl = video['snippet']['thumbnails']['medium']['url']
        title = video['snippet']['title']
        video_status = video['snippet']['liveBroadcastContent']
        doc_ref = db.collection('videos').document(video_doc.id)
        dt_fs = str(doc_ref.get(field_paths={'scheduledStartTime'}).to_dict().get('scheduledStartTime'))
        
        if (str(dt) == dt_fs[:19] and
            thumbnailUrl == doc_ref.get(field_paths={'thumbnailUrl'}).to_dict().get('thumbnailUrl') and
            title == doc_ref.get(field_paths={'title'}).to_dict().get('title') and
            video_status == doc_ref.get(field_paths={'videoStatus'}).to_dict().get('videoStatus')):
            continue
        else:
            #firestoreにデータを格納
            doc_ref.update({
                'scheduledStartTime': dt,
                'thumbnailUrl': thumbnailUrl,
                'title': title,
                'videoStatus': video_status,
                })
        

def main_processing():
    print('This job is main.')
    video_ids, channel_ids = get_search()
    upcoming_video_ids, upcoming_channel_ids = get_search_upcoming()
    video_ids += upcoming_video_ids
    channel_ids += upcoming_channel_ids
    
    for i in range(len(video_ids)):
        #チャンネルアイコンの取得と774inc所属以外のチャンネルなら以降の処理をしない
        try:
            channel_icon = all_livers.livers_list[channel_ids[i]]
        except KeyError as e:
            print('catch KeyError:', e)
            continue
        
        video = get_video(video_ids[i])
        dt = iso_format(video['liveStreamingDetails']['scheduledStartTime'])
        
        print('iconUrl', channel_icon)
        print('channelTitle', video['snippet']['channelTitle'])
        print('scheduledStartTime', dt)
        print('thumbnailUrl', video['snippet']['thumbnails']['medium']['url'])
        print('title', video['snippet']['title'])
        print('videoStatus', video['snippet']['liveBroadcastContent'])
        print('videoUrl', 'https://www.youtube.com/watch?v=' + video_ids[i])
        print('channelUrl', 'https://www.youtube.com/channel/' + video['snippet']['channelId'])
        print('------------------------------------------------------------------')
        print('')
        #firestoreにデータを格納
        doc_ref = db.collection('videos').document(video_ids[i])
        doc = doc_ref.get()
        if doc.exists:
            doc_ref.update({
                'iconUrl': channel_icon,
                'channelTitle': video['snippet']['channelTitle'],
                'scheduledStartTime': dt,
                'thumbnailUrl': video['snippet']['thumbnails']['medium']['url'],
                'title': video['snippet']['title'],
                'videoStatus': video['snippet']['liveBroadcastContent'],
                'videoUrl': 'https://www.youtube.com/watch?v=' + video_ids[i],
                'channelUrl': 'https://www.youtube.com/channel/' + video['snippet']['channelId']
                })
        else:
            doc_ref.set({
                'iconUrl': channel_icon,
                'channelTitle': video['snippet']['channelTitle'],
                'scheduledStartTime': dt,
                'thumbnailUrl': video['snippet']['thumbnails']['medium']['url'],
                'title': video['snippet']['title'],
                'videoStatus': video['snippet']['liveBroadcastContent'],
                'videoUrl': 'https://www.youtube.com/watch?v=' + video_ids[i],
                'channelUrl': 'https://www.youtube.com/channel/' + video['snippet']['channelId']
                })
scheduler.add_job(main_processing, 'cron', minute=1)
    
    
scheduler.start()  


    
