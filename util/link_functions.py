from chat_downloader import ChatDownloader, errors
from datetime import timedelta
from functools import reduce
from itertools import islice
from joblib import Parallel, delayed
from pathlib import Path
from pyyoutube import Api
import csv
import datetime
import dateutil.parser
import gc
import isodate
import multiprocessing
import os
import pandas as pd
import psutil
import pytz
import sys
import tarfile
import time
import signal

from util import feature_check

def read_api_key(file):
    #try:
    #    with open(file, 'r') as f:
    #        return Api(api_key=f.readline())
    #except:
     #   print("Unable to read API key from key.txt")
     #   sys.exit(1)
    return Api(api_key="AIzaSyD_welXlfDLZvrQNpB40um1HdSFQPSO1vM")
        
def build_name_playlist_mapping(group, streamers):
    name_list = []
    pl_list = []
    
    targets = group.lower().split(',')
    for tgt in targets:
        if tgt in streamers.group_dict:
            for name in streamers.group_dict[tgt]:
                name_list.append(name.capitalize())
                pl_list.append(streamers.channel_ids[name])
        elif tgt in streamers.channel_ids:
            name_list.append(tgt.capitalize())
            pl_list.append(streamers.channel_ids[tgt])
    return name_list, pl_list
    
def calc_dates(start_date_a, end_date_a, timezone):
    now = datetime.datetime.now().astimezone(pytz.timezone(timezone))
    if (start_date_a and not end_date_a) or (end_date_a and not start_date_a):
        print("Start date and end date required")
        sys.exit(1)
    elif not start_date_a and not end_date_a:
        if now.weekday() == 6:
            start_date = now - timedelta(days=7)
            end_date = now - timedelta(days=1)
        else:
            start_date = now - timedelta(days=now.weekday() + 8)
            end_date = now - timedelta(days=now.weekday() + 2)
    else:
        start_split = start_date_a.split('-')
        end_split = end_date_a.split('-')
        if len(start_split) != 3 or len(end_split) != 3:
            print('Date not formatted properly')
            sys.exit(1)
        start_date = datetime.datetime(int(start_split[0]), int(start_split[1]), int(start_split[2]), 0, 0, 0)
        end_date = datetime.datetime(int(end_split[0]), int(end_split[1]), int(end_split[2]), 23, 59, 59)
    start_range = datetime.datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0, tzinfo=pytz.timezone(timezone))
    end_range = datetime.datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59, tzinfo=pytz.timezone(timezone))
    return start_range, end_range
    
def setup_output_files(directory, date_str, features, group, dump):
    base_path = directory + "/" + date_str
    
    chat_log_dir = directory + "/logs"
    Path(chat_log_dir).mkdir(parents=True, exist_ok=True)
    Path(base_path).mkdir(parents=True, exist_ok=True)
    
    csv_path_dict = {}
    csv_file_dict = {}
    csv_writer_dict = {}
    
    make_csvs = []
    if features == 'all':
        make_csvs = feature_check.feature_list
    else:
        for feat in features.split(','):
            make_csvs.append(feat)
    
    for feature in make_csvs:
        csv_path_dict[feature] = base_path + "/" + feature + "_" + group.lower() + "_" + date_str + ".csv"
        csv_file_dict[feature] = open(csv_path_dict[feature], 'w', newline='', encoding='utf-8')
        csv_writer_dict[feature] = csv.writer(csv_file_dict[feature], delimiter=',')
        csv_writer_dict[feature].writerow(['streamer', 'title', 'link'])
        csv_file_dict[feature].close()
        if dump:
            dump_file = csv_path_dict[feature].replace(".csv", "_count.csv")
            d_file = open(dump_file, 'w', newline='', encoding='utf-8')
            d_csv = csv.writer(d_file, delimiter=',')
            d_csv.writerow(['streamer', 'title', 'count'])
            d_file.close()
        
    return csv_path_dict, csv_writer_dict, base_path, chat_log_dir
    
def compress_ops(op, base_path):
    if op == "compress":
        print("Compressing logs")
        tar = tarfile.open(base_path + "/logs.tar.xz", "w:xz")
        tar.add(base_path + "/logs")
        tar.close()
        print("Compression complete")
        sys.exit(0)
    elif op == "decompress":
        print("Decompressing logs")
        tar = tarfile.open(path_pre + "/logs.tar.gz")
        tar.extractall(path=path_pre)
        tar.close()
        print("Decompression complete")
        sys.exit(0)
        
def get_playlists(name_list, pl_list, api):
    pl_idx = 0
    playlists = {}
    for name in name_list:
        playlists[name] = []
        playlists[name]= api.get_playlist_items(playlist_id=pl_list[pl_idx], count=None)
        pl_idx += 1
        
    return playlists
    
    
# chat download function
def download_chat(dir, name, title, vidId, api):
    success = False
    retryCount = 0
    while not success:
        if retryCount == 10:
            print("Unable to get complete chat data, skipping: " + name + "-" + title + " - " + vidId)
            return (vidId, None)
        try:
            print("Downloading chat for: " + name + " - " + title)
            crd = ChatDownloader()
            chat = crd.get_chat(f"https://youtube.com/watch?v={vidId}", message_types=['text_message', 'paid_message'])
        except errors.NoChatReplay:
            print("No chat replay, skipping: " + name + " - " + title)
            return (vidId, None)
        except Exception as e:
            print("Chat replay unavailable, retrying: " + name + "-" + title)
            retryCount += 1
            time.sleep(5)
            continue
            
        last_tstamp = chat.duration
        if not last_tstamp:
            print("Unable to determine video length: " + name + '-' + title)
            return (vidId, None)
        end_time = last_tstamp / 60
        vid_by_id = api.get_video_by_id(video_id=vidId)
        if not vid_by_id.items:
            print("Unable to parse video metadata: " + name + '-' + title)
            return(vidId, None)
        vid_duration = isodate.parse_duration(vid_by_id.items[0].contentDetails.duration).seconds
        dur_short = last_tstamp < vid_duration - 60
        if dur_short:
            retryCount += 1
            print("Duration mismatch (Vid: " + str(vid_duration) + "s, Chat: " + str(last_tstamp) + "s) Retrying...: " + name + "-" + title)
            time.sleep(5)
        else:
            if not chat:
                return (vidId, None)
            else:
                try:
                    return (vidId, list(chat))
                except:
                    return (vidId, None)
                
# mulitprocess chat download chunking function
def chunk_dict(data, size):
    it = iter(data)
    for i in range(0, len(data), size):
        yield {k:data[k] for k in islice(it, size)}
        
# dataframe calculations for feature detection and csv writing
def df_calc(dlist, vidId, path, offset, num_link_arg, streamer, title, dump):
    dframe = pd.DataFrame(dlist, columns=['tstamp', 'count'])
    dframe = dframe.set_index(['tstamp'])
    dframe.index = pd.to_timedelta(dframe.index, unit='s')
    dframe = dframe.groupby(['tstamp']).sum()
    if dump:
        dframe2 = dframe.copy()
        dframe2 = dframe2.resample("60S").sum()
        dframe2 = dframe2[dframe2['count'] >= 5]
        dframe2.reset_index(inplace=True)
        time_list = dframe2['tstamp'].tolist()
        time_inc_list = []
        for time in time_list:
            time_inc_list.append(time + timedelta(minutes=1))
        dframe2 = dframe2[~dframe2['tstamp'].isin(time_inc_list)]
    dframe = dframe.resample("30S").sum()
    dframe = dframe[dframe['count'] != 0]
    dframe.sort_values(by=['count'], inplace=True, ascending=False)
    dframe.reset_index(inplace=True)
    if dump:
        ff2 = open(path.replace(".csv", "_count.csv"), 'a', newline='', encoding='utf-8')
        wr2 = csv.writer(ff2, delimiter=',')
        wr2.writerow([streamer, title, str(len(dframe2))])
        ff2.close()
    if len(dframe):
        num_links = 1
        if len(dframe) > num_link_arg:
            num_links = num_link_arg
        else:
            num_links = len(dframe) - 1
        ff = open(path, 'a', newline='', encoding='utf-8')
        wr = csv.writer(ff, delimiter=',')
        has_written = False
        for i in range(0, num_links):
            if dframe.iloc[i][1] >= 5:
                tstamp = str(dframe.iloc[i][0]).split('days')[1].strip()
                index = reduce(lambda sum, d: sum * 60 + int(d), tstamp.split(":"), 0) - offset
                if index <= 0:
                    index = 1
                tstamp = "https://www.youtube.com/watch?v=" + vidId + "&t=" + str(index)
                wr.writerow([streamer, title, tstamp])
                has_written = True
        ff.close()
        
            
def create_dl_queue(playlists, start_range, end_range, timezone, chat_log_dir, ignore):
    dl_queue = {}
    ignore_list = []
    if ignore:
        ignore_list = ignore.split(',')
    for key, val in playlists.items():
        for vid in reversed(val.items):
            pub_date = vid.contentDetails.videoPublishedAt
            if (pub_date):
                pub_dt = dateutil.parser.isoparse(pub_date).astimezone(pytz.timezone(timezone))
                if (start_range <= pub_dt <= end_range):
                    if not os.path.isfile(os.path.join(chat_log_dir, vid.snippet.resourceId.videoId)):
                        if vid.snippet.resourceId.videoId not in ignore_list :
                            dl_queue[vid.snippet.resourceId.videoId] = (key, vid.snippet.title)
                        
    return dl_queue
    
def get_thread_chunk_size():
    #set max simultaneous requests to prevent getting blocked by youtube 
    if multiprocessing.cpu_count() > 6:
        thread_count = 6
    else:
        thread_count = multiprocessing.cpu_count()
    #chunk for low memory (rpi, etc)
    avail_mem = psutil.virtual_memory().free / 1024 / 1024 / 1024
    if avail_mem < 3:
        chunk_size = 30
    else:
        chunk_size = 100
        
    return thread_count, chunk_size
    
def parallel_download(thread_count, chunk_size, dl_queue, api, chat_log_dir):
    # chunk for simultaneous downloads
    for chunk in chunk_dict(dl_queue, chunk_size):
        dl_items = Parallel(n_jobs=thread_count)(delayed(download_chat)(chat_log_dir, val[0], val[1], key, api) for key, val in chunk.items())
        
        for vidId, chat in dl_items:
            if chat:
                chat_file = open(os.path.join(chat_log_dir, vidId), 'w', encoding='utf-8')
                for line in chat:
                    if 'message' in line and 'author' in line and 'time_in_seconds' in line and not 'ticker_duration' in line:
                        if line['time_in_seconds'] > 0 and line['message'] is not None:
                            msg = line['message'].replace('\n', '').replace('\t', '').replace(',', '')
                            if 'name' not in line['author']:
                                continue
                            author = line['author']['name'].replace('\n', '').replace('\t', '').replace(',', '')
                            if 'badges' in line['author']:
                                badge = line['author']['badges'][0]['title']
                            else:
                                badge = ""
                            if 'amount' in line:
                                chat_file.write(str(line['time_in_seconds']) + ',' + author + ',' + msg + ',' + line['amount'] + ',' + badge + '\n')
                            else:
                                chat_file.write(str(line['time_in_seconds']) + ',' + author + ',' + msg + ",," + badge + '\n')
                chat_file.close()
        del dl_items
        gc.collect()
    
