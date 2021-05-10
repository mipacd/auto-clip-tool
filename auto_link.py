from pyyoutube import Api
import sys
from chat_replay_downloader import ChatDownloader, errors
import csv
import dateutil.parser
import datetime
import pytz
import re
import pandas as pd
import os
from pathlib import Path
from datetime import timedelta
from collections import Counter, defaultdict
import isodate
from functools import reduce
import argparse
import tarfile
from joblib import Parallel, delayed
import time
import gc
from itertools import islice
import multiprocessing
import psutil

import streamers
from util import feature_check

try:
    with open('key.txt', 'r') as f:
        api = Api(api_key=f.readline())
except:
    print("Unable to read API key from key.txt")
    sys.exit(1)

parser = argparse.ArgumentParser(description='Automatically generate clip URLs from Youtube Chat', formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('group', help="Specify group or streamer name(s) defined in streamers.py, comma seperated")
parser.add_argument('dir', help="Output directory")
parser.add_argument('-s', dest='start_date', help="Start date (YYYY-MM-DD). Default: 8 days ago")
parser.add_argument('-e', dest='end_date', help="End date (YYYY-MM-DD). Default: 1 day ago")
parser.add_argument('-n', dest='number_of_links', default=1, help="Attempt to generate the specified number of links per feature for each VOD. Default: 1")
parser.add_argument('-f', dest='features', default="all", help="Feature type CSVs to generate (comma seperated), Default: all\n" \
    "Feature types: humor, teetee, faq, lewd, clip, fail, hic")
parser.add_argument('-o', dest='offset', default='30', help="Negative offset (in seconds) from feature grouping. Default: 30")
parser.add_argument('-t', dest='timezone', default='Asia/Tokyo', help="tz database timezone. Default: Asia/Tokyo")
parser.add_argument('-i', dest='ignore', help="List of YouTube video IDs to skip, comma separated")
parser.add_argument('-c', dest='compress', action='store_true', help="Compress logs")
parser.add_argument('-x', dest='decompress', action='store_true', help="Decompress logs")
parser.add_argument('-d', dest='download', action='store_true', help="Download logs only")
args = parser.parse_args()

name_list = []
pl_list = []
    
#set group
if not args.group:
    print("Enter a group")
    sys.exit(1)
else:
    targets = args.group.lower().split(',')
    for tgt in targets:
        if tgt in streamers.group_dict:
            for name in streamers.group_dict[tgt]:
                name_list.append(name.capitalize())
                pl_list.append(streamers.channel_ids[name])
        elif tgt in streamers.channel_ids:
            name_list.append(tgt.capitalize())
            pl_list.append(streamers.channel_ids[tgt])

#date calculations
jp_now = datetime.datetime.now().astimezone(pytz.timezone(args.timezone))
if (args.start_date and not args.end_date) or (args.end_date and not args.start_date):
    print("Start date and end date required")
    sys.exit(1)
elif not args.start_date and not args.end_date:
    if jp_now.weekday() == 6:
        start_date = jp_now - timedelta(days=7)
        end_date = jp_now - timedelta(days=1)
    else:
        start_date = jp_now - timedelta(days=jp_now.weekday() + 8)
        end_date = jp_now - timedelta(days=jp_now.weekday() + 2)
else:
    start_split = args.start_date.split('-')
    end_split = args.end_date.split('-')
    if len(start_split) != 3 or len(end_split) != 3:
        print('Date not formatted properly')
        sys.exit(1)
    start_date = datetime.datetime(int(start_split[0]), int(start_split[1]), int(start_split[2]), 0, 0, 0)
    end_date = datetime.datetime(int(end_split[0]), int(end_split[1]), int(end_split[2]), 23, 59, 59)
start_range = datetime.datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0, tzinfo=pytz.timezone(args.timezone))
end_range = datetime.datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59, tzinfo=pytz.timezone(args.timezone))
date_str = start_date.strftime("%Y-%m-%d")

# setup working dir and initialize csvs
if not args.dir:
    print("Output directory missing")
    sys.exit(1)
else:
    path_pre = args.dir + "/" + date_str
    
    chat_log_dir = args.dir + "/logs"
    Path(chat_log_dir).mkdir(parents=True, exist_ok=True)
    Path(path_pre).mkdir(parents=True, exist_ok=True)
    
    csv_path_dict = {}
    csv_file_dict = {}
    csv_writer_dict = {}
    
    make_csvs = []
    if args.features == 'all':
        make_csvs = feature_check.feature_list
    else:
        for feat in args.features.split(','):
            make_csvs.append(feat)
    
    for feature in make_csvs:
        csv_path_dict[feature] = path_pre + "/" + feature + "_" + args.group.lower() + "_" + date_str + ".csv"
        csv_file_dict[feature] = open(csv_path_dict[feature], 'w', newline='', encoding='utf-8')
        csv_writer_dict[feature] = csv.writer(csv_file_dict[feature], delimiter=',')
        csv_writer_dict[feature].writerow(['streamer', 'title', 'link'])
        csv_file_dict[feature].close()

# log compression/decompression
if args.compress:
    print("Compressing logs")
    tar = tarfile.open(path_pre + "/logs.tar.xz", "w:xz")
    tar.add(path_pre + "/logs")
    tar.close()
    print("Compression complete")
    sys.exit(0)
if args.decompress:
    print("Decompressing logs")
    tar = tarfile.open(path_pre + "/logs.tar.gz")
    tar.extractall(path=path_pre)
    tar.close()
    print("Decompression complete")
    sys.exit(0)
    
# get video playlists from yt api
pl_idx = 0
playlists = {}
for name in name_list:
    playlists[name] = []
    playlists[name]= api.get_playlist_items(playlist_id=pl_list[pl_idx], count=None)
    pl_idx += 1
    
# chat download function
def download(dir, name, title, vidId):
    success = False
    retryCount = 0
    while not success:
        if retryCount == 10:
            print("Unable to get complete chat data, skipping: " + name + "-" + title + " - " + vidId)
            return False
        try:
            print("Downloading chat for: " + name + " - " + title)
            crd = ChatDownloader()
            chat = crd.get_chat(f"https://youtube.com/watch?v={vidId}", message_types=['text_message', 'paid_message'])
        except errors.NoChatReplay:
            print("No chat replay, skipping: " + name + " - " + title)
            return False
        except Exception as e:
            print("Chat replay unavailable, retrying: " + name + "-" + title)
            retryCount += 1
            time.sleep(5)
            continue
            
        last_tstamp = chat.duration
        end_time = last_tstamp / 60
        vid_by_id = api.get_video_by_id(video_id=vidId)
        vid_duration = isodate.parse_duration(vid_by_id.items[0].contentDetails.duration).seconds
        dur_short = last_tstamp < vid_duration - 60
        if dur_short:
            retryCount += 1
            print("Duration mismatch (Vid: " + str(vid_duration) + "s, Chat: " + str(last_tstamp) + "s) Retrying...: " + name + "-" + title)
            time.sleep(5)
        else:
            return chat
            
def downloadChat(name, title, vidId):
    chat = download(chat_log_dir, name, title, vidId)
    if not chat:
        return (vidId, None)
        
    return (vidId, list(chat))
    
def chunkDict(data, size):
    it = iter(data)
    for i in range(0, len(data), size):
        yield {k:data[k] for k in islice(it, size)}
        
def dfCalc(dlist, vidId, path, offset):
    dframe = pd.DataFrame(dlist, columns=['tstamp', 'count'])
    dframe = dframe.set_index(['tstamp'])
    dframe.index = pd.to_timedelta(dframe.index, unit='s')
    dframe = dframe.groupby(['tstamp']).sum()
    dframe = dframe.resample("30S").sum()
    dframe = dframe[dframe['count'] != 0]
    dframe.sort_values(by=['count'], inplace=True, ascending=False)
    dframe.reset_index(inplace=True)
    if len(dframe):
        num_links = 1
        if len(dframe) > int(args.number_of_links):
            num_links = int(args.number_of_links)
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
                wr.writerow([key, vid.snippet.title, tstamp])
                has_written = True
        ff.close()
        if not has_written:
            os.remove(path)
            
# assemble lists of video ids to download logs for, skipping logs we already have
dl_queue = {}
for key, val in playlists.items():
    for vid in reversed(val.items):
        pub_date = vid.contentDetails.videoPublishedAt
        pub_dt = dateutil.parser.isoparse(pub_date).astimezone(pytz.timezone(args.timezone))
        if (start_range <= pub_dt <= end_range):
            if not os.path.isfile(os.path.join(chat_log_dir, vid.snippet.resourceId.videoId)):
                #ignore specified vidIDs, script will freeze if premiere is given in pre-chat mode
                ignore = []
                if args.ignore:
                    ignore = args.ignore.split(',')
                if vid.snippet.resourceId.videoId not in ignore :
                    dl_queue[vid.snippet.resourceId.videoId] = (key, vid.snippet.title)
                
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
                
# chunk for simultaneous downloads
for chunk in chunkDict(dl_queue, chunk_size):
    dl_items = Parallel(n_jobs=thread_count)(delayed(downloadChat)(val[0], val[1], key) for key, val in chunk.items())

    for vidId, chat in dl_items:
        if chat:
            chat_file = open(os.path.join(chat_log_dir, vidId), 'w', encoding='utf-8')
            for line in chat:
                if 'message' in line and 'author' in line and not 'ticker_duration' in line:
                    if line['time_in_seconds'] > 0:
                        msg = line['message'].replace('\n', '').replace('\t', '').replace(',', '')
                        author = line['author']['name'].replace('\n', '').replace('\t', '').replace(',', '')
                        chat_file.write(str(line['time_in_seconds']) + ',' + author + ',' + msg + '\n')
            chat_file.close()
    del dl_items
    gc.collect()
    
# stop here if download only is set
if args.download:
    sys.exit(0)
    
# parse each chat file
for key, val in playlists.items():
    for vid in reversed(val.items):
        pub_date = vid.contentDetails.videoPublishedAt
        pub_dt = dateutil.parser.isoparse(pub_date).astimezone(pytz.timezone(args.timezone))

        if (start_range <= pub_dt <= end_range):
        
            feature_dict = defaultdict(list)
            for feature in feature_check.feature_list:
                feature_dict[feature].append([timedelta(seconds=0), 0])
           
            is_log_file = False
            
            if not os.path.isfile(os.path.join(chat_log_dir, vid.snippet.resourceId.videoId)):
                continue
            else:
                print("Processing chat file for: " + key + " - " + vid.snippet.title)
                opened = False
                while not opened:
                    try:
                        log_file = open(chat_log_dir + '/' + vid.snippet.resourceId.videoId, encoding='utf-8')
                        chat = log_file.readlines()
                    except:
                        print("Unable to open file, waiting 5 seconds...")
                        time.sleep(5)
                    else:
                        opened = True
                is_log_file = True
                last_tstamp = int(float(chat[-1].split(',', 1)[0]))
            
            for msg in chat:
                if not is_log_file:
                    if 'ticker_duration' in msg:
                        continue
                    if msg['message']:
                        msg_lower = msg['message'].lower()
                    else:
                        msg_lower = ""
                    tstamp = msg['time_in_seconds']
                    log_output.write(str(tstamp) + "," + msg_lower + "\n")
                else:
                    msg_lower = msg.split(',', 2)[2].lower()
                    tstamp = int(float(msg.split(',', 1)[0]))
                    
                # humor counter
                if feature_check.has_humor(msg_lower, key): feature_dict['humor'].append([tstamp, 1])
                         
                # teetee counter
                if feature_check.has_teetee(msg_lower): feature_dict['teetee'].append([tstamp, 1])
                
                # faq counter
                if feature_check.has_faq(msg_lower): feature_dict['faq'].append([tstamp, 1])
                    
                # lewd counter
                if feature_check.has_lewd(msg_lower): feature_dict['lewd'].append([tstamp, 1])
                    
                # requested clip counter
                if feature_check.has_clip(msg_lower): feature_dict['clip'].append([tstamp, 1])
                    
                # fail counter
                if feature_check.has_fail(msg_lower): feature_dict['fail'].append([tstamp, 1])
                    
                # hic counter
                if feature_check.has_hic(msg_lower): feature_dict['hic'].append([tstamp, 1])
                
            if not is_log_file:    
                log_output.close()
                
            # append zero to last time
            for feature in feature_check.feature_list:
                feature_dict[feature].append([last_tstamp, 0])
            
            # dataframe calculations and csv output
            for feature in make_csvs:
                dfCalc(feature_dict[feature], vid.snippet.resourceId.videoId, csv_path_dict[feature], int(args.offset))