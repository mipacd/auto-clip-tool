from pyyoutube import Api
import sys
sys.path.append("./chat-replay-downloader")
import chat_replay_downloader
import csv
import dateutil.parser
import datetime
import pytz
import re
import pandas as pd
import numpy as np
import os
from pathlib import Path
from slugify import slugify
from datetime import timedelta
from collections import Counter, defaultdict
import isodate
from functools import reduce
import traceback
import argparse
import tarfile
from joblib import Parallel, delayed
import time
import gc
from itertools import islice
import multiprocessing
import psutil

api = Api(api_key='YT_API_KEY')

parser = argparse.ArgumentParser(description='Automatically generate clips from Youtube Chat')
parser.add_argument('group', help="Specify group (hljpen, hljp, hlen, hlid, hs, hlall, nj, vm, njvm)")
parser.add_argument('dir', help="Output directory")
parser.add_argument('-s', dest='start_date', help="Start date (YYYY-MM-DD)")
parser.add_argument('-e', dest='end_date', help="End date (YYYY-MM-DD)")
parser.add_argument('-y', dest='funny', action='store_true', help="Generate funny links only")
parser.add_argument('-o', dest='teetee', action='store_true', help="Generate wholesome (teetee) links only")
parser.add_argument('-f', dest='elite', action='store_true', help="Generate elite (faq) links only")
parser.add_argument('-u', dest='sug', action='store_true', help="Generate suggestive links only")
parser.add_argument('-r', dest='req', action='store_true', help="Generate requested links only")
parser.add_argument('-c', dest='compress', action='store_true', help="Compress logs")
parser.add_argument('-x', dest='decompress', action='store_true', help="Decompress logs")
parser.add_argument('-d', dest='download', action='store_true', help="Download only")
args = parser.parse_args()

hljp_names=['AZki', 'Miko', 'Roboco', 'Sora', 'Suisei', 'Mel', 'Haato', 'Fubuki', 'Matsuri', 'Aki', 'Shion', 'Aqua',
    'Ayame', 'Choco', 'Choco', 'Subaru', 'Korone', 'Mio', 'Okayu', 'Noel', 'Rushia', 'Pekora', 'Flare', 'Marine',
    'Luna', 'Coco', 'Watame', 'Kanata', 'Towa', 'Lamy', 'Nene', 'Botan', 'Polka']

hljp_ids=['UU0TXe_LYZ4scaW2XMyi5_kw', 'UU-hM6YJuNYVAmUWxeIr9FeA', 'UUDqI2jOz0weumE8s7paEk6g', 'UUp6993wxpyDPHUpavwDFqgg',
    'UU5CwaMl1eIgY8h02uZw7u8A', 'UUD8HOxPs4Xvsm8H0ZxXGiBw', 'UU1CfXB_kRs3C-zaeTG3oGyg', 'UUdn5BQ06XqgXoAxIhbqw5Rg',
    'UUQ0UDLQCjY0rmuxCDE38FGg', 'UUFTLzh12_nrtzqBPsTCqenA', 'UUXTpFs_3PqI41qX2d9tL2Rw', 'UU1opHUrw8rvnsadT-iGp7Cg',
    'UU7fk0CB07ly8oSl0aqKkqFg', 'UU1suqwovbL1kzsoaZgFZLKg', 'UUp3tgHXw_HI0QMk1K8qh3gQ', 'UUvzGlP9oQwU--Y0r9id_jnA',
    'UUhAnqc_AY5_I3Px5dig3X1Q', 'UUp-5t9SrOQwXMU7iIjQfARg', 'UUvaTdHTWBGv3MKj3KVqJVCw', 'UUdyqAaZDKHXg4Ahi7VENThQ',
    'UUl_gCybOJRIgOXw6Qb4qJzQ', 'UU1DCedRgGHBdm81E1llLhOQ', 'UUvInZx9h3jC2JzsIzoOebWg', 'UUCzUftO8KOVkV4wQG1vkUvg',
    'UUa9Y57gfeY0Zro_noHRVrnw', 'UUS9uQI-jC3DE0L4IpXyvr6w', 'UUqm3BQLlJfvkTsX_hvm0UmA', 'UUZlDXzGoo7d44bwdNObFacg',
    'UU1uv2Oq6kNxgATlCiez59hw', 'UUFKOVgVbGmX65RxO3EtH3iw', 'UUAWSyEs_Io8MtpY3m-zqILA', 'UUUKD-uaobj9jiqB-VXt71mA',
    'UUK9V2B22uJYu3N7eR_BT9QA']
    
hlen_names = ['Calli', 'Kiara', 'Ina', 'Gura', 'Amelia']

hlen_ids = ['UUL_qhgtOy0dy1Agp8vkySQg', 'UUHsx4Hqa-1ORjQTh9TYDhww', 'UUMwGHR0BTZuLsmjY_NT5Pwg', 'UUoSrY_IQQVpmIRZ9Xf-y93g', 'UUyl1z3jo3XHR1riLFKG5UAg']
    
vm_names=['Pikamee', 'Tomoshika', 'Monoe']

vm_ids=['UUajhBT4nMrg3DLS-bLL2RCg', 'UU3vzVK_N_SUVKqbX69L_X4g', 'UUaFhsCKSSS821N-EcWmPkUQ']

nj_names=['Lulu', 'Hana']

nj_ids=['UU_a1ZYZ8ZTXpjg9xUY9sj8w', 'UUpJtk0myFr5WnyfsmnInP-w']

hlid_names=['Risu', 'Moona', 'Iofi', 'Civia', 'Reine', 'Anya', 'Ollie']

hlid_ids=['UUOyYb1c43VlX9rc_lT6NKQw', 'UUP0BspO_AMEe3aQqqpo89Dg', 'UUAoy6rzhSf4ydcYjJw3WoVg', 'UUgNVXGlZIFK96XdEY20sVjg', 'UUhgTyjG-pdNvxxhdsXfHQ5Q', 'UU727SQYUvx5pDDGQpTICNWg', 'UUYz_5n-uDuChHtLo7My1HnQ']

hs_names=['Miyabi', 'Kira', 'Izuru', 'Aruran', 'Rikka', 'Astel', 'Temma', 'Roberu', 'Shien', 'Oga']

hs_ids=['UU6t3-_N8A6ME1JShZHHqOMw', 'UUEzsociuFqVwgZuMaZqaCsg', 'UUZgOv3YDEs-ZnZWDYVwJdmA', 'UUKeAhJvy8zgXWbh9duVjIaQ',
    'UU9mf_ZVpouoILRY9NUIaK-w', 'UUNVEsYbiZjH5QLmGeSgTSzg', 'UUGNI4MENvnsymYjKiZwv9eg', 'UUANDOlYTJT7N5jlRC3zfzVA',
    'UUhSvpZYRPh0FvG4SJGSga3g', 'UUwL7dgTxKo8Y4RFIKWaf8gA']
    
#set group
if not args.group:
    print("Enter a group")
    sys.exit(1)
if args.group.lower() == 'hljpen':
    NAMES = hljp_names + hlen_names
    PLAYLIST_IDS = hljp_ids + hlen_ids
elif args.group.lower() == 'hljp':
    NAMES = hljp_names
    PLAYLIST_IDS = hljp_ids
elif args.group.lower() == 'hlen':
    NAMES = hlen_names
    PLAYLIST_IDS = hlen_ids
elif args.group.lower() == 'hlid':
    NAMES = hlid_names
    PLAYLIST_IDS = hlid_ids
elif args.group.lower() == 'hs':
    NAMES = hs_names
    PLAYLIST_IDS = hs_ids
elif args.group.lower() == 'hlall':
    NAMES = hljp_names + hlen_names + hlid_names + hs_names
    PLAYLIST_IDS = hljp_ids + hlen_ids + hlid_ids + hs_ids
elif args.group.lower() == 'nj':
    NAMES = nj_names
    PLAYLIST_IDS = nj_ids
elif args.group.lower() == 'vm':
    NAMES = vm_names
    PLAYLIST_IDS = vm_ids
elif args.group.lower() == 'njvm':
    NAMES = nj_names + vm_names
    PLAYLIST_IDS = nj_ids + vm_ids

#date calculations
jp_now = datetime.datetime.now().astimezone(pytz.timezone('Asia/Tokyo'))
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
start_range = datetime.datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0, tzinfo=pytz.timezone("Asia/Tokyo"))
end_range = datetime.datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59, tzinfo=pytz.timezone("Asia/Tokyo"))
date_str = start_date.strftime("%Y-%m-%d")

if not args.funny and not args.teetee and not args.elite:
    doAll = True
else:
    doAll = False

#setup working dir
if not args.dir:
    print("Output directory missing")
    sys.exit(1)
else:
    path_pre = args.dir + "/" + date_str + "/" + args.group.lower()
    
    chat_log_dir = path_pre + "/logs"
    Path(chat_log_dir).mkdir(parents=True, exist_ok=True)
    
    if doAll or args.funny:
        funny_csv_path = path_pre + "/funny_" + args.group.lower() + "_" + date_str + ".csv"
        funny_csv_file = open(funny_csv_path, 'w')
        wr_funny = csv.writer(funny_csv_file, delimiter=',')
        wr_funny.writerow(['streamer', 'title', 'link'])
        funny_csv_file.close()
    if doAll or args.teetee:
        teetee_csv_path = path_pre + "/wholesome_" + args.group.lower() + "_" + date_str + ".csv"
        teetee_csv_file = open(teetee_csv_path, 'w')
        wr_teetee = csv.writer(teetee_csv_file, delimiter=',')
        wr_teetee.writerow(['streamer', 'title', 'link'])
        teetee_csv_file.close()
    if doAll or args.elite:
        elite_csv_path = path_pre + "/elite_" + args.group.lower() + "_" + date_str + ".csv"
        elite_csv_file = open(elite_csv_path, 'w')
        wr_elite = csv.writer(elite_csv_file, delimiter=',')
        wr_elite.writerow(['streamer', 'title', 'link'])
        elite_csv_file.close()
    if doAll or args.sug:
        lewd_csv_path = path_pre + "/sug_" + args.group.lower() + "_" + date_str + ".csv"
        lewd_csv_file = open(lewd_csv_path, 'w')
        wr_lewd = csv.writer(lewd_csv_file, delimiter=',')
        wr_lewd.writerow(['streamer', 'title', 'link'])
        lewd_csv_file.close()
    if doAll or args.req:
        req_csv_path = path_pre + "/req_" + args.group.lower() + "_" + date_str + ".csv"
        req_csv_file = open(req_csv_path, 'w')
        wr_req = csv.writer(req_csv_file, delimiter=',')
        wr_req.writerow(['streamer', 'title', 'link'])
        req_csv_file.close()

#log compression/decompression
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
    
#get video playlists from yt api
pl_idx = 0
playlists = {}
jp_regex = "[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff\uff66-\uff9f]"
for name in NAMES:
    playlists[name] = []
    playlists[name]= api.get_playlist_items(playlist_id=PLAYLIST_IDS[pl_idx], count=None)
    pl_idx += 1
    
#chat download function
def download(dir, name, title, vidId):
    success = False
    retryCount = 0
    while not success:
        if retryCount == 10:
            print("Unable to get complete chat data, skipping: " + name + "-" + title + " - " + vidId)
            return False
        try:
            print("Downloading chat for: " + name + " - " + title)
            crd = chat_replay_downloader.ChatReplayDownloader()
            chat = crd.get_youtube_messages(vidId, message_type='all')
        except chat_replay_downloader.NoChatReplay:
            print("No chat replay, skipping: " + name + " - " + title)
            return False
        except Exception as e:
            print("Chat replay unavailable, retrying: " + name + "-" + title)
            retryCount += 1
            time.sleep(5)
            continue
            
        last_tstamp = None
        for line in reversed(chat):
            if 'time_in_seconds' in line:
                last_tstamp = line['time_in_seconds']
                break
        if not last_tstamp:
            print("Unable to determine last timestamp: " + name + "-" + title)
            return False
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
    
    return (vidId, chat)
    
def chunkDict(data, size):
    it = iter(data)
    for i in range(0, len(data), size):
        yield {k:data[k] for k in islice(it, size)}
        
def dfCalc(dlist, vidId, path):
    dframe = pd.DataFrame(dlist, columns=['tstamp', 'count'])
    dframe = dframe.set_index(['tstamp'])
    dframe.index = pd.to_timedelta(dframe.index, unit='s')
    dframe = dframe.groupby(['tstamp']).sum()
    dframe = dframe.resample("30S").sum()
    dframe = dframe[dframe['count'] != 0]
    dframe.sort_values(by=['count'], inplace=True, ascending=False)
    dframe.reset_index(inplace=True)
    if len(dframe):
        tstamp = str(dframe.iloc[0][0]).split('days')[1].strip()
        index = reduce(lambda sum, d: sum * 60 + int(d), tstamp.split(":"), 0) - 30
        if index <= 0:
            index = 1
        tstamp = "https://www.youtube.com/watch?v=" + vidId + "&t=" + str(index)
        ff = open(path, 'a')
        wr = csv.writer(ff, delimiter=',')
        wr.writerow([key, vid.snippet.title, tstamp])
        ff.close()
            
dl_queue = {}
for key, val in playlists.items():
    for vid in val.items:
        pub_date = vid.contentDetails.videoPublishedAt
        pub_dt = dateutil.parser.isoparse(pub_date).astimezone(pytz.timezone("Asia/Tokyo"))
        if (start_range <= pub_dt <= end_range):
            if not os.path.isfile(os.path.join(chat_log_dir, vid.snippet.resourceId.videoId)):
                ignore = []
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
                

for chunk in chunkDict(dl_queue, chunk_size):
    dl_items = Parallel(n_jobs=thread_count)(delayed(downloadChat)(val[0], val[1], key) for key, val in chunk.items())

    for vidId, chat in dl_items:
        if chat:
            chat_file = open(os.path.join(chat_log_dir, vidId), 'w')
            for line in chat:
                if line['message'] and not 'ticker_duration' in line:
                    msg = line['message'].replace('\n', '').replace('\t', '').replace(',', '')
                    author = line['author'].replace('\n', '').replace('\t', '').replace(',', '')
                    chat_file.write(str(line['time_in_seconds']) + ',' + author + ',' + msg + '\n')
            chat_file.close()
    del dl_items
    gc.collect()
    
if args.download:
    sys.exit(0)
    
for key, val in playlists.items():
    for vid in val.items:
        pub_date = vid.contentDetails.videoPublishedAt
        pub_dt = dateutil.parser.isoparse(pub_date).astimezone(pytz.timezone("Asia/Tokyo"))
        slug_str = slugify(vid.snippet.title)

        if (start_range <= pub_dt <= end_range):
            faq_list = []
            faq_list.append([timedelta(seconds=0), 0])
            tete_list = []
            tete_list.append([timedelta(seconds=0), 0])
            humor_list = []
            humor_list.append([timedelta(seconds=0), 0])
            lewd_list = []
            lewd_list.append([timedelta(seconds=0), 0])
            req_list = []
            req_list.append([timedelta(seconds=0), 0])
            faq_tstamp = ""
            tete_tstamp = ""
            humor_tstamp = ""
            lewd_tstamp = ""
            req_tstamp = ""
            is_log_file = False
            
            if not os.path.isfile(os.path.join(chat_log_dir, vid.snippet.resourceId.videoId)):
                continue
            else:
                print("Processing chat file for: " + key + " - " + vid.snippet.title)
                opened = False
                while not opened:
                    try:
                        log_file = open(chat_log_dir + '/' + vid.snippet.resourceId.videoId)
                        chat = log_file.readlines()
                    except:
                        print("Unable to open file, waiting 5 seconds...")
                        time.sleep(5)
                    else:
                        opened = True
                is_log_file = True
                last_tstamp = int(chat[-1].split(',', 1)[0])
            
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
                    tstamp = int(msg.split(',', 1)[0])
                    
                #humor counter
                has_jp = re.search(jp_regex, msg_lower)
                w_end = has_jp and msg_lower.endswith("w")
                if "草" in msg_lower or "kusa" in msg_lower or "grass" in msg_lower or "茶葉" in msg_lower or "_fbkcha" in msg_lower or w_end or msg_lower.endswith("ｗ") or "_lol" in msg_lower or "lmao" in msg_lower or "lmfao" in msg_lower or "haha" in msg_lower or "🤣" in msg_lower or "😆" in msg_lower or "jaja" in msg_lower or "笑" in msg_lower or "xd" in msg_lower or "wkwk" in msg_lower:
                    if not (key == "Coco" and "_kusa" in msg_lower):
                        humor_list.append([tstamp, 1])
                else:
                    for sub in msg_lower.split():
                        if sub.startswith('lol'):
                            humor_list.append([tstamp, 1])
                            break
                         
                #teetee counter
                if "てぇてぇ" in msg_lower or ":_tee::_tee:" in msg_lower or "tee tee" in msg_lower or "teetee" in msg_lower or "tete" in msg_lower:
                    tete_list.append([tstamp, 1])
                
                #faq counters
                if "faq" in msg_lower:
                    faq_list.append([tstamp, 1])
                    
                #lewd counter
                if "lewd" in msg_lower:
                    lewd_list.append([tstamp, 1])
                    
                #requested counter
                if "clip" in msg_lower:
                    req_list.append([tstamp, 1])
                
            if not is_log_file:    
                log_output.close()
                
            #append zero to last time
            humor_list.append([last_tstamp, 0])
            tete_list.append([last_tstamp, 0])
            faq_list.append([last_tstamp, 0])
            lewd_list.append([last_tstamp, 0])
            req_list.append([last_tstamp, 0])
            
            #dataframe stuff for finding groupings
            if doAll or args.funny:
                dfCalc(humor_list, vid.snippet.resourceId.videoId, funny_csv_path)
            if doAll or args.teetee:
                dfCalc(tete_list, vid.snippet.resourceId.videoId, teetee_csv_path)
            if doAll or args.elite:
                dfCalc(faq_list, vid.snippet.resourceId.videoId, elite_csv_path)
            if doAll or args.sug:
                dfCalc(lewd_list, vid.snippet.resourceId.videoId, lewd_csv_path)
            if doAll or args.req:
                dfCalc(req_list, vid.snippet.resourceId.videoId, req_csv_path)
                    
    
