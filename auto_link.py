#!/usr/bin/env python3

from collections import defaultdict
from datetime import timedelta
import argparse
import dateutil.parser
import pytz
import os
import time

import streamers
from util import feature_check, link_functions as lf

if __name__ == "__main__":
    
    # arguments
    parser = argparse.ArgumentParser(description='Automatically generate clip URLs from Youtube Chat', formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('group', help="Specify group or streamer name(s) defined in streamers.py, comma seperated")
    parser.add_argument('dir', help="Output directory")
    parser.add_argument('-s', dest='start_date', help="Start date (YYYY-MM-DD). Default: 8 days ago")
    parser.add_argument('-e', dest='end_date', help="End date (YYYY-MM-DD). Default: 1 day ago")
    parser.add_argument('-n', dest='number_of_links', default=1, help="Attempt to generate the specified number of links per feature for each VOD. Default: 1")
    parser.add_argument('-f', dest='features', default="all", help="Feature type CSVs to generate (comma seperated), Default: all\n" \
        "Feature types: humor, teetee, faq, lewd, clip, fail, hic, inaff, guh, guramm, superchat")
    parser.add_argument('-o', dest='offset', default='30', help="Negative offset (in seconds) from feature grouping. Default: 30")
    parser.add_argument('-t', dest='timezone', default='Asia/Tokyo', help="tz database timezone. Default: Asia/Tokyo")
    parser.add_argument('-i', dest='ignore', help="List of YouTube video IDs to skip, comma separated")
    parser.add_argument('-c', dest='compress', action='store_true', help="Compress logs")
    parser.add_argument('-x', dest='decompress', action='store_true', help="Decompress logs")
    parser.add_argument('-d', dest='download', action='store_true', help="Download logs only")
    parser.add_argument('-u', dest='dump', action='store_true', help="Dump one-minute feature counts above 10 (event counter)")
    args = parser.parse_args()

    # date calculations
    start_range, end_range = lf.calc_dates(args.start_date, args.end_date, args.timezone)
    date_str = start_range.strftime("%Y-%m-%d")

    # setup working dir and initialize csvs
    csv_path_dict, csv_writer_dict, base_path, chat_log_dir = lf.setup_output_files(args.dir, date_str, args.features, args.group, args.dump)

    # log compression/decompression
    if args.compress:
        lf.compress_ops("compress", base_path)
    elif args.decompress:
        lf.compress_ops("decompress", base_path)

    # read api key from file
    api = lf.read_api_key("key.txt")
                
    # build name/playlist mapping
    name_list, pl_list = lf.build_name_playlist_mapping(args.group, streamers)
        
    # populate playlist dict from YT API
    playlists = lf.get_playlists(name_list, pl_list, api) 
                
    # assemble lists of video ids to download logs for, skipping logs we already have
    dl_queue = lf.create_dl_queue(playlists, start_range, end_range, args.timezone, chat_log_dir, args.ignore)

    # return thread and chunk size for available system resources
    thread_count, chunk_size = lf.get_thread_chunk_size()
                    
    # parallel log downloader
    lf.parallel_download(thread_count, chunk_size, dl_queue, api, chat_log_dir)
        
    # parse each chat file
    if not args.download:
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
                    
                    # read each chat message
                    for msg in chat:
                        if not is_log_file:
                            if 'ticker_duration' in msg:
                                continue
                            if msg['message']:
                                msg_lower = msg['message'].lower()
                            else:
                                msg_lower = ""
                            tstamp = msg['time_in_seconds']
                            superchat = 'amount' in msg
                            log_output.write(str(tstamp) + "," + msg_lower + "\n")
                        else:
                            msg_lower = msg.split(',', 2)[2].lower()
                            tstamp = int(float(msg.split(',', 1)[0]))
                            superchat = len(msg.split(',')) >= 4
                            
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
                        
                        # inaff counter
                        if feature_check.has_inaff(msg_lower): feature_dict['inaff'].append([tstamp, 1])
                        
                        # guh counter
                        if feature_check.has_guh(msg_lower): feature_dict['guh'].append([tstamp, 1])
                        
                        # superchat counter
                        if superchat: feature_dict['superchat'].append([tstamp, 1])
                        
                        if feature_check.has_tmt(msg_lower): feature_dict['tmt'].append([tstamp, 1])
                        
                        if feature_check.has_bottomleft(msg_lower): feature_dict['bottomleft'].append([tstamp, 1])
                        
                    if not is_log_file:    
                        log_output.close()
                        
                    # append zero to last time
                    for feature in feature_check.feature_list:
                        feature_dict[feature].append([last_tstamp, 0])
                    
                    # dataframe calculations and csv output
                    for feature in csv_path_dict.keys():
                        lf.df_calc(feature_dict[feature], vid.snippet.resourceId.videoId, csv_path_dict[feature], int(args.offset), int(args.number_of_links), key, vid.snippet.title, args.dump)