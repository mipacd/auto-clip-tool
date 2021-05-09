import argparse
import csv
import datetime
import subprocess
import os
import shutil

import streamers

#arguments
parser = argparse.ArgumentParser(description="AutoLink CSV to Video")
parser.add_argument('path', type=str, help="Path to AutoLink CSV")
parser.add_argument('output', type=str, help="Output video file path (MP4 file)")
parser.add_argument('--length', '-l', type=int, default=60, help="Length of each clip in seconds (default: 60)")
parser.add_argument('--streamer', '-s', type=str, help="Specify one or more streamers, comma separated or group as defined in streamers.py")
parser.add_argument("--concat", '-c', action="store_true", help="Concatenate videos into a compilation")
parser.add_argument("--desc", '-e', action="store_true", help="Write description file for compilation timestamps and channel links")
parser.add_argument("--delete", '-d', action="store_true", help="Delete video cache")
parser.add_argument("--thumb", '-t', action="store_true", help="Generate thumbnail for concatenated video, requires vcsi package")

args = parser.parse_args()

clip_dir = "clips"

if not os.path.isdir(clip_dir):
    os.mkdir(clip_dir)

if args.delete:
    shutil.rmtree(clip_dir)
    os.mkdir(clip_dir)

#open csv, make clip from each line
with open(args.path, newline='', encoding='utf-8') as csvfile:
    csvreader = csv.reader(csvfile)
    filelist = open("filelist.txt", 'w')
    desc_file = open("description.txt", 'w')
    count = 0
    streamer_tstamp = datetime.timedelta(seconds=0)
    last_streamer = ""
    
    #build streamer list
    streamer_list = []
    if args.streamer:
        arg_list = args.streamer.split()
        for item in arg_list:
            item_lower = item.lower()
            if item_lower == 'hljp':
                streamer_list += streamers.hljp
            elif item_lower == 'hlen':
                streamer_list += streamers.hlen
            elif item_lower == 'hlid':
                streamer_list += streamers.hlid
            elif item_lower == 'hstars':
                streamer_list += streamers.hstars
            elif item_lower in streamers.streamer_dict:
                streamer_list += streamers.streamer_dict[item]
            else:
                streamer_list.append(item_lower)
                
    for row in csvreader:
        full_url = row[2]
        streamer = row[0]
        title = row[1]
        url_list = []
        if full_url != 'link':
        
            #only process streamers in list if specified
            if streamer_list:
                if streamer not in streamer_list:
                    continue

            #skip covers
            if "cover" in title.lower():
                continue
                
            url = full_url.split('&')[0]
                    
            #write new streamer timestamp for YT descriptions
            if args.desc:
                if streamer != last_streamer:
                    desc_file.write(str(streamer_tstamp).split(".")[0] + " - " + streamer + '\n')
                    if streamer.lower() in streamers.channel_ids:
                        channel = streamers.channel_ids[streamer.lower()].replace("UU", "UC", 1)
                        desc_file.write(f"https://www.youtube.com/channel/{channel}\n")
                    last_streamer = streamer
                desc_file.write(title + '\n')
                desc_file.write(url + '\n')
                
            
            vid_id = url.split('=')[1]
            timecode = full_url.split('=')[2]
            filename = f"{clip_dir}/{vid_id}-{timecode}.mp4"
            url_list.append(url)
            
            #process stream if file doesn't exist
            if not os.path.exists(filename):
                proc = subprocess.Popen(f"youtube-dl --youtube-skip-dash-manifest -g \"{url}\"", stdout=subprocess.PIPE, shell=True)
                vid_strm = proc.communicate()[0]
                vid_strm = vid_strm.decode('utf-8').split('\n')
                
                #timecode adjustments for start of stream and iframe detection
                ss = 30
                if int(timecode) > 30:
                    start_time = str(datetime.timedelta(seconds=int(timecode)-30))
                else:
                    start_time = str(datetime.timedelta(seconds=int(timecode)))
                    ss = 0
                end_time = str(datetime.timedelta(seconds=int(timecode)) + datetime.timedelta(seconds=args.length))
                length = str(datetime.timedelta(seconds=args.length))
                
                proc = subprocess.Popen(f"ffmpeg -ss {start_time} -i \"{vid_strm[0]}\" -ss {start_time} -i \"{vid_strm[1]}\" -map 0:v -map 1:a -ss {str(ss)} -t {length} -c:v libx264 -r 30 -c:a aac -ar 48000 -b:a 192k -avoid_negative_ts make_zero -fflags +genpts \"{filename}\"", shell=True)
                out = proc.communicate()
                
            
            filelist.write(f"file \'{filename}\'\n")
            
            #calculate timestamp
            proc = subprocess.Popen(f"ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 {filename}", stdout=subprocess.PIPE, shell=True)
            duration = proc.communicate()[0]
            duration = float(duration.decode('utf-8'))
            streamer_tstamp += datetime.timedelta(seconds=duration)


            
    if args.concat:
        #concat files
        filelist.close()
        proc = subprocess.Popen(f"ffmpeg -f concat -segment_time_metadata 1 -i filelist.txt -vf select=concatdec_select -af aselect=concatdec_select,aresample=async=1 {args.output}", shell=True)
        out = proc.communicate()
        
        if args.thumb:
            proc = subprocess.Popen(f"vcsi {args.output} -g 2x2 -w 1280 --metadata-position hidden", shell=True)
            out = proc.communicate()
            
    filelist.close()
    os.remove("filelist.txt")
