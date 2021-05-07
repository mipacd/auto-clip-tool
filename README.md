# Auto Clip Tool

Find interesting timestamps in VTuber VODs using Youtube chat logs. Can be used to generate clips in conjunction with [this project](https://github.com/mipacd/autoclip-vid-gen).

## Requirements
A Python 3.7+ environment with pip installed. Tested on Windows 10 under WSL and Linux. Should work with Mac OS.
 Also requires a [YouTube Data API v3 key](https://developers.google.com/youtube/registering_an_application) saved to key.txt. YouTube API keys are free but have a daily quota.
 It is recommended to set the terminal font to one that supports Japanese characters, such as "NSimSun" in Windows 10.  

## Usage
1. Install the required Python dependencies" `pip3 install -r requirements.txt`.  
2. Ensure YouTube API key is saved to key.txt.  

`usage: auto_clip.py [-h] [-s START_DATE] [-e END_DATE] [-i IGNORE] [-y] [-o] [-f] [-u] [-r] [-c] [-x] [-d] group dir`  

Required arguments:  
group - Name of group or one or more streamers (comma seperated) defined in streamers.py  
dir - Output directory. Subdirectories are created in this directory. Output CSVs are stored in "dir/start_date/group/". Chat logs are stored in "dir/start_date/group/logs".  

Optional arguments:  
-h, --help Help message  
-s START_DATE Provide a start date. If given, an end date is required. Default start date one week before yesterday in JST.  
-e END_DATE Provide an end date. If given, a start date is required. Default end date is yesterday in JST.  
-i IGNORE Provide a list of YouTube video IDs to skip. The script will freeze if there is an unaired premiere created before the end date.  

By default all types of links are generate in seperate CSVs. Use the following flags to choose which types to generate.  
-y Generate funny moments only (groupings of LOL, kusa, etc., sometimes generates links with discussions of these terms instead, or scenes about grass)  
-o Generate wholesome (teetee) links only (Wholesome moments, works best on JP language streams)  
-f Generate FAQ links only (may also generate links to "We Will Rock You" renditions)  
-u Generate lewd links only (as determined by chat)  
-r Generate requested links only (groupings of "clip this", results vary with this)

Other options:  
-c Compress logs (compress the log directory as a .tar.xz, requires tar utility)  
-x Decompress logs (decompress the log directory, requires tar utility)
-d Download logs without generating links

## Examples

1. Generate all links from 2021-05-03 to 2021-05-06 for Amelia and save to ./output directory. CSVs are in ./output/2021-05-03/amelia/.
`python3 ./auto_clip.py -s 2021-05-03 -e 2021-05-06 Amelia ./output`

2. Generate funny links only for HoloID Gen 2 from the past week and save to ./output directory. CSVs are in ./output/WEEK_AGO_MINUS_ONE/hlid2/.
`python3 ./auto-clip.py -y hlid2 ./output`

3. Generate FAQ links for Miko and Calli from 2021-04-01 to 2021-04-30 and save to ./output directory. CSVs are in ./output/2021-04-01/miko,calli/.
`python3 ./auto-clip.py -s 2021-04-01 -e 2021-04-30 -f miko,calli ./output`