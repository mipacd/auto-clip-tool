# Auto Clip Tools

Find interesting timestamps in VTuber VODs using Youtube chat logs and use them to generate video clips.  

# Auto-Link

Use to generate links from YouTube chat logs.

## Requirements
A Python 3.7+ environment with pip installed. Tested on Windows 10 and Linux. Should work with Mac OS.
 Also requires a [YouTube Data API v3 key](https://developers.google.com/youtube/registering_an_application) saved to key.txt. YouTube API keys are free but have a daily quota.
 It is recommended to set the terminal font to one that supports Japanese characters, such as "NSimSun" in Windows 10.  

## Usage
1. Install the required Python dependencies" `pip3 install -r requirements.txt`.  
2. Ensure YouTube API key is saved to key.txt.  

`usage: auto_clip.py [-h] [-s START_DATE] [-e END_DATE] [-i IGNORE] [-y] [-o] [-f] [-u] [-r] [-c] [-x] [-d] group dir`  

Required arguments:  
group - Name of group or one or more streamers (comma seperated) defined in streamers.py  
dir - Output directory. Subdirectories are created in this directory. Output CSVs are stored in "dir/start_date/". Chat logs are stored in "dir/start_date/logs".  

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
-n Number of links to attempt to generate of each type. Less may be generated depending on number of chat regions of interest. Manual inspection is recommended to ensure links don't overlap.  
-c Compress logs (compress the log directory as a .tar.xz, requires tar utility)  
-x Decompress logs (decompress the log directory, requires tar utility)
-d Download logs without generating links

## Examples

1. Generate all links from 2021-05-03 to 2021-05-06 for Amelia and save to ./output directory. CSVs are in ./output/2021-05-03/.
`python3 ./auto_link.py -s 2021-05-03 -e 2021-05-06 Amelia ./output`

2. Generate funny links only for HoloID Gen 2 from the past week and save to ./output directory. CSVs are in ./output/WEEK_AGO_MINUS_ONE/.
`python3 ./auto_link.py -y hlid2 ./output`

3. Generate FAQ links for Miko and Calli from 2021-04-01 to 2021-04-30 and save to ./output directory. CSVs are in ./output/2021-04-01/.
`python3 ./auto_link.py -s 2021-04-01 -e 2021-04-30 -f miko,calli ./output`


# Auto-Clip
Generate video clips / compilations from Auto-Link datasets. Requires Python (not if using binary), youtube-dl and ffmpeg.(in PATH).
Optionally requires the vcsi Python package for thumbnail generation. [This YouTube channel](https://www.youtube.com/channel/UC6sfBMKXtwZBJ7j5WPyvf6g/videos) has examples of generated videos.

## Usage

1. Generate data using Auto-Link or use the dataset from https://github.com/mipacd/auto-clip.
2. Run on a CSV to generate a compilation: `python3 ./auto-clip-vid-gen.py -s hlen -t ../auto-clip/csv/hl/humor/2021-4-25.csv hlen-funny-comp-2021-04-25.mp4`
3. One minute clips from each stream are downloaded to ./clips/. Timestamps and channel citations are saved in description.txt.


## Options
--length, -l Length of clip. Clip starts from 30 seconds before region of interest and continues for 1 minute by default. 

--streamer, -s Channel/Streamer. Use individual names seperated by commas or use a grouping defined in streamers.py  

--no-concat, -n Download clips without concatinating into a compilation. Clips can be modified and combined in a subsequent run without the -n flag, given that all videos remain in the same format and the -d flag is not used.  

--delete, -d Delete all clips from ./clips/ directory  

--thumb, -t Lazy thumbnail generation. Requires the vcsi Python package: `pip3 install vcsi`  

## Examples

1. Create video compilation from output of Auto-Link example 1  
`python3 ./auto_clip.py -c ./output/2021-05-03/funny_amelia_2021-05-03.csv output.mp4`
