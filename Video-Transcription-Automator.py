import time
import os
import concurrent.futures
from datetime import datetime, timedelta
import copy
from speechmatics.models import ConnectionSettings, BatchTranscriptionConfig
from speechmatics.batch_client import BatchClient
from httpx import HTTPStatusError

# Insert API Key Here
API_KEY = "0000" 

settings = ConnectionSettings(
    url="https://asr.api.speechmatics.com/v2",
    auth_token=API_KEY,
)

# Define default transcription parameters
base_conf = {
    "type": "transcription",
    "transcription_config": {
        "operating_point": "enhanced"
    }
}

# Get transcript from specific job
def getTranscript():
    # Example job ID
    job_id = "00123"
    with BatchClient(API_KEY) as client:
        try:
            transcript = client.get_job_result(job_id, transcription_format="srt")
            print("Transcript retrieved successfully!")

            # Save the transcript to a TXT file
            txt_file_path = f"{job_id}.txt"
            with open(txt_file_path, "w", encoding="utf-8") as txt_file:
                txt_file.write(transcript)
                print(f"Transcript saved to {txt_file_path}")
        except HTTPStatusError as e:
            if e.response.status_code == 404:
                print(f"Job {job_id} not found!")
            elif e.response.status_code == 401:
                print("Invalid API key!")
            else:
                raise e

# List all Job IDs
def getJobIDs():
    with BatchClient(API_KEY) as client:
        jobs_list = client.list_jobs()

        # Check if there are jobs
        if jobs_list:
            # Loop through the list of jobs and print the job IDs
            for job in jobs_list:
                print(f"Job ID: {job['id']}, Data file name: {job['data_name']}, created at:{job['created_at']}")

# Function to dynamically change the language based on the directory
def get_language_from_directory(directory):
    # You can map directories to their respective languages
    language_map = {
        "Channel 5": "en",       # Example: English for Channel 5
        "Channel 8": "cmn",      # Example: Mandarin for Channel 8
        "Channel U": "cmn",      # Example: Mandarin for Channel U
        "Suria": "ms",           # Example: Malay for Suria
        "Vasantham": "ta"        # Example: Tamil for Vasantham
    }
    
    # Extract the folder name (assuming folder names are like 'Channel 5', 'Channel 8', etc.)
    folder_name = os.path.basename(os.path.dirname(directory))
    
    # Return the corresponding language code or default to 'en-GB' (English)
    return language_map.get(folder_name, "auto")

# Process video files from a directory in parallel
def process_and_upload_transcripts_in_parallel(directories):
    with BatchClient(settings) as client:
        # Gather all video files with their corresponding languages from all directories in the batch
        video_files_and_configs = []
        for directory in directories:
            # Determine the language based on the directory
            language = get_language_from_directory(directory)

            # Modify the transcription config with the correct language
            conf = copy.deepcopy(base_conf)  # Use deep copy to prevent shared references
            conf["transcription_config"]["language"] = language
            
            # Get a list of all video files in the directory
            for root, _, files in os.walk(directory):
                for file in files:
                    if file.endswith(".mp4"):
                        video_files_and_configs.append({
                            "video_path": os.path.join(root, file),
                            "config": conf,
                        })
        
        # Define a function to process a single file
        def process_single_file(file_info):
            video_path = file_info["video_path"]
            conf = file_info["config"]
            
            print(f"Processing file: {video_path} with language {conf["transcription_config"]["language"]}")

            try:                
                # Submit the transcription job
                job_id = client.submit_job(
                    audio=video_path,
                    transcription_config=conf,
                )
                print(f"Job {job_id} submitted for {video_path}, waiting for transcription...")
                
                # Wait for the transcription job to complete
                transcript = client.wait_for_completion(
                    job_id,
                    transcription_format="srt"
                )
                print(f"Transcript retrieved for {video_path}")
                
                # Save the transcript to the same directory as the video
                transcript_path = os.path.splitext(video_path)[0] + ".txt"
                with open(transcript_path, "w", encoding="utf-8") as transcript_file:
                    transcript_file.write(transcript)
                print(f"Transcript saved to {transcript_path}")
            except HTTPStatusError as e:
                if e.response.status_code == 401:
                    print("Invalid API key - Check your API_KEY!")
                elif e.response.status_code == 400:
                    print(e.response.json().get("detail", "Unknown error"))
                else:
                    print(f"HTTP error for {video_path}: {e}")
            except Exception as e:
                print(f"Error processing {video_path}: {e}")

        # Use ThreadPoolExecutor to process files in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = [
                executor.submit(process_single_file, file_info)
                for file_info in video_files_and_configs
            ]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()  # Raise any exception that occurred during processing
                except Exception as e:
                    print(f"Error occurred in thread: {e}")

# Get the files from its respective locations
# Get yesterday's date
yesterday_date = datetime.today() - timedelta(days=1)

# Format the date as "e.g. 13 January"
formatted_yesterday_date = yesterday_date.strftime("%d %B")

# Insert your file path here
g_drive_file_path = "your_file_path"
monitoring_period = "Period (2025 H1)"

CH5_dir = f"{g_drive_file_path}/{monitoring_period}/Channel 5/{formatted_yesterday_date}"
CH8_dir = f"{g_drive_file_path}/{monitoring_period}/Channel 8/{formatted_yesterday_date}"
CHU_dir = f"{g_drive_file_path}/{monitoring_period}/Channel U/{formatted_yesterday_date}"
Suria_dir = f"{g_drive_file_path}/{monitoring_period}/Suria/{formatted_yesterday_date}"
Vasantham_dir = f"{g_drive_file_path}/{monitoring_period}/Vasantham/{formatted_yesterday_date}"

batch_1 = [CH5_dir, CH8_dir]
batch_2 = [CHU_dir, Suria_dir, Vasantham_dir]

print("Processing Batch 1 in parallel...")
process_and_upload_transcripts_in_parallel(batch_1)

print("Waiting before starting Batch 2...")
time.sleep(30)

print("Processing Batch 2 in parallel...")
process_and_upload_transcripts_in_parallel(batch_2)

print("All batches processed.")

