#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from ast import parse
import logging
import argparse
import sys
import os
from time import localtime, strftime
from pathlib import Path
import re
import inotify.adapters
import boto3

PICTURES_FOLDER_REGEX=r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
RESYNC_INTERVAL_COUNT=100

class Synchronizer:

    def __init__(self, base_folder, tracker_file, bucket_name, bucket_prefix, aws_profile, s3_endpoint_url):
        self.base_folder = base_folder
        self.tracker_file = tracker_file
        self.bucket_name = bucket_name
        self.bucket_prefix = bucket_prefix
        self.aws_profile = aws_profile
        self.s3_endpoint_url = s3_endpoint_url

        self.synced_files = self._get_already_sync()
        self._init_tracker()
        self._s3_connect()

    def _init_tracker(self):
        """
        Initialize the tracker file if not exist
        """
        if not os.path.exists(self.tracker_file):
            with open(self.tracker_file, "w") as fd:
                fd.write("")

    def _update_tracker(self, new_uploads):
        """
        Update the tracker file
        """
        self.synced_files += new_uploads
        with open(self.tracker_file, "w") as fd:
            fd.write("\n".join(self.synced_files))

    def _s3_connect(self):
        """
        Initialize an S3 session
        """
        self.s3 = boto3.session.Session(
            profile_name=self.aws_profile
        ).resource(
            's3',
            endpoint_url=self.s3_endpoint_url
        )

    def _get_already_sync(self):
        """
        Get list of files already synced to S3
        """
        with open(self.tracker_file, "r") as fd:
            files = fd.read().splitlines()
        return files

    def _get_new_files(self, pics_folder):
        """
        List all files that have not already been synced to S3 from a specific folder
        """
        # Get list of files locally
        folder_files = sorted(os.listdir(pics_folder))
        # Keep only ".jpg" files
        filtered_files = [file for file in folder_files if Path(file).suffix == '.jpg']

        # Diff between synced and local files
        new_files = list(set(filtered_files) - set(self.synced_files))
        return new_files

    def _send_file(self, file_path):
        """
        Send a file to S3 bucket.
        """
        # Construct object path
        obj_name = os.path.join(self.bucket_prefix, Path(file_path).name)
        logging.info('Sending picture %s to bucket %s', obj_name, self.bucket_name)
        # Upload to S3
        self.s3.meta.client.upload_file(file_path, self.bucket_name, obj_name)

    def sync(self, pics_folder):
        """
        Synchronize a local folder with S3
        """
        new_files = self._get_new_files(pics_folder)
        # Track successful uploads
        successful_uploads = []
        # Loop on files to upload
        for file in new_files:
            try:
                src_path = os.path.join(pics_folder, file)
                self._send_file(src_path)
            except Exception as err:
                logging.error("Unable to send picture " + str(src_path) + " to S3 : ", err)
            else:
                successful_uploads.append(file)
        # Update the synced file list
        self._update_tracker(successful_uploads)

    def run(self):
        """
        Start watching for new files on the local folder to sync
        """
        # Watch photobooth folder
        watcher = inotify.adapters.InotifyTree(self.base_folder)

        # Count files to trigger a full resync sometimes
        file_counter = 0
        processed_folders = []

        # Loop on events        
        for event in watcher.event_gen():
            if event is not None:
                (header, type_names, watch_path, filename) = event

                # Only send file when closed, in folder matching the pattern and with jpg extension
                if "IN_CLOSE_WRITE" in type_names and \
                    bool(re.match(PICTURES_FOLDER_REGEX, Path(watch_path).name)) and \
                    Path(filename).suffix == '.jpg':
                    # Get full file path
                    src_path = os.path.join(watch_path, filename)
                    try:
                        # Send
                        self._send_file(src_path)
                    except Exception as err:
                        logging.error("Unable to send picture " + str(src_path) + " to S3 : ", err)
                    else:
                        # Add the file to the synced list
                        self._update_tracker([filename])
                    # Update counter and folder tracker
                    file_counter += 1
                    if watch_path not in processed_folders:
                        processed_folders.append(watch_path)

                # After some file processings, trigger a resync
                if file_counter >= RESYNC_INTERVAL_COUNT:
                    file_counter = 0
                    for folder in processed_folders:
                        self.sync(folder)

def parse_args(argv):

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--debug',
        action='store_true',
        help='enable additional debug output'
    )
    parser.add_argument(
        '--photobooth-folder',
        help="Base folder where photobooth is running",
        default="/home/photobooth/photobooth"
    )
    parser.add_argument(
        '--folder-resync', 
        help='Force resync of a folder (value must be folder full path)',
        default=None
    )
    parser.add_argument(
        '--tracker-file',
        help="File used to track already synced pictures with S3",
        default="synced-files.txt"
    )
    parser.add_argument(
        '--bucket-name',
        help="Name of S3 bucket to sync with",
        default="photobooth"
    )
    parser.add_argument(
        '--bucket-prefix',
        help="Prefix for uploaded files inside bucket",
        default="input/"
    )
    parser.add_argument(
        '--aws-profile',
        help="AWS profile to use for S3",
        default="default"
    )
    parser.add_argument(
        '--s3-endpoint-url',
        help="Endpoint URL for S3",
        default="https://s3.fr-par.scw.cloud"
    )

    return parser.parse_known_args()

def main(argv):
    # Parse command line arguments
    parsed_args, unparsed_args = parse_args(argv)
    argv = argv[:1] + unparsed_args

    # Setup log level and format
    if parsed_args.debug:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y/%m/%d %H:%M:%S',
        level=log_level
    )

    # Check arguments
    if parsed_args.photobooth_folder is None and parsed_args.folder_resync is None:
        logging.error("At least one of --photobooth-folder or --folder-resync argument is required")
        sys.exit(-1)
    
    synchronizer = Synchronizer(
        parsed_args.photobooth_folder,
        parsed_args.tracker_file,
        parsed_args.bucket_name,
        parsed_args.bucket_prefix,
        parsed_args.aws_profile,
        parsed_args.s3_endpoint_url
    )

    if parsed_args.folder_resync is not None:
        synchronizer.sync(parsed_args.folder_resync)
    else:
        synchronizer.run()

if __name__ == '__main__':
    main(sys.argv)
