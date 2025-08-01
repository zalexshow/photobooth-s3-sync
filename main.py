#!/usr/bin/env python3

import os
import time
import logging
import hashlib
import sqlite3
import threading
import argparse
from pathlib import Path
from datetime import datetime
import re

import boto3
import inotify.adapters
import requests

PICTURES_FOLDER_REGEX = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"

class FileTracker:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS files (
                    filepath TEXT PRIMARY KEY,
                    size INTEGER NOT NULL,
                    hash TEXT NOT NULL,
                    uploaded_at TIMESTAMP,
                    mtime REAL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_uploaded ON files(uploaded_at)")
            
            # Add mtime column if it doesn't exist (migration)
            try:
                conn.execute("ALTER TABLE files ADD COLUMN mtime REAL")
            except sqlite3.OperationalError:
                pass  # Column already exists
    
    def _file_hash(self, filepath: str) -> str:
        hash_md5 = hashlib.md5()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def track_file(self, filepath: str) -> bool:
        """Track file if new or changed. Returns True if added/updated."""
        try:
            if not os.path.exists(filepath):
                return False
            
            stat = os.stat(filepath)
            size, mtime = stat.st_size, stat.st_mtime
            
            with sqlite3.connect(self.db_path) as conn:
                # Check if file exists and is unchanged (size + mtime check first)
                cursor = conn.execute(
                    "SELECT size, hash, mtime FROM files WHERE filepath = ?", 
                    (filepath,)
                )
                row = cursor.fetchone()
                
                # Quick check: if size and mtime match, assume unchanged
                if row and row[0] == size and row[2] == mtime:
                    return False  # File unchanged, skip hash calculation
                
                # If size/mtime different, calculate hash
                file_hash = self._file_hash(filepath)
                
                # Double check with hash if we have existing data
                if row and row[0] == size and row[1] == file_hash:
                    # Same content but mtime changed, just update mtime
                    conn.execute(
                        "UPDATE files SET mtime = ? WHERE filepath = ?",
                        (mtime, filepath)
                    )
                    return False
                
                # Insert or update file
                conn.execute("""
                    INSERT OR REPLACE INTO files (filepath, size, hash, uploaded_at, mtime)
                    VALUES (?, ?, ?, NULL, ?)
                """, (filepath, size, file_hash, mtime))
                
                logging.info(f"Tracked: {filepath}")
                return True
                
        except Exception as e:
            logging.error(f"Failed to track {filepath}: {e}")
            return False
    
    def get_pending(self) -> list:
        """Get all files not yet uploaded."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT filepath FROM files WHERE uploaded_at IS NULL ORDER BY filepath"
            )
            return [row[0] for row in cursor.fetchall()]
    
    def mark_uploaded(self, filepath: str):
        """Mark file as successfully uploaded."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE files SET uploaded_at = ? WHERE filepath = ?",
                (datetime.now(), filepath)
            )
    
    def mark_all_uploaded(self, filepaths: list):
        """Mark multiple files as uploaded in batch."""
        current_time = datetime.now()
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                "UPDATE files SET uploaded_at = ? WHERE filepath = ?",
                [(current_time, fp) for fp in filepaths]
            )
    
    def get_stats(self) -> dict:
        """Get upload statistics."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(uploaded_at) as uploaded,
                    COUNT(*) - COUNT(uploaded_at) as pending
                FROM files
            """)
            row = cursor.fetchone()
            return {
                'total': row[0],
                'uploaded': row[1], 
                'pending': row[2]
            }

class SimpleSync:
    def __init__(self, watch_folder: str, db_path: str, s3_config: dict, refresh_url: str):
        self.watch_folder = watch_folder
        self.refresh_url = refresh_url
        self.tracker = FileTracker(db_path)
        self.running = False
        
        # S3 setup
        self.s3_client = boto3.session.Session(
            profile_name=s3_config['profile']
        ).client('s3', endpoint_url=s3_config['endpoint'])
        self.bucket = s3_config['bucket']
        self.prefix = s3_config['prefix']
    
    def _upload_file(self, filepath: str) -> bool:
        """Upload single file to S3. Returns True on success."""
        try:
            s3_key = os.path.join(self.prefix, Path(filepath).name)
            logging.info(f"Uploading: {filepath} -> s3://{self.bucket}/{s3_key}")
            
            self.s3_client.upload_file(filepath, self.bucket, s3_key)
            
            # Trigger refresh
            try:
                requests.post(self.refresh_url, timeout=10)
            except Exception as e:
                logging.warning(f"Refresh failed: {e}")
            
            return True
            
        except Exception as e:
            logging.error(f"Upload failed {filepath}: {e}")
            return False
    
    def _uploader_thread(self):
        """Background thread that uploads pending files."""
        while self.running:
            try:
                pending = self.tracker.get_pending()
                
                if not pending:
                    time.sleep(2)
                    continue
                
                for filepath in pending:
                    if not self.running:
                        break
                    
                    if not os.path.exists(filepath):
                        logging.warning(f"File disappeared: {filepath}")
                        continue
                    
                    if self._upload_file(filepath):
                        self.tracker.mark_uploaded(filepath)
                        logging.info(f"Uploaded: {filepath}")
                    else:
                        time.sleep(5)  # Wait before retry
                        
            except Exception as e:
                logging.error(f"Uploader error: {e}")
                time.sleep(10)
    
    def startup_scan(self):
        """Scan existing files and track them."""
        logging.info(f"Scanning: {self.watch_folder}")
        
        if not os.path.exists(self.watch_folder):
            logging.error(f"Folder not found: {self.watch_folder}")
            return
        
        # Collect all files first
        all_files = []
        for item in os.listdir(self.watch_folder):
            folder_path = os.path.join(self.watch_folder, item)
            
            if (os.path.isdir(folder_path) and 
                re.match(PICTURES_FOLDER_REGEX, item)):
                
                for filename in os.listdir(folder_path):
                    if filename.lower().endswith('.jpg'):
                        filepath = os.path.join(folder_path, filename)
                        all_files.append(filepath)
        
        logging.info(f"Found {len(all_files)} files to scan")
        
        # Process files with progress logging
        count = 0
        for i, filepath in enumerate(all_files):
            if self.tracker.track_file(filepath):
                count += 1
            
            # Progress logging every 1000 files
            if (i + 1) % 1000 == 0:
                logging.info(f"Progress: {i + 1}/{len(all_files)} files scanned, {count} new")
        
        stats = self.tracker.get_stats()
        logging.info(f"Scan complete: {count} new files, {stats['pending']} pending")
    
    def run(self):
        """Main run loop."""
        logging.info("Starting sync...")
        
        # Initial scan
        self.startup_scan()
        
        # Start uploader thread
        self.running = True
        uploader = threading.Thread(target=self._uploader_thread, daemon=True)
        uploader.start()
        
        # Watch for new files
        try:
            watcher = inotify.adapters.InotifyTree(self.watch_folder)
            logging.info(f"Watching: {self.watch_folder}")
            
            for event in watcher.event_gen():
                if not self.running:
                    break
                    
                if event is None:
                    continue
                
                (_, type_names, watch_path, filename) = event
                
                if ("IN_CLOSE_WRITE" in type_names and 
                    filename.lower().endswith('.jpg') and
                    re.match(PICTURES_FOLDER_REGEX, Path(watch_path).name)):
                    
                    filepath = os.path.join(watch_path, filename)
                    self.tracker.track_file(filepath)
                    
        except KeyboardInterrupt:
            logging.info("Stopping...")
        finally:
            self.running = False

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--folder', required=True, help='Folder to watch')
    parser.add_argument('--db', default='sync.db', help='SQLite database')
    parser.add_argument('--bucket', required=True, help='S3 bucket name') 
    parser.add_argument('--prefix', default='input/', help='S3 key prefix')
    parser.add_argument('--profile', default='default', help='AWS profile')
    parser.add_argument('--endpoint', required=True, help='S3 endpoint URL')
    parser.add_argument('--refresh-url', required=True, help='Refresh URL')
    parser.add_argument('--debug', action='store_true', help='Debug logging')
    
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    s3_config = {
        'bucket': args.bucket,
        'prefix': args.prefix, 
        'profile': args.profile,
        'endpoint': args.endpoint
    }
    
    sync = SimpleSync(args.folder, args.db, s3_config, args.refresh_url)
    sync.run()

if __name__ == '__main__':
    main()
