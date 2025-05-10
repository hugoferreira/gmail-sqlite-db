import argparse
import asyncio
import datetime
import email
import imaplib
import json
import os
import re
import sys
from email.header import decode_header
from email.utils import parsedate_to_datetime

# Third-party imports
import aiosqlite
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from tqdm import tqdm

# OAuth2 setup
SCOPES = ['https://mail.google.com/']
TOKEN_PATH = 'token.json'
CHECKPOINT_PATH = 'checkpoint.json'  # Default (legacy support)
CHECKPOINT_HEADERS_PATH = 'checkpoint_headers.json'
CHECKPOINT_FULL_PATH = 'checkpoint_full.json'
CHUNK_SIZE = 250  # Reduced for more reliable processing and frequent commits
EMAILS_PER_COMMIT = 20  # Commit after processing this many emails
DEBUG = False   # Enable debug mode - set to False by default for full processing

def debug_print(*args, **kwargs):
    if DEBUG:
        print(*args, **kwargs)

# Checkpoint system for tracking sync state
class CheckpointManager:
    def __init__(self, checkpoint_path=CHECKPOINT_PATH):
        self.checkpoint_path = checkpoint_path
        self.state = self._load_state()
        
    def _load_state(self):
        """Load checkpoint state from file if it exists"""
        if os.path.exists(self.checkpoint_path):
            try:
                with open(self.checkpoint_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Error loading checkpoint state: {e}")
        
        # Default state if no checkpoint file exists or loading fails
        return {
            'last_uid': 0,        # Last successfully processed UID
            'failed_uids': [],    # UIDs that failed to process
            'in_progress': False, # Whether a sync was interrupted
            'mailbox': None,      # Last mailbox being processed
            'timestamp': None     # Last sync timestamp
        }
    
    def save_state(self):
        """Save current state to checkpoint file"""
        self.state['timestamp'] = datetime.datetime.now().isoformat()
        try:
            with open(self.checkpoint_path, 'w') as f:
                json.dump(self.state, f, indent=2)
        except Exception as e:
            print(f"Error saving checkpoint state: {e}")
    
    def mark_start(self, mailbox):
        """Mark the start of a sync operation"""
        self.state['in_progress'] = True
        self.state['mailbox'] = mailbox
        self.save_state()
    
    def mark_complete(self):
        """Mark the completion of a sync operation"""
        self.state['in_progress'] = False
        self.save_state()
    
    def update_progress(self, uid):
        """Update the last processed UID"""
        # Only update if this UID is greater than the last one
        uid_int = int(uid)
        if uid_int > self.state['last_uid']:
            self.state['last_uid'] = uid_int
            # Only save periodically to avoid excessive disk writes
            if uid_int % 100 == 0:  # Save more frequently (was 500)
                self.save_state()
    
    def add_failed_uid(self, uid):
        """Add a UID to the failed list"""
        if uid not in self.state['failed_uids']:
            self.state['failed_uids'].append(uid)
            # Only save periodically to avoid excessive disk writes
            if len(self.state['failed_uids']) % 100 == 0:
                self.save_state()
    
    def get_last_uid(self):
        """Get the last successfully processed UID"""
        return self.state['last_uid']
    
    def get_failed_uids(self):
        """Get the list of failed UIDs"""
        return self.state['failed_uids']
    
    def clear_failed_uid(self, uid):
        """Remove a UID from the failed list if it's been processed successfully"""
        if uid in self.state['failed_uids']:
            self.state['failed_uids'].remove(uid)
            
    def was_interrupted(self):
        """Check if a previous sync was interrupted"""
        return self.state['in_progress']

# Obtain or refresh OAuth2 credentials via "Sign in with Google"
def get_credentials(creds_path):
    if not os.path.exists(creds_path):
        sys.exit(f"Error: OAuth2 client secrets JSON file not found at '{creds_path}'. Please download from Google Cloud Console.")
    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_PATH, 'w') as token_file:
            token_file.write(creds.to_json())
    return creds

# Parse email date into ISO format for better querying
def parse_email_date(date_str):
    if not date_str:
        return None
    
    try:
        return parsedate_to_datetime(date_str).isoformat()
    except:
        return date_str

# Decode MIME headers with better error handling
def decode_field(field):
    if not field:
        return ''
    parts = decode_header(field)
    decoded = ''
    for part, encoding in parts:
        if isinstance(part, bytes):
            try:
                # Handle unknown encodings gracefully
                if encoding and encoding.lower() == 'unknown-8bit':
                    decoded += part.decode('utf-8', errors='replace')
                else:
                    decoded += part.decode(encoding or 'utf-8', errors='replace')
            except (LookupError, UnicodeDecodeError):
                # Fallback to utf-8 with error replacement
                decoded += part.decode('utf-8', errors='replace')
        else:
            decoded += part
    return decoded

# Extract UID from FETCH response
def extract_uid(response_line):
    if isinstance(response_line, bytes):
        response_line = response_line.decode('utf-8', errors='replace')
    
    debug_print(f"Parsing response line: {response_line}")
    
    # Try different regex patterns to extract UID
    patterns = [
        r'UID (\d+)',         # Standard format
        r'\(UID (\d+)',       # Parenthesized format
        r'UID=(\d+)',         # Key-value format
        r'[^\d](\d+)[^\d]'    # Any number surrounded by non-digits
    ]
    
    for pattern in patterns:
        match = re.search(pattern, response_line)
        if match:
            uid = match.group(1)
            debug_print(f"  - Extracted UID: {uid}")
            return uid
    
    debug_print("  - No UID found")
    return None

# Parse IMAP response for UIDs and headers
def parse_imap_response(data):
    """Parse the IMAP FETCH response to extract UIDs and header data."""
    debug_print(f"Response data has {len(data)} elements")
    
    # Debug first few elements
    for i in range(min(2, len(data))):
        debug_print(f"Data[{i}] type: {type(data[i])}")
        if isinstance(data[i], bytes):
            try:
                debug_print(f"Data[{i}] (first 100 bytes): {data[i][:100]}")
            except:
                debug_print(f"Data[{i}]: Unable to print")
        elif isinstance(data[i], tuple):
            debug_print(f"Data[{i}] is tuple of length {len(data[i])}")
    
    messages = []
    i = 0
    
    while i < len(data):
        # For normal IMAP responses, the pattern is usually:
        # 1. A bytes object with message metadata (including UID)
        # 2. A tuple containing the message data
        uid = None
        header_data = None
        
        # Try to extract UID and header data
        if i < len(data) and isinstance(data[i], bytes):
            # Try to extract UID from this line
            uid = extract_uid(data[i])
            i += 1
        
        # Try to get header data from the next element
        if i < len(data):
            if isinstance(data[i], tuple) and len(data[i]) > 1:
                # This is likely the header data tuple
                header_data = data[i][1]  # Second element is typically the data
            elif isinstance(data[i], bytes):
                # Sometimes header data might be directly in bytes
                header_data = data[i]
            
            # If we found header data, process it only if we have a UID
            if header_data and uid:
                messages.append((uid, header_data))
            
            i += 1
        else:
            i += 1
    
    debug_print(f"Extracted {len(messages)} message pairs")
    return messages

async def setup_schema(db):
    # Performance pragmas for better SQLite performance
    await db.execute("PRAGMA journal_mode=WAL;")
    await db.execute("PRAGMA synchronous=NORMAL;")
    await db.execute("PRAGMA temp_store=MEMORY;")
    await db.execute("PRAGMA cache_size=-50000;")  # Use about 50MB of memory for caching
    await db.execute("PRAGMA foreign_keys=OFF;")   # Disable foreign key checks for imports
    
    # Check if we need to update the schema
    async with db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='emails'") as cursor:
        row = await cursor.fetchone()
        
    if row is None:
        # Create table and indexes - using ISO 8601 format for date field for better querying
        await db.execute('''
            CREATE TABLE IF NOT EXISTS emails (
                uid TEXT PRIMARY KEY,
                msg_from TEXT,
                msg_to TEXT,
                msg_cc TEXT,
                subject TEXT,
                msg_date TEXT,
                mailbox TEXT
            )
        ''')
        # Add indexes
        await db.execute('CREATE INDEX IF NOT EXISTS idx_from ON emails(msg_from)')
        await db.execute('CREATE INDEX IF NOT EXISTS idx_to ON emails(msg_to)')
        await db.execute('CREATE INDEX IF NOT EXISTS idx_cc ON emails(msg_cc)')
        await db.execute('CREATE INDEX IF NOT EXISTS idx_date ON emails(msg_date)')
        await db.execute('CREATE INDEX IF NOT EXISTS idx_mailbox ON emails(mailbox)')
    else:
        # Check if the table contains the mailbox column
        has_mailbox_column = False
        async with db.execute("PRAGMA table_info(emails)") as cursor:
            rows = await cursor.fetchall()
            for row in rows:
                if row[1] == 'mailbox':
                    has_mailbox_column = True
                    break
        
        # Add the mailbox column if it doesn't exist
        if not has_mailbox_column:
            print("Adding 'mailbox' column to emails table...")
            await db.execute('ALTER TABLE emails ADD COLUMN mailbox TEXT')
            await db.execute('CREATE INDEX IF NOT EXISTS idx_mailbox ON emails(mailbox)')
        
        # Check if we need to add the date index
        async with db.execute("SELECT sql FROM sqlite_master WHERE type='index' AND name='idx_date'") as cursor:
            row = await cursor.fetchone()
        if row is None:
            await db.execute('CREATE INDEX IF NOT EXISTS idx_date ON emails(msg_date)')
    
    # Create a table to track sync status for extra reliability
    await db.execute('''
        CREATE TABLE IF NOT EXISTS sync_status (
            id INTEGER PRIMARY KEY,
            last_uid INTEGER,
            start_time TEXT,
            end_time TEXT,
            status TEXT,
            message TEXT
        )
    ''')
    # Create a table to store full emails
    await db.execute('''
        CREATE TABLE IF NOT EXISTS full_emails (
            uid TEXT PRIMARY KEY,
            mailbox TEXT,
            raw_email BLOB,
            fetched_at TEXT
        )
    ''')
    await db.commit()

# Create a non-async function to authenticate with IMAP using XOAUTH2
def imap_oauth2_login(host, user, access_token):
    # Connect to IMAP server
    imap = imaplib.IMAP4_SSL(host)
    
    # Create the auth string in the format: "user=<email>\x01auth=Bearer <token>\x01\x01"
    auth_string = f"user={user}\x01auth=Bearer {access_token}\x01\x01"
    
    # Authenticate using XOAUTH2
    imap.authenticate('XOAUTH2', lambda x: auth_string)
    
    return imap

async def list_mailboxes(host, user, creds):
    """List all available mailboxes on the server"""
    print(f"Connecting to {host} as {user}...")
    
    # Run the synchronous IMAP authentication in a thread pool
    loop = asyncio.get_event_loop()
    imap = await loop.run_in_executor(
        None, 
        lambda: imap_oauth2_login(host, user, creds.token)
    )
    
    try:
        print("Fetching mailboxes:")
        status, mailboxes = await loop.run_in_executor(None, imap.list)
        
        if status != 'OK':
            print(f"Failed to list mailboxes: {status}")
            return
            
        print("\nAvailable mailboxes:")
        print("--------------------")
        for i, mailbox in enumerate(mailboxes, 1):
            # Parse the mailbox name from the response
            if isinstance(mailbox, bytes):
                mailbox = mailbox.decode('utf-8', errors='replace')
                
            # The format is typically like: (\\HasNoChildren) "/" "INBOX.Sent"
            parts = mailbox.split(' "')
            if len(parts) > 1:
                # Extract the mailbox name (removing trailing quote)
                name = parts[-1].rstrip('"')
                print(f"{i}. {name}")
            else:
                print(f"{i}. {mailbox}")
        print("\nUse any of these names with the --mailbox argument")
    
    finally:
        try:
            await loop.run_in_executor(None, lambda: imap.logout())
        except Exception as e:
            print(f"Error during logout: {e}")

class EmailSyncer:
    def __init__(self, db, host, user, creds, mailbox, mode, fetch_fn):
        self.db = db
        self.host = host
        self.user = user
        self.creds = creds
        self.mailbox = mailbox
        self.mode = mode
        self.fetch_fn = fetch_fn
        # Use mode-specific checkpoint file
        checkpoint_path = CHECKPOINT_HEADERS_PATH if mode == 'headers' else CHECKPOINT_FULL_PATH
        self.checkpoint = CheckpointManager(checkpoint_path)
        self.loop = asyncio.get_event_loop()
        self.imap = None
        self.last_status_id = None
        self.failed_uids = self.checkpoint.get_failed_uids()
        self.commit_interval = 1
        self.emails_since_commit = 0
        self.pbar = None

    async def connect(self):
        print(f"Connecting to {self.host} as {self.user} for {self.mode} sync...")
        self.imap = await self.loop.run_in_executor(
            None, lambda: imap_oauth2_login(self.host, self.user, self.creds.token)
        )
        print(f"Connection established successfully")

    async def start_sync(self, start_message):
        self.checkpoint.mark_start(self.mailbox)
        await self.db.execute('''
            INSERT INTO sync_status (start_time, status, message)
            VALUES (?, 'STARTED', ?)
        ''', (datetime.datetime.now().isoformat(), start_message))
        async with self.db.execute('SELECT last_insert_rowid()') as cursor:
            row = await cursor.fetchone()
            self.last_status_id = row[0] if row else None

    async def finish_sync(self, status, message):
        await self.db.execute(f"UPDATE sync_status SET end_time = ?, status = ?, message = ? WHERE id = ?", 
                          (datetime.datetime.now().isoformat(), status, message, self.last_status_id))
        await self.db.commit()
        self.checkpoint.mark_complete()

    async def run(self, uids_to_fetch, total_count, fetch_mode_desc):
        processed_count = 0
        skipped_count = 0
        saved_count = 0
        self.emails_since_commit = 0
        
        if DEBUG:
            # Print the first few UIDs we'll be processing
            sample_uids = uids_to_fetch[:min(10, len(uids_to_fetch))]
            print(f"[DEBUG] First {len(sample_uids)} UIDs to process: {sample_uids}")
            print(f"[DEBUG] Total UIDs to process: {total_count}")
            if self.failed_uids:
                sample_failed = self.failed_uids[:min(10, len(self.failed_uids))]
                print(f"[DEBUG] First {len(sample_failed)} failed UIDs: {sample_failed}")
        
        self.pbar = tqdm(total=total_count, desc=f'Fetching {fetch_mode_desc}')
        prev_mailbox = None
        try:
            for chunk_idx, i in enumerate(range(0, len(uids_to_fetch), CHUNK_SIZE)):
                chunk = uids_to_fetch[i:i + CHUNK_SIZE]
                if not chunk:
                    continue
                for uid, mbox in chunk:
                    is_retry = uid in self.failed_uids
                    try:
                        # Always select the correct mailbox before fetching
                        if prev_mailbox != mbox:
                            status, _ = await self.loop.run_in_executor(None, lambda: self.imap.select(mbox, readonly=True))
                            if status != 'OK':
                                print(f"Failed to select mailbox {mbox}: {status}")
                                self.checkpoint.add_failed_uid(uid)
                                skipped_count += 1
                                self.pbar.update(1)
                                prev_mailbox = mbox
                                continue
                            prev_mailbox = mbox
                        # Call the fetch function (header or full)
                        result = await self.fetch_fn(self, uid, mbox)
                        if result == 'fail':
                            skipped_count += 1
                        elif result == 'saved':
                            saved_count += 1
                            processed_count += 1
                        self.pbar.update(1)
                        self.emails_since_commit += 1
                        if self.emails_since_commit >= EMAILS_PER_COMMIT:
                            self.checkpoint.save_state()
                            try:
                                await self.db.commit()
                                try:
                                    await self.db.execute("BEGIN TRANSACTION")
                                except Exception as e:
                                    debug_print(f"Transaction already started: {e}")
                            except Exception as commit_err:
                                print(f"Error committing transaction: {commit_err}")
                            self.emails_since_commit = 0
                    except Exception as e:
                        debug_print(f"Error processing UID {uid}: {e}")
                        self.checkpoint.add_failed_uid(uid)
                        skipped_count += 1
                        self.pbar.update(1)
                if chunk_idx % self.commit_interval == 0:
                    self.checkpoint.save_state()
                    try:
                        await self.db.commit()
                        try:
                            await self.db.execute("BEGIN TRANSACTION")
                        except Exception as e:
                            debug_print(f"Transaction already started: {e}")
                    except Exception as commit_err:
                        print(f"Error committing transaction: {commit_err}")
            await self.db.commit()
            await self.finish_sync('COMPLETED', f'Successfully processed {saved_count} {fetch_mode_desc}')
        except KeyboardInterrupt:
            print("\nOperation interrupted by user. Saving progress...")
            try:
                await self.db.commit()
                await self.finish_sync('INTERRUPTED', 'Interrupted by user')
                self.checkpoint.save_state()
                print(f"Progress saved. Last processed UID: {self.checkpoint.get_last_uid()}")
                print(f"Failed UIDs count: {len(self.checkpoint.get_failed_uids())}")
            except Exception as e:
                print(f"Error saving progress: {e}")
            raise
        except Exception as e:
            print(f"\nUnexpected error: {e}")
            try:
                await self.db.commit()
                await self.finish_sync('ERROR', str(e)[:200])
                self.checkpoint.save_state()
                print(f"Partial progress saved. Last processed UID: {self.checkpoint.get_last_uid()}")
                print(f"Failed UIDs count: {len(self.checkpoint.get_failed_uids())}")
            except Exception as commit_err:
                print(f"Error saving progress: {commit_err}")
            raise
        finally:
            self.pbar.n = processed_count
            self.pbar.refresh()
            self.pbar.close()
            print(f"Successfully processed {processed_count} messages")
            print(f"Saved {saved_count} {fetch_mode_desc} to database")
            print(f"Skipped {skipped_count} messages")
        return processed_count, saved_count, skipped_count

async def fetch_headers_syncer(syncer, uid, mailbox):
    # Fetch headers for a single UID
    status, data = await syncer.loop.run_in_executor(
        None,
        lambda: syncer.imap.uid('FETCH', uid, '(BODY.PEEK[HEADER.FIELDS (FROM TO CC SUBJECT DATE)])')
    )
    if status != 'OK':
        debug_print(f"Failed to fetch headers for UID {uid}: {status}")
        syncer.checkpoint.add_failed_uid(uid)
        return 'fail'
    header_data = None
    for item in data:
        if isinstance(item, tuple) and len(item) > 1:
            header_data = item[1]
            break
    if not header_data:
        debug_print(f"No header data found for UID {uid}")
        syncer.checkpoint.add_failed_uid(uid)
        return 'fail'
    msg = email.message_from_bytes(header_data if isinstance(header_data, bytes) else header_data.encode('utf-8'))
    date_str = decode_field(msg.get('Date', ''))
    iso_date = parse_email_date(date_str)
    await syncer.db.execute(
        'INSERT OR REPLACE INTO emails(uid, msg_from, msg_to, msg_cc, subject, msg_date, mailbox) VALUES(?,?,?,?,?,?,?)',
        (
            uid,
            decode_field(msg.get('From', '')),
            decode_field(msg.get('To', '')),
            decode_field(msg.get('Cc', '')),
            decode_field(msg.get('Subject', '')),
            iso_date,
            mailbox
        )
    )
    syncer.checkpoint.update_progress(uid)
    if uid in syncer.failed_uids:
        syncer.checkpoint.clear_failed_uid(uid)
    return 'saved'

async def fetch_full_email_syncer(syncer, uid, mailbox):
    debug_print(f"Fetching full email for UID {uid} in mailbox {mailbox}...")
    status, data = await syncer.loop.run_in_executor(
        None,
        lambda: syncer.imap.uid('FETCH', uid, '(BODY.PEEK[])')
    )
    if status != 'OK' or not data:
        print(f"[ERROR] Failed to fetch UID {uid}: status={status}, data_len={len(data) if data else 0}")
        syncer.checkpoint.add_failed_uid(uid)
        return 'fail'
    debug_print(f"Received data for UID {uid}, processing...")
    raw_email = None
    for item in data:
        if isinstance(item, tuple) and len(item) > 1:
            raw_email = item[1]
            break
    if not raw_email:
        if syncer.pbar.n < 10:
            debug_print(f"[DEBUG] No raw email data found for UID {uid} in mailbox {mailbox}. Data: {data}")
        syncer.checkpoint.add_failed_uid(uid)
        return 'fail'
    await syncer.db.execute(
        'INSERT OR REPLACE INTO full_emails(uid, mailbox, raw_email, fetched_at) VALUES(?,?,?,?)',
        (uid, mailbox, raw_email, datetime.datetime.now().isoformat())
    )
    if syncer.pbar.n < 10:
        print(f"[DEBUG] Successfully saved full email for UID {uid} in mailbox {mailbox}.")
    syncer.checkpoint.update_progress(uid)
    if uid in syncer.failed_uids:
        syncer.checkpoint.clear_failed_uid(uid)
    return 'saved'

# Refactored fetch_headers and fetch_full_emails to use EmailSyncer and the above fetch functions
async def fetch_headers(db, host, user, creds, mailbox='INBOX'):
    checkpoint = CheckpointManager()
    loop = asyncio.get_event_loop()
    imap = await loop.run_in_executor(
        None, 
        lambda: imap_oauth2_login(host, user, creds.token)
    )
    try:
        checkpoint.mark_start(mailbox)
        await db.execute('''
            INSERT INTO sync_status (start_time, status, message)
            VALUES (?, 'STARTED', 'Starting sync operation')
        ''', (datetime.datetime.now().isoformat(),))
        async with db.execute('SELECT last_insert_rowid()') as cursor:
            row = await cursor.fetchone()
            last_status_id = row[0] if row else None
        status, data = await loop.run_in_executor(None, lambda: imap.select(mailbox, readonly=True))
        if status != 'OK':
            sys.exit(f"Failed to select mailbox: {status}")
        last_uid_checkpoint = checkpoint.get_last_uid()
        async with db.execute("SELECT MAX(CAST(uid AS INTEGER)) FROM emails") as cursor:
            row = await cursor.fetchone()
        last_uid_db = int(row[0]) if row and row[0] else 0
        if last_uid_checkpoint > 0 and last_uid_db > 0:
            last_uid = min(last_uid_checkpoint, last_uid_db)
        else:
            last_uid = max(last_uid_checkpoint, last_uid_db)
        print(f"Resuming from UID > {last_uid}")
        failed_uids = checkpoint.get_failed_uids()
        if failed_uids:
            print(f"Found {len(failed_uids)} failed UIDs from previous runs. Will retry these.")
        status, data = await loop.run_in_executor(None, lambda: imap.uid('SEARCH', None, 'ALL'))
        if status != 'OK':
            sys.exit(f"Failed to search mailbox: {status}")
        uid_list = data[0].decode().split() if isinstance(data[0], bytes) else data[0].split()
        all_uids = list(map(str, uid_list))
        new_uids = [uid for uid in all_uids if int(uid) > last_uid]
        if failed_uids:
            failed_uids_int = [str(uid) for uid in failed_uids]
            retry_uids = [uid for uid in failed_uids_int if uid in all_uids]
            retry_uids = [uid for uid in retry_uids if uid not in new_uids]
            new_uids.extend(retry_uids)
            new_uids.sort(key=int)
        if not new_uids:
            print('No new messages to fetch.')
            checkpoint.mark_complete()
            await db.execute(f"UPDATE sync_status SET end_time = ?, status = ?, message = ? WHERE id = ?", 
                          (datetime.datetime.now().isoformat(), 'COMPLETED', 'No new messages to fetch', last_status_id))
            await db.commit()
            return
        total_count = len(new_uids)
        await EmailSyncer(db, host, user, creds, mailbox, 'headers', fetch_headers_syncer).run([(uid, mailbox) for uid in new_uids], total_count, 'headers')
    except Exception as e:
        print(f"Error in fetch_headers: {e}")
        try:
            await db.commit()
            await db.execute(f"UPDATE sync_status SET end_time = ?, status = ?, message = ? WHERE id = ?", 
                         (datetime.datetime.now().isoformat(), 'ERROR', str(e)[:200], last_status_id))
            await db.commit()
        except Exception as commit_err:
            print(f"Error committing to database: {commit_err}")
        raise
    finally:
        try:
            await loop.run_in_executor(None, lambda: imap.logout())
        except Exception as e:
            print(f"Error during logout: {e}")

async def fetch_full_emails(db, host, user, creds, mailbox='INBOX'):
    checkpoint = CheckpointManager(CHECKPOINT_FULL_PATH)
    loop = asyncio.get_event_loop()
    imap = await loop.run_in_executor(
        None,
        lambda: imap_oauth2_login(host, user, creds.token)
    )
    try:
        checkpoint.mark_start(mailbox)
        await db.execute('''
            INSERT INTO sync_status (start_time, status, message)
            VALUES (?, 'STARTED', 'Starting full email sync')
        ''', (datetime.datetime.now().isoformat(),))
        async with db.execute('SELECT last_insert_rowid()') as cursor:
            row = await cursor.fetchone()
            last_status_id = row[0] if row else None
        async with db.execute('SELECT uid, mailbox FROM emails WHERE mailbox = ?', (mailbox,)) as cursor:
            all_rows = await cursor.fetchall()
        all_uids = [(str(row[0]), row[1]) for row in all_rows]
        
        print(f"Found {len(all_uids)} total emails in database for mailbox {mailbox}")
        
        async with db.execute('SELECT uid FROM full_emails WHERE mailbox = ?', (mailbox,)) as cursor:
            fetched_rows = await cursor.fetchall()
        fetched_uids = set(str(row[0]) for row in fetched_rows)
        
        print(f"Already fetched {len(fetched_uids)} full emails")
        
        failed_uids = checkpoint.get_failed_uids()
        if failed_uids:
            print(f"Found {len(failed_uids)} failed UIDs from previous full-email fetches. Will retry these.")
            print(f"First 5 failed UIDs: {failed_uids[:5]}")
        
        uids_to_fetch = [(uid, mbox) for (uid, mbox) in all_uids if uid not in fetched_uids]
        retry_uids = [(uid, mbox) for (uid, mbox) in all_uids if uid in failed_uids and uid not in fetched_uids]
        print(f"UIDs needing full email fetch: {len(uids_to_fetch)}")
        print(f"Failed UIDs to retry: {len(retry_uids)}")
        
        for item in retry_uids:
            if item not in uids_to_fetch:
                uids_to_fetch.append(item)
        total_count = len(uids_to_fetch)
        print(f"Total UIDs to fetch: {total_count}")
        
        if not uids_to_fetch:
            checkpoint.mark_complete()
            await db.execute(f"UPDATE sync_status SET end_time = ?, status = ?, message = ? WHERE id = ?", 
                          (datetime.datetime.now().isoformat(), 'COMPLETED', 'No new full emails to fetch', last_status_id))
            await db.commit()
            return
            
        # Sample a few UIDs for debugging
        if len(uids_to_fetch) > 0:
            print(f"Sample UIDs to fetch (first 5): {uids_to_fetch[:5]}")
        
        # Limit number of UIDs to fetch for debugging if needed
        if DEBUG and total_count > 100:
            print(f"DEBUG mode: Limiting to first 100 UIDs for testing")
            uids_to_fetch = uids_to_fetch[:100]
            total_count = len(uids_to_fetch)

        syncer = EmailSyncer(db, host, user, creds, mailbox, 'full', fetch_full_email_syncer)
        await syncer.connect()
        await syncer.run(uids_to_fetch, total_count, 'full emails')
    except Exception as e:
        print(f"Error in fetch_full_emails: {e}")
        try:
            await db.commit()
            await db.execute(f"UPDATE sync_status SET end_time = ?, status = ?, message = ? WHERE id = ?", 
                         (datetime.datetime.now().isoformat(), 'ERROR', str(e)[:200], last_status_id))
            await db.commit()
        except Exception as commit_err:
            print(f"Error committing to database: {commit_err}")
        raise
    finally:
        try:
            await loop.run_in_executor(None, lambda: imap.logout())
        except Exception as e:
            print(f"Error during logout: {e}")

async def main():
    parser = argparse.ArgumentParser(description='Fetch Gmail headers to SQLite using OAuth2')
    parser.add_argument('--db', default='emails.db', help='Path to SQLite database')
    parser.add_argument('--creds', required=True, help='Path to OAuth2 client secrets JSON')
    parser.add_argument('--host', default='imap.gmail.com', help='IMAP host')
    parser.add_argument('--user', required=True, help='Gmail address')
    parser.add_argument('--mailbox', default='INBOX', help='Mailbox name')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--list-mailboxes', action='store_true', help='List available mailboxes and exit')
    parser.add_argument('--mode', choices=['headers', 'full'], default='headers', help='Execution mode: headers (default) or full (fetch full emails)')
    args = parser.parse_args()
    
    global DEBUG
    DEBUG = args.debug or DEBUG

    creds = get_credentials(args.creds)
    
    # If the user wants to list mailboxes, do that and exit
    if args.list_mailboxes:
        await list_mailboxes(args.host, args.user, creds)
        return
    
    async with aiosqlite.connect(args.db) as db:
        await setup_schema(db)
        if args.mode == 'headers':
            await fetch_headers(db, args.host, args.user, creds, args.mailbox)
        elif args.mode == 'full':
            await fetch_full_emails(db, args.host, args.user, creds, args.mailbox)
    print('Done.')

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram terminated by user")
    except Exception as e:
        print(f"Program terminated due to error: {e}")
        sys.exit(1)
