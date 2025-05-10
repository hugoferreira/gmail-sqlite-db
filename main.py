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
CHECKPOINT_PATH = 'checkpoint.json'  # To track sync state
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

async def fetch_headers(db, host, user, creds, mailbox='INBOX'):
    # Initialize the checkpoint manager
    checkpoint = CheckpointManager()
    
    # Check if a previous run was interrupted
    if checkpoint.was_interrupted():
        print(f"A previous sync was interrupted. Resuming from last checkpoint.")
        
    # Run the synchronous IMAP authentication in a thread pool
    loop = asyncio.get_event_loop()
    imap = await loop.run_in_executor(
        None, 
        lambda: imap_oauth2_login(host, user, creds.token)
    )
    
    try:
        # Mark the start of the sync operation
        checkpoint.mark_start(mailbox)
        
        # Record sync start
        await db.execute('''
            INSERT INTO sync_status (start_time, status, message)
            VALUES (?, 'STARTED', 'Starting sync operation')
        ''', (datetime.datetime.now().isoformat(),))
        async with db.execute('SELECT last_insert_rowid()') as cursor:
            row = await cursor.fetchone()
            last_status_id = row[0] if row else None
        
        # Start transaction for better SQLite performance during heavy inserts
        try:
            await db.execute("BEGIN TRANSACTION")
        except Exception as e:
            debug_print(f"Transaction already started: {e}")
            # If a transaction is already active, we can continue
        
        # Select mailbox
        status, data = await loop.run_in_executor(None, lambda: imap.select(mailbox, readonly=True))
        if status != 'OK':
            sys.exit(f"Failed to select mailbox: {status}")
            
        # Determine resume point - use the max of:
        # 1. Last successfully processed UID from checkpoint
        # 2. Last UID in database (fallback)
        last_uid_checkpoint = checkpoint.get_last_uid()
        
        async with db.execute("SELECT MAX(CAST(uid AS INTEGER)) FROM emails") as cursor:
            row = await cursor.fetchone()
        last_uid_db = int(row[0]) if row and row[0] else 0
        
        # Use the lower of the two to be safe - we'd rather re-process some emails
        # than skip any if the database is ahead of the checkpoint
        if last_uid_checkpoint > 0 and last_uid_db > 0:
            last_uid = min(last_uid_checkpoint, last_uid_db)
        else:
            last_uid = max(last_uid_checkpoint, last_uid_db)
            
        print(f"Resuming from UID > {last_uid}")
        
        # Check for failed UIDs from previous runs
        failed_uids = checkpoint.get_failed_uids()
        if failed_uids:
            print(f"Found {len(failed_uids)} failed UIDs from previous runs. Will retry these.")
        
        # Fetch all UIDs
        status, data = await loop.run_in_executor(None, lambda: imap.uid('SEARCH', None, 'ALL'))
        if status != 'OK':
            sys.exit(f"Failed to search mailbox: {status}")
            
        uid_list = data[0].decode().split() if isinstance(data[0], bytes) else data[0].split()
        all_uids = list(map(int, uid_list))
        
        # Get new UIDs not yet processed and add any previously failed UIDs
        new_uids = [uid for uid in all_uids if uid > last_uid]
        if failed_uids:
            # Convert failed UIDs to integers for sorting
            failed_uids_int = [int(uid) for uid in failed_uids]
            # Add them to new_uids if they exist in all_uids (still on server)
            retry_uids = [uid for uid in failed_uids_int if uid in all_uids]
            # Add any retry UIDs that are not already in new_uids (to avoid duplicates)
            retry_uids = [uid for uid in retry_uids if uid not in new_uids]
            new_uids.extend(retry_uids)
            # Sort to process in UID order
            new_uids.sort()
            
        if not new_uids:
            print('No new messages to fetch.')
            checkpoint.mark_complete()
            await db.execute(f"UPDATE sync_status SET end_time = ?, status = ?, message = ? WHERE id = ?", 
                          (datetime.datetime.now().isoformat(), 'COMPLETED', 'No new messages to fetch', last_status_id))
            await db.commit()
            return
        
        total_count = len(new_uids)
        print(f"Found {total_count} new emails to fetch")
        
        # Process in chunks with progress bar
        processed_count = 0
        skipped_count = 0
        saved_count = 0
        emails_since_commit = 0
        commit_interval = 1  # Commit after every chunk for safety
        pbar = tqdm(total=total_count, desc='Fetching headers')
        
        try:
            # Process emails in chunks
            for chunk_idx, i in enumerate(range(0, len(new_uids), CHUNK_SIZE)):
                chunk = new_uids[i:i + CHUNK_SIZE]
                if not chunk:
                    continue
                    
                range_str = f"{chunk[0]}:{chunk[-1]}"
                debug_print(f"\nProcessing range: {range_str}")
                
                try:
                    # Manually fetch UIDs first
                    status, data = await loop.run_in_executor(
                        None, 
                        lambda: imap.uid('FETCH', range_str, '(UID)')
                    )
                    
                    if status != 'OK':
                        print(f"Failed to fetch UIDs for range {range_str}: {status}")
                        # Mark these UIDs as failed in the checkpoint
                        for uid in chunk:
                            checkpoint.add_failed_uid(str(uid))
                        pbar.update(len(chunk))
                        skipped_count += len(chunk)
                        continue
                    
                    # Extract the actual UIDs from the response
                    actual_uids = []
                    for item in data:
                        if isinstance(item, bytes):
                            uid = extract_uid(item)
                            if uid:
                                actual_uids.append(uid)
                    
                    debug_print(f"Found {len(actual_uids)} actual UIDs in range")
                    
                    if not actual_uids:
                        print(f"No valid UIDs found in range {range_str}")
                        pbar.update(len(chunk))
                        skipped_count += len(chunk)
                        continue
                    
                    # Now fetch headers for just the first 10 UIDs in debug mode
                    test_count = min(50, len(actual_uids)) if DEBUG else len(actual_uids)
                    test_uids = actual_uids[:test_count]
                    
                    debug_print(f"Fetching headers for {test_count} test UIDs")
                    
                    for uid in test_uids:
                        # Check if this was a previously failed UID
                        is_retry = uid in failed_uids
                        
                        # Fetch headers for a single UID
                        status, data = await loop.run_in_executor(
                            None, 
                            lambda: imap.uid('FETCH', uid, '(BODY.PEEK[HEADER.FIELDS (FROM TO CC SUBJECT DATE)])')
                        )
                        
                        if status != 'OK':
                            debug_print(f"Failed to fetch headers for UID {uid}: {status}")
                            # Mark this UID as failed in the checkpoint
                            checkpoint.add_failed_uid(uid)
                            continue
                        
                        debug_print(f"Response for UID {uid}: {len(data)} items")
                        
                        try:
                            # Find the header data in the response
                            header_data = None
                            for item in data:
                                if isinstance(item, tuple) and len(item) > 1:
                                    header_data = item[1]
                                    break
                            
                            if not header_data:
                                debug_print(f"No header data found for UID {uid}")
                                # Mark this UID as failed in the checkpoint
                                checkpoint.add_failed_uid(uid)
                                continue
                                
                            # Process the header data
                            msg = email.message_from_bytes(header_data if isinstance(header_data, bytes) else header_data.encode('utf-8'))
                            
                            # Parse the date into ISO format
                            date_str = decode_field(msg.get('Date', ''))
                            iso_date = parse_email_date(date_str)
                            
                            # Insert this email
                            await db.execute(
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
                            saved_count += 1
                            processed_count += 1
                            
                            # Update progress bar
                            pbar.update(1)
                            
                            # Update checkpoint - mark as processed
                            checkpoint.update_progress(uid)
                            
                            # If this was a retry of a failed UID, clear it from the failed list
                            if is_retry:
                                checkpoint.clear_failed_uid(uid)
                                
                            # Commit after processing a certain number of emails
                            emails_since_commit += 1
                            if emails_since_commit >= EMAILS_PER_COMMIT:
                                debug_print(f"Committing after {emails_since_commit} emails")
                                # Save state before committing
                                checkpoint.save_state()
                                
                                try:
                                    await db.commit()
                                    
                                    # Start a new transaction
                                    try:
                                        await db.execute("BEGIN TRANSACTION")
                                    except Exception as e:
                                        debug_print(f"Transaction already started: {e}")
                                except Exception as commit_err:
                                    print(f"Error committing transaction: {commit_err}")
                                
                                emails_since_commit = 0
                                
                        except Exception as e:
                            debug_print(f"Error processing UID {uid}: {e}")
                            # Mark this UID as failed in the checkpoint
                            checkpoint.add_failed_uid(uid)
                    
                    # If in debug mode, we only process the first chunk
                    if DEBUG:
                        # Commit what we have
                        await db.commit()
                        debug_print(f"Debug mode: processed {saved_count} emails in first chunk")
                        break
                    
                    # Commit periodically
                    if chunk_idx % commit_interval == 0:
                        # Save state of transaction before committing
                        checkpoint.save_state()
                        
                        # Try to commit what we have
                        try:
                            await db.commit()
                            
                            # Start a new transaction
                            try:
                                await db.execute("BEGIN TRANSACTION")
                            except Exception as e:
                                debug_print(f"Transaction already started: {e}")
                        except Exception as commit_err:
                            print(f"Error committing transaction: {commit_err}")
                        
                        debug_print(f"Committed after chunk {chunk_idx}, saved {saved_count} emails so far")
                
                except Exception as e:
                    print(f"Error fetching range {range_str}: {e}")
                    
                    # Mark all UIDs in this chunk as failed
                    for uid in actual_uids:
                        checkpoint.add_failed_uid(uid)
                        
                    pbar.update(len(chunk))
                    skipped_count += len(chunk)
                    
                    # Try to commit what we have
                    try:
                        await db.commit()
                    except Exception as commit_err:
                        print(f"Error committing transaction: {commit_err}")
                
                # Small delay to avoid hammering the server
                await asyncio.sleep(0.1)
            
            # Final commit
            await db.commit()
            
            # Update the sync status
            await db.execute(f"UPDATE sync_status SET end_time = ?, status = ?, message = ?, last_uid = ? WHERE id = ?", 
                          (datetime.datetime.now().isoformat(), 'COMPLETED', f'Successfully processed {saved_count} emails', 
                           checkpoint.get_last_uid(), last_status_id))
            await db.commit()
            
            # Mark the sync as complete in the checkpoint
            checkpoint.mark_complete()
            
        except KeyboardInterrupt:
            print("\nOperation interrupted by user. Saving progress...")
            try:
                await db.commit()
                # Update the sync status
                await db.execute(f"UPDATE sync_status SET end_time = ?, status = ?, message = ?, last_uid = ? WHERE id = ?", 
                             (datetime.datetime.now().isoformat(), 'INTERRUPTED', 'Interrupted by user', 
                              checkpoint.get_last_uid(), last_status_id))
                await db.commit()
                
                # Save the checkpoint state
                checkpoint.save_state()
                
                print(f"Progress saved. Last processed UID: {checkpoint.get_last_uid()}")
                print(f"Failed UIDs count: {len(checkpoint.get_failed_uids())}")
            except Exception as e:
                print(f"Error saving progress: {e}")
            raise
            
        except Exception as e:
            print(f"\nUnexpected error: {e}")
            try:
                await db.commit()
                # Update the sync status
                await db.execute(f"UPDATE sync_status SET end_time = ?, status = ?, message = ?, last_uid = ? WHERE id = ?", 
                             (datetime.datetime.now().isoformat(), 'ERROR', str(e)[:200], 
                              checkpoint.get_last_uid(), last_status_id))
                await db.commit()
                
                # Save the checkpoint state
                checkpoint.save_state()
                
                print(f"Partial progress saved. Last processed UID: {checkpoint.get_last_uid()}")
                print(f"Failed UIDs count: {len(checkpoint.get_failed_uids())}")
            except Exception as commit_err:
                print(f"Error saving progress: {commit_err}")
            raise
            
        finally:
            pbar.n = processed_count
            pbar.refresh()
            pbar.close()
            
            print(f"Successfully processed {processed_count} messages")
            print(f"Saved {saved_count} messages to database")
            print(f"Skipped {skipped_count} messages")
            
    except Exception as e:
        print(f"Error in fetch_headers: {e}")
        try:
            await db.commit()
            # Update the sync status
            await db.execute(f"UPDATE sync_status SET end_time = ?, status = ?, message = ? WHERE id = ?", 
                         (datetime.datetime.now().isoformat(), 'ERROR', str(e)[:200], last_status_id))
            await db.commit()
        except Exception as commit_err:
            print(f"Error committing to database: {commit_err}")
        raise
        
    finally:
        try:
            # Try to logout gracefully
            try:
                await loop.run_in_executor(None, lambda: imap.logout())
            except imaplib.IMAP4.error as e:
                # These are expected IMAP errors that can be safely ignored during logout
                print(f"Normal IMAP error during logout: {e}")
            except ConnectionError as e:
                # Connection might be closed already
                print(f"Connection error during logout (can be ignored): {e}")
            except Exception as e:
                # Other unexpected errors
                print(f"Error during logout: {e}")
        except Exception as e:
            # Catch-all for any other unexpected errors
            print(f"Unexpected error during logout: {e}")

async def main():
    parser = argparse.ArgumentParser(description='Fetch Gmail headers to SQLite using OAuth2')
    parser.add_argument('--db', default='emails.db', help='Path to SQLite database')
    parser.add_argument('--creds', required=True, help='Path to OAuth2 client secrets JSON')
    parser.add_argument('--host', default='imap.gmail.com', help='IMAP host')
    parser.add_argument('--user', required=True, help='Gmail address')
    parser.add_argument('--mailbox', default='INBOX', help='Mailbox name')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--list-mailboxes', action='store_true', help='List available mailboxes and exit')
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
        await fetch_headers(db, args.host, args.user, creds, args.mailbox)
    print('Done.')

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram terminated by user")
    except Exception as e:
        print(f"Program terminated due to error: {e}")
        sys.exit(1)
