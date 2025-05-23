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
from typing import List, Dict, Any

# Third-party imports
import aiosqlite
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from tqdm import tqdm
import tabulate
import subprocess
import shutil
import hashlib

# OAuth2 setup
SCOPES = ['https://mail.google.com/']
TOKEN_PATH = 'token.json'
CHUNK_SIZE = 250  # Reduced for more reliable processing and frequent commits
EMAILS_PER_COMMIT = 20  # Commit after processing this many emails
DEBUG = False   # Enable debug mode - set to False by default for full processing

# Predefined queries
QUERIES = {
    'top_senders': {
        'name': 'Top Email Senders',
        'description': 'Shows the top email senders by count',
        'query': '''
            SELECT msg_from, COUNT(*) as count 
            FROM emails 
            GROUP BY msg_from 
            ORDER BY count DESC 
            LIMIT ?;
        ''',
        'params': {'limit': 10},
    },
    'email_addresses': {
        'name': 'Extracted Email Addresses',
        'description': 'Extracts and counts unique email addresses from the sender field',
        'setup': '''
            CREATE VIEW IF NOT EXISTS email_senders AS 
            SELECT 
                SUBSTR(msg_from, INSTR(msg_from, '<') + 1, INSTR(msg_from, '>') - INSTR(msg_from, '<') - 1) AS email_address, 
                COUNT(*) as count 
            FROM emails 
            WHERE INSTR(msg_from, '<') > 0 AND INSTR(msg_from, '>') > INSTR(msg_from, '<') 
            GROUP BY email_address 
            ORDER BY count DESC;
        ''',
        'query': '''
            SELECT * FROM email_senders LIMIT ?;
        ''',
        'params': {'limit': 20},
    },
    'email_domains': {
        'name': 'Email Domains',
        'description': 'Shows the distribution of email domains',
        'setup': '''
            CREATE VIEW IF NOT EXISTS domain_senders AS 
            SELECT 
                SUBSTR(msg_from, INSTR(msg_from, '@') + 1, INSTR(msg_from, '>') - INSTR(msg_from, '@') - 1) AS domain, 
                COUNT(*) as count 
            FROM emails 
            WHERE INSTR(msg_from, '@') > 0 AND INSTR(msg_from, '>') > INSTR(msg_from, '@') 
            GROUP BY domain 
            ORDER BY count DESC;
        ''',
        'query': '''
            SELECT * FROM domain_senders LIMIT ?;
        ''',
        'params': {'limit': 20},
    },
    'date_range': {
        'name': 'Emails by Date Range',
        'description': 'Shows emails within a specified date range',
        'query': '''
            SELECT uid, msg_from, msg_to, subject, msg_date 
            FROM emails 
            WHERE msg_date BETWEEN ? AND ? 
            ORDER BY msg_date DESC
            LIMIT ?;
        ''',
        'params': {
            'start_date': datetime.date.today().replace(day=1).isoformat(),  # First day of current month
            'end_date': datetime.date.today().isoformat(),  # Today
            'limit': 50
        },
    },
    'mailbox_count': {
        'name': 'Count by Mailbox',
        'description': 'Shows the distribution of emails across mailboxes',
        'query': '''
            SELECT 
                mailbox, 
                COUNT(*) as email_count 
            FROM emails 
            GROUP BY mailbox 
            ORDER BY email_count DESC;
        ''',
    },
    'large_attachments': {
        'name': 'Large Emails with Attachments',
        'description': 'Shows the largest emails with attachments',
        'query': '''
            SELECT e.subject, f.message_size_kb, e.msg_date, e.msg_from
            FROM emails e
            JOIN full_emails f ON e.uid = f.uid AND e.mailbox = f.mailbox
            WHERE f.has_attachments = 1
            ORDER BY f.message_size_kb DESC
            LIMIT ?;
        ''',
        'params': {'limit': 20},
    },
    'emails_with_images': {
        'name': 'Emails with Images',
        'description': 'Shows emails containing embedded images',
        'query': '''
            SELECT e.subject, e.msg_from, e.msg_date
            FROM emails e
            JOIN full_emails f ON e.uid = f.uid AND e.mailbox = f.mailbox
            WHERE f.has_images = 1
            ORDER BY e.msg_date DESC
            LIMIT ?;
        ''',
        'params': {'limit': 50},
    },
    'thread': {
        'name': 'Email Thread',
        'description': 'Shows all emails in a conversation thread',
        'query': '''
            WITH RECURSIVE thread(uid, mailbox, message_id, level) AS (
                -- Start with a specific message ID
                SELECT uid, mailbox, message_id, 0
                FROM full_emails
                WHERE message_id = ?
                
                UNION ALL
                
                -- Find all replies
                SELECT f.uid, f.mailbox, f.message_id, t.level + 1
                FROM full_emails f
                JOIN thread t ON f.in_reply_to = t.message_id
            )
            SELECT e.subject, e.msg_from, e.msg_date, t.level
            FROM thread t
            JOIN emails e ON t.uid = e.uid AND t.mailbox = e.mailbox
            ORDER BY e.msg_date;
        ''',
        'params': {'message_id': '<example-message-id@domain.com>'},
    },
    'summary': {
        'name': 'Database Summary',
        'description': 'Shows a summary of the database contents',
        'query': '''
            SELECT 
                (SELECT COUNT(*) FROM emails) AS total_emails,
                (SELECT COUNT(*) FROM full_emails) AS full_emails,
                (SELECT COUNT(DISTINCT mailbox) FROM emails) AS mailbox_count,
                (SELECT COUNT(DISTINCT msg_from) FROM emails) AS unique_senders,
                (SELECT COUNT(*) FROM full_emails WHERE has_attachments = 1) AS emails_with_attachments,
                (SELECT COUNT(*) FROM full_emails WHERE has_images = 1) AS emails_with_images;
        ''',
    },
    'recent': {
        'name': 'Recent Emails',
        'description': 'Shows the most recent emails',
        'query': '''
            SELECT uid, msg_from, subject, msg_date, mailbox
            FROM emails
            ORDER BY msg_date DESC
            LIMIT ?;
        ''',
        'params': {'limit': 20},
    },
}

METRIC_QUERIES = {
    'emails': {
        'label': 'Number of Emails',
        'monthly_sql': '''
            SELECT strftime('%m', msg_date) as period, COUNT(*)
            FROM emails
            WHERE strftime('%Y', msg_date) = ?
            GROUP BY period
            ORDER BY period
        ''',
        'calendar_sql': '''
            SELECT strftime('%Y-%m-%d', msg_date) as period, COUNT(*)
            FROM emails
            WHERE strftime('%Y', msg_date) = ?
            GROUP BY period
            ORDER BY period
        '''
    },
    'attachments': {
        'label': 'Number of Attachments',
        'monthly_sql': '''
            SELECT strftime('%m', e.msg_date) as period, COUNT(*)
            FROM email_attachments ea
            JOIN emails e ON ea.uid = e.uid AND ea.mailbox = e.mailbox
            WHERE strftime('%Y', e.msg_date) = ?
            GROUP BY period
            ORDER BY period
        ''',
        'calendar_sql': '''
            SELECT strftime('%Y-%m-%d', e.msg_date) as period, COUNT(*)
            FROM email_attachments ea
            JOIN emails e ON ea.uid = e.uid AND ea.mailbox = e.mailbox
            WHERE strftime('%Y', e.msg_date) = ?
            GROUP BY period
            ORDER BY period
        '''
    },
    'attachment_size': {
        'label': 'Total Attachment Size (bytes)',
        'monthly_sql': '''
            SELECT strftime('%m', e.msg_date) as period, SUM(ab.size)
            FROM email_attachments ea
            JOIN emails e ON ea.uid = e.uid AND ea.mailbox = e.mailbox
            JOIN attachment_blobs ab ON ea.sha256 = ab.sha256
            WHERE strftime('%Y', e.msg_date) = ?
            GROUP BY period
            ORDER BY period
        ''',
        'calendar_sql': '''
            SELECT strftime('%Y-%m-%d', e.msg_date) as period, SUM(ab.size)
            FROM email_attachments ea
            JOIN emails e ON ea.uid = e.uid AND ea.mailbox = e.mailbox
            JOIN attachment_blobs ab ON ea.sha256 = ab.sha256
            WHERE strftime('%Y', e.msg_date) = ?
            GROUP BY period
            ORDER BY period
        '''
    },
    'unique_attachments': {
        'label': 'Unique Attachments',
        'monthly_sql': '''
            SELECT strftime('%m', e.msg_date) as period, COUNT(DISTINCT ea.sha256)
            FROM email_attachments ea
            JOIN emails e ON ea.uid = e.uid AND ea.mailbox = e.mailbox
            WHERE strftime('%Y', e.msg_date) = ?
            GROUP BY period
            ORDER BY period
        ''',
        'calendar_sql': '''
            SELECT strftime('%Y-%m-%d', e.msg_date) as period, COUNT(DISTINCT ea.sha256)
            FROM email_attachments ea
            JOIN emails e ON ea.uid = e.uid AND ea.mailbox = e.mailbox
            WHERE strftime('%Y', e.msg_date) = ?
            GROUP BY period
            ORDER BY period
        '''
    },
    'avg_attachment_size': {
        'label': 'Avg. Attachment Size (bytes)',
        'monthly_sql': '''
            SELECT strftime('%m', e.msg_date) as period, AVG(ab.size)
            FROM email_attachments ea
            JOIN emails e ON ea.uid = e.uid AND ea.mailbox = e.mailbox
            JOIN attachment_blobs ab ON ea.sha256 = ab.sha256
            WHERE strftime('%Y', e.msg_date) = ?
            GROUP BY period
            ORDER BY period
        ''',
        'calendar_sql': '''
            SELECT strftime('%Y-%m-%d', e.msg_date) as period, AVG(ab.size)
            FROM email_attachments ea
            JOIN emails e ON ea.uid = e.uid AND ea.mailbox = e.mailbox
            JOIN attachment_blobs ab ON ea.sha256 = ab.sha256
            WHERE strftime('%Y', e.msg_date) = ?
            GROUP BY period
            ORDER BY period
        '''
    },
}

def debug_print(*args, **kwargs):
    if DEBUG:
        print(*args, **kwargs)

# Unified checkpoint system
class CheckpointManager:
    def __init__(self, mode='headers', mailbox=None):
        """Initialize checkpoint manager with a specific mode
        
        Args:
            mode: Sync mode - 'headers' or 'full'
            mailbox: Current mailbox name - used for mailbox-specific state
        """
        self.mode = mode
        self.mailbox = mailbox if mailbox else 'INBOX'
        self.checkpoint_path = f'checkpoint_{mode}.json'
        self.state = self._load_state()
        
        # Initialize mailbox state if it doesn't exist
        if self.mailbox not in self.state:
            self.state[self.mailbox] = {
                'last_uid': 0,        # Last successfully processed UID
                'failed_uids': [],    # UIDs that failed to process
                'in_progress': False, # Whether a sync was interrupted
                'timestamp': None     # Last sync timestamp
            }
        
    def _load_state(self):
        """Load checkpoint state from file if it exists"""
        if os.path.exists(self.checkpoint_path):
            try:
                with open(self.checkpoint_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Error loading checkpoint state: {e}")
        
        # Default state if no checkpoint file exists or loading fails
        return {}
    
    def save_state(self):
        """Save current state to checkpoint file"""
        self.state[self.mailbox]['timestamp'] = datetime.datetime.now().isoformat()
        try:
            with open(self.checkpoint_path, 'w') as f:
                json.dump(self.state, f, indent=2)
        except Exception as e:
            print(f"Error saving checkpoint state: {e}")
    
    def set_mailbox(self, mailbox):
        """Set current mailbox and initialize state if needed"""
        self.mailbox = mailbox if mailbox else 'INBOX'
        if self.mailbox not in self.state:
            self.state[self.mailbox] = {
                'last_uid': 0,
                'failed_uids': [],
                'in_progress': False,
                'timestamp': None
            }

    def mark_start(self):
        """Mark the start of a sync operation"""
        self.state[self.mailbox]['in_progress'] = True
        self.save_state()
    
    def mark_complete(self):
        """Mark the completion of a sync operation"""
        self.state[self.mailbox]['in_progress'] = False
        self.save_state()
    
    def update_progress(self, uid):
        """Update the last processed UID"""
        # Only update if this UID is greater than the last one
        uid_int = int(uid)
        if uid_int > self.state[self.mailbox]['last_uid']:
            self.state[self.mailbox]['last_uid'] = uid_int
            # Only save periodically to avoid excessive disk writes
            if uid_int % 100 == 0:  # Save more frequently (was 500)
                self.save_state()
    
    def add_failed_uid(self, uid):
        """Add a UID to the failed list"""
        if uid not in self.state[self.mailbox]['failed_uids']:
            self.state[self.mailbox]['failed_uids'].append(uid)
            # Only save periodically to avoid excessive disk writes
            if len(self.state[self.mailbox]['failed_uids']) % 100 == 0:
                self.save_state()
    
    def get_last_uid(self):
        """Get the last successfully processed UID"""
        return self.state[self.mailbox]['last_uid']
    
    def get_failed_uids(self):
        """Get the list of failed UIDs"""
        return self.state[self.mailbox]['failed_uids']
    
    def clear_failed_uid(self, uid):
        """Remove a UID from the failed list if it's been processed successfully"""
        if uid in self.state[self.mailbox]['failed_uids']:
            self.state[self.mailbox]['failed_uids'].remove(uid)
            
    def was_interrupted(self):
        """Check if a previous sync was interrupted"""
        return self.state[self.mailbox]['in_progress']

# Database connection manager
class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.db = None

    async def connect(self):
        """Connect to the database"""
        self.db = await aiosqlite.connect(self.db_path)
        await self.setup_schema()
        return self.db
        
    async def close(self):
        """Close the database connection"""
        if self.db:
            await self.db.commit()
            await self.db.close()
            
    async def commit_with_retry(self, max_retries=3):
        """Commit transaction with retry logic"""
        for attempt in range(max_retries):
            try:
                await self.db.commit()
                try:
                    await self.db.execute("BEGIN TRANSACTION")
                except:
                    pass  # Transaction already started
                return True
            except Exception as e:
                if attempt == max_retries - 1:
                    print(f"Failed to commit after {max_retries} attempts: {e}")
                    return False
                await asyncio.sleep(0.1 * (attempt + 1))  # Exponential backoff
        
    async def setup_schema(self):
        """Set up the database schema"""
        # Performance pragmas for better SQLite performance
        await self.db.execute("PRAGMA journal_mode=WAL;")
        await self.db.execute("PRAGMA synchronous=NORMAL;")
        await self.db.execute("PRAGMA temp_store=MEMORY;")
        await self.db.execute("PRAGMA cache_size=-50000;")  # Use about 50MB of memory for caching
        await self.db.execute("PRAGMA foreign_keys=OFF;")   # Disable foreign key checks for imports
        
        # Create emails table if it doesn't exist
        await self.db.execute('''
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
        await self.db.execute('CREATE INDEX IF NOT EXISTS idx_from ON emails(msg_from)')
        await self.db.execute('CREATE INDEX IF NOT EXISTS idx_to ON emails(msg_to)')
        await self.db.execute('CREATE INDEX IF NOT EXISTS idx_cc ON emails(msg_cc)')
        await self.db.execute('CREATE INDEX IF NOT EXISTS idx_date ON emails(msg_date)')
        await self.db.execute('CREATE INDEX IF NOT EXISTS idx_mailbox ON emails(mailbox)')
        
        # Create sync_status table
        await self.db.execute('''
            CREATE TABLE IF NOT EXISTS sync_status (
                id INTEGER PRIMARY KEY,
                last_uid INTEGER,
                start_time TEXT,
                end_time TEXT,
                status TEXT,
                message TEXT
            )
        ''')
        
        # Check and create full_emails table with generated columns
        await self._setup_full_emails_table()
        
        await self.db.commit()
        
    async def _setup_full_emails_table(self):
        """Set up the full_emails table with all generated columns"""
        # Check if table exists
        table_exists = False
        need_migration = False
        
        async with self.db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='full_emails'") as cursor:
            row = await cursor.fetchone()
            if row:
                table_exists = True
                
                # Check if it has the generated columns
                async with self.db.execute("PRAGMA table_xinfo(full_emails)") as cursor:
                    columns = await cursor.fetchall()
                    has_generated_columns = False
                    for col in columns:
                        if col[1] == 'has_attachments' and col[6] > 0:  # col[6] > 0 means it's a generated column
                            has_generated_columns = True
                            break
                    
                    if not has_generated_columns:
                        need_migration = True
                        print("Migrating full_emails table to add generated columns...")
        
        if need_migration and table_exists:
            # Create new table with generated columns
            await self._create_full_emails_table('full_emails_new')
            
            # Copy data from old table to new table
            print("Copying email data to new table with generated columns...")
            await self.db.execute("INSERT INTO full_emails_new (uid, mailbox, raw_email, fetched_at) SELECT uid, mailbox, raw_email, fetched_at FROM full_emails")
            
            # Drop old table and rename new one
            await self.db.execute("DROP TABLE full_emails")
            await self.db.execute("ALTER TABLE full_emails_new RENAME TO full_emails")
            
            # Create indexes
            await self._create_full_emails_indexes()
            
            print("Migration complete!")
        elif not table_exists:
            # Create table with generated columns
            await self._create_full_emails_table('full_emails')
            await self._create_full_emails_indexes()
        else:
            # Check and create any missing indexes
            await self._ensure_full_emails_indexes()
    
    async def _create_full_emails_table(self, table_name):
        """Create the full_emails table with all generated columns"""
        await self.db.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                uid TEXT PRIMARY KEY,
                mailbox TEXT,
                raw_email BLOB,
                fetched_at TEXT,
                has_attachments BOOLEAN GENERATED ALWAYS AS (
                    instr(raw_email, 'Content-Disposition: attachment') > 0 
                    OR instr(raw_email, 'Content-Type: image/') > 0
                    OR instr(raw_email, 'Content-Type: application/') > 0
                    OR instr(raw_email, 'Content-Type: audio/') > 0
                    OR instr(raw_email, 'Content-Type: video/') > 0
                    OR (instr(raw_email, 'Content-Disposition: inline') > 0 AND instr(raw_email, 'filename=') > 0)
                ) VIRTUAL,
                message_size_kb INTEGER GENERATED ALWAYS AS (
                    length(raw_email) / 1024
                ) VIRTUAL,
                is_html BOOLEAN GENERATED ALWAYS AS (
                    instr(raw_email, 'Content-Type: text/html') > 0
                ) VIRTUAL,
                is_plain_text BOOLEAN GENERATED ALWAYS AS (
                    instr(raw_email, 'Content-Type: text/plain') > 0
                ) VIRTUAL,
                has_images BOOLEAN GENERATED ALWAYS AS (
                    instr(raw_email, 'Content-Type: image/') > 0
                ) VIRTUAL,
                in_reply_to TEXT GENERATED ALWAYS AS (
                    CASE 
                        WHEN instr(raw_email, 'In-Reply-To: ') > 0 
                        THEN substr(
                            raw_email,
                            instr(raw_email, 'In-Reply-To: ') + 13,
                            instr(substr(raw_email, instr(raw_email, 'In-Reply-To: ') + 13), CHAR(10)) - 1
                        )
                        ELSE NULL
                    END
                ) VIRTUAL,
                message_id TEXT GENERATED ALWAYS AS (
                    CASE 
                        WHEN instr(raw_email, 'Message-ID: ') > 0 
                        THEN substr(
                            raw_email,
                            instr(raw_email, 'Message-ID: ') + 12,
                            instr(substr(raw_email, instr(raw_email, 'Message-ID: ') + 12), CHAR(10)) - 1
                        )
                        ELSE NULL
                    END
                ) VIRTUAL
            )
        ''')
    
    async def _create_full_emails_indexes(self):
        """Create all indexes for the full_emails table"""
        print("Creating indexes for optimized queries...")
        await self.db.execute("CREATE INDEX IF NOT EXISTS idx_full_emails_has_attachments ON full_emails(has_attachments)")
        await self.db.execute("CREATE INDEX IF NOT EXISTS idx_full_emails_message_size ON full_emails(message_size_kb)")
        await self.db.execute("CREATE INDEX IF NOT EXISTS idx_full_emails_is_html ON full_emails(is_html)")
        await self.db.execute("CREATE INDEX IF NOT EXISTS idx_full_emails_has_images ON full_emails(has_images)")
        await self.db.execute("CREATE INDEX IF NOT EXISTS idx_full_emails_in_reply_to ON full_emails(in_reply_to)")
        await self.db.execute("CREATE INDEX IF NOT EXISTS idx_full_emails_message_id ON full_emails(message_id)")
        await self.db.execute("CREATE INDEX IF NOT EXISTS idx_full_emails_mailbox ON full_emails(mailbox)")
    
    async def _ensure_full_emails_indexes(self):
        """Check and create any missing indexes for the full_emails table"""
        print("Checking and creating missing indexes...")
        
        # First check if table exists
        async with self.db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='full_emails'") as cursor:
            row = await cursor.fetchone()
            if not row:
                # Table doesn't exist, create it first
                await self._create_full_emails_table('full_emails')
                await self._create_full_emails_indexes()
                return
        
        # Check if columns exist by querying table info
        columns = {}
        async with self.db.execute("PRAGMA table_xinfo(full_emails)") as cursor:
            rows = await cursor.fetchall()
            for col in rows:
                columns[col[1]] = {
                    'name': col[1],
                    'is_generated': col[6] > 0  # col[6] > 0 means it's a generated column
                }
        
        # If missing important generated columns, recreate the table
        required_columns = ['has_attachments', 'message_size_kb', 'is_html', 
                           'is_plain_text', 'has_images', 'in_reply_to', 'message_id']
        
        missing_columns = [col for col in required_columns if col not in columns]
        if missing_columns:
            print(f"Missing columns in full_emails: {missing_columns}. Recreating table...")
            # Get current data
            existing_data = []
            try:
                async with self.db.execute("SELECT uid, mailbox, raw_email, fetched_at FROM full_emails") as cursor:
                    existing_data = await cursor.fetchall()
            except Exception as e:
                print(f"Error fetching existing data: {e}")
            
            # Create new table
            await self.db.execute("DROP TABLE IF EXISTS full_emails_new")
            await self._create_full_emails_table('full_emails_new')
            
            # Copy data
            if existing_data:
                print(f"Copying {len(existing_data)} existing emails to new table structure...")
                for row in existing_data:
                    await self.db.execute(
                        "INSERT INTO full_emails_new (uid, mailbox, raw_email, fetched_at) VALUES (?, ?, ?, ?)",
                        row
                    )
            
            # Replace old table
            await self.db.execute("DROP TABLE IF EXISTS full_emails")
            await self.db.execute("ALTER TABLE full_emails_new RENAME TO full_emails")
        
        # Now check and create any missing indexes
        index_names = [
            "idx_full_emails_has_attachments",
            "idx_full_emails_message_size",
            "idx_full_emails_is_html",
            "idx_full_emails_has_images",
            "idx_full_emails_in_reply_to",
            "idx_full_emails_message_id",
            "idx_full_emails_mailbox"
        ]
        
        for index_name in index_names:
            async with self.db.execute(f"SELECT name FROM sqlite_master WHERE type='index' AND name='{index_name}'") as cursor:
                row = await cursor.fetchone()
                if not row:
                    try:
                        # Create the missing index
                        if index_name == "idx_full_emails_has_attachments":
                            await self.db.execute("CREATE INDEX IF NOT EXISTS idx_full_emails_has_attachments ON full_emails(has_attachments)")
                        elif index_name == "idx_full_emails_message_size":
                            await self.db.execute("CREATE INDEX IF NOT EXISTS idx_full_emails_message_size ON full_emails(message_size_kb)")
                        elif index_name == "idx_full_emails_is_html":
                            await self.db.execute("CREATE INDEX IF NOT EXISTS idx_full_emails_is_html ON full_emails(is_html)")
                        elif index_name == "idx_full_emails_has_images":
                            await self.db.execute("CREATE INDEX IF NOT EXISTS idx_full_emails_has_images ON full_emails(has_images)")
                        elif index_name == "idx_full_emails_in_reply_to":
                            await self.db.execute("CREATE INDEX IF NOT EXISTS idx_full_emails_in_reply_to ON full_emails(in_reply_to)")
                        elif index_name == "idx_full_emails_message_id":
                            await self.db.execute("CREATE INDEX IF NOT EXISTS idx_full_emails_message_id ON full_emails(message_id)")
                        elif index_name == "idx_full_emails_mailbox":
                            await self.db.execute("CREATE INDEX IF NOT EXISTS idx_full_emails_mailbox ON full_emails(mailbox)")
                        print(f"Created index {index_name}")
                    except Exception as e:
                        print(f"Error creating index {index_name}: {e}")
                        # If we can't create an index, the column might be missing
                        if "no such column" in str(e).lower():
                            print(f"Column for index {index_name} is missing. Run the program again to recreate the table structure.")
    
    async def log_sync_start(self, message):
        """Log the start of a sync operation"""
        await self.db.execute('''
            INSERT INTO sync_status (start_time, status, message)
            VALUES (?, 'STARTED', ?)
        ''', (datetime.datetime.now().isoformat(), message))
        async with self.db.execute('SELECT last_insert_rowid()') as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None
            
    async def log_sync_end(self, status_id, status, message):
        """Log the completion of a sync operation"""
        await self.db.execute(
            "UPDATE sync_status SET end_time = ?, status = ?, message = ? WHERE id = ?", 
            (datetime.datetime.now().isoformat(), status, message, status_id)
        )
        await self.db.commit()

# Unified IMAP client
class ImapClient:
    def __init__(self, host, user, creds):
        self.host = host
        self.user = user
        self.creds = creds
        self.imap = None
        self.current_mailbox = None
        self.loop = asyncio.get_event_loop()
        
    async def connect(self):
        """Connect to the IMAP server"""
        print(f"Connecting to {self.host} as {self.user}...")
        self.imap = await self.loop.run_in_executor(
            None, lambda: self._login_oauth2()
        )
        print(f"Connection established successfully")
        return self.imap
        
    def _login_oauth2(self):
        """Authenticate with IMAP server using OAuth2"""
        imap = imaplib.IMAP4_SSL(self.host)
        auth_string = f"user={self.user}\x01auth=Bearer {self.creds.token}\x01\x01"
        imap.authenticate('XOAUTH2', lambda x: auth_string)
        return imap
        
    def _quote_mailbox_if_needed(self, mailbox):
        """Add double quotes around mailbox names that contain spaces or slashes"""
        if not (mailbox.startswith('"') and mailbox.endswith('"')):
            if " " in mailbox or "/" in mailbox:
                return f'"{mailbox}"'
        return mailbox
        
    async def select_mailbox(self, mailbox, readonly=True):
        """Select a mailbox"""
        if self.current_mailbox == mailbox:
            return True
            
        quoted_mailbox = self._quote_mailbox_if_needed(mailbox)
            
        status, _ = await self.loop.run_in_executor(
            None, lambda: self.imap.select(quoted_mailbox, readonly=readonly)
        )
        if status == 'OK':
            self.current_mailbox = mailbox
            return True
        else:
            print(f"Failed to select mailbox {mailbox}: {status}")
            return False
            
    async def fetch(self, uid, fetch_type='headers'):
        """Fetch email data by UID"""
        fetch_command = None
        if fetch_type == 'headers':
            fetch_command = '(BODY.PEEK[HEADER.FIELDS (FROM TO CC SUBJECT DATE)])'
        elif fetch_type == 'full':
            fetch_command = '(BODY.PEEK[])'
            
        status, data = await self.loop.run_in_executor(
            None, lambda: self.imap.uid('FETCH', uid, fetch_command)
        )
        return status, data
        
    async def search_all(self):
        """Search for all messages in the current mailbox"""
        status, data = await self.loop.run_in_executor(
            None, lambda: self.imap.uid('SEARCH', None, 'ALL')
        )
        if status != 'OK':
            return []
            
        uid_list = data[0].decode().split() if isinstance(data[0], bytes) else data[0].split()
        return list(map(str, uid_list))
        
    async def search_chunked(self, chunk_size=10000):
        """Search for messages in chunks to avoid response size limits
        
        This method fetches UIDs in chunks by date ranges to avoid hitting
        Gmail's 1MB response size limit.
        """
        try:
            # First try to get message count
            status, data = await self.loop.run_in_executor(
                None, lambda: self.imap.status(self.current_mailbox, '(MESSAGES)')
            )
            
            if status != 'OK':
                print(f"Warning: Could not get message count for {self.current_mailbox}")
                # Fall back to normal search but with a smaller limit
                return await self.search_all()
            
            # Parse the message count
            match = re.search(r'MESSAGES\s+(\d+)', data[0].decode())
            if not match:
                print(f"Warning: Could not parse message count for {self.current_mailbox}")
                return await self.search_all()
                
            message_count = int(match.group(1))
            
            if message_count < chunk_size:
                # If we have fewer messages than the chunk size, just use search_all
                return await self.search_all()
            
            print(f"Large mailbox detected ({message_count} messages). Using chunked fetch.")
            
            # For large mailboxes like [Gmail]/All Mail, fetch UIDs in chunks
            all_uids = []
            
            # Get the UIDs in sequence number chunks to avoid response size limits
            for start in range(1, message_count + 1, chunk_size):
                end = min(start + chunk_size - 1, message_count)
                sequence_set = f"{start}:{end}"
                
                status, data = await self.loop.run_in_executor(
                    None, lambda: self.imap.fetch(sequence_set, '(UID)')
                )
                
                if status != 'OK':
                    print(f"Warning: Failed to fetch UIDs for chunk {start}-{end}")
                    continue
                
                # Extract UIDs from the response
                for item in data:
                    if isinstance(item, tuple) and len(item) == 2:
                        match = re.search(r'UID\s+(\d+)', item[0].decode())
                        if match:
                            all_uids.append(match.group(1))
            
            return all_uids
        except Exception as e:
            print(f"Error in search_chunked: {e}")
            # Fall back to normal search
            return await self.search_all()

    async def search_by_date_chunks(self, start_year=None, end_year=None):
        """Search for messages by date range chunks
        
        This method is specifically designed for extremely large mailboxes
        like [Gmail]/All Mail where other methods fail due to size limitations.
        It breaks down the search into year-month chunks.
        """
        try:
            # Determine date range to search
            if not start_year:
                # Start from a reasonable point in the past if not specified
                start_year = 2004  # Gmail launched in 2004
            
            if not end_year:
                # Default to current year if not specified
                end_year = datetime.datetime.now().year
                
            print(f"Searching emails from {start_year} to {end_year} in date chunks...")
            
            all_uids = []
            total_chunks = (end_year - start_year + 1) * 12
            current_chunk = 0
            
            # Search by year and month to avoid large responses
            for year in range(start_year, end_year + 1):
                for month in range(1, 13):
                    current_chunk += 1
                    
                    # Skip future months in current year
                    if year == end_year and month > datetime.datetime.now().month:
                        continue
                        
                    # Format date criteria for IMAP
                    # IMAP search date format is DD-MMM-YYYY
                    date_start = f"01-{['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'][month-1]}-{year}"
                    
                    # Last day of month (account for leap years)
                    last_day = 31
                    if month in [4, 6, 9, 11]:
                        last_day = 30
                    elif month == 2:  # February
                        last_day = 29 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 28
                        
                    date_end = f"{last_day}-{['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'][month-1]}-{year}"
                    
                    print(f"Searching chunk {current_chunk}/{total_chunks}: {date_start} to {date_end}")
                    
                    try:
                        # Search with date range
                        status, data = await self.loop.run_in_executor(
                            None, lambda: self.imap.uid('SEARCH', None, f'(SINCE "{date_start}" BEFORE "{date_end}")')
                        )
                        
                        if status != 'OK':
                            print(f"Warning: Failed to search date range {date_start} to {date_end}")
                            continue
                            
                        uids = data[0].decode().split() if isinstance(data[0], bytes) else data[0].split()
                        if uids and len(uids) > 0:
                            all_uids.extend(uids)
                            print(f"  Found {len(uids)} messages for {['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][month-1]} {year}")
                    except Exception as e:
                        print(f"Error searching {date_start} to {date_end}: {e}")
                        # Continue to next chunk, don't let one failed chunk stop the whole process
                        continue
            
            return all_uids
        except Exception as e:
            print(f"Error in search_by_date_chunks: {e}")
            return []
        
    async def list_mailboxes(self):
        """List all available mailboxes"""
        status, mailboxes = await self.loop.run_in_executor(
            None, lambda: self.imap.list()
        )
        if status != 'OK':
            return []
            
        result = []
        for mailbox in mailboxes:
            if isinstance(mailbox, bytes):
                mailbox = mailbox.decode('utf-8', errors='replace')
                
            # The format is typically like: (\\HasNoChildren) "/" "INBOX.Sent"
            parts = mailbox.split(' "')
            if len(parts) > 1:
                # Extract the mailbox name (removing trailing quote)
                name = parts[-1].rstrip('"')
                result.append(name)
            else:
                result.append(mailbox)
                
        return result
        
    async def close(self):
        """Close the IMAP connection"""
        if self.imap:
            try:
                await self.loop.run_in_executor(None, lambda: self.imap.logout())
            except Exception as e:
                print(f"Error during logout: {e}")

# Obtain or refresh OAuth2 credentials
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

# Email processor class for fetching and syncing emails
class EmailSyncer:
    def __init__(self, db_manager, imap_client, mode='headers', mailbox=None):
        """Initialize the EmailSyncer
        
        Args:
            db_manager: DatabaseManager instance
            imap_client: ImapClient instance
            mode: 'headers' or 'full'
            mailbox: Current mailbox name
        """
        self.db = db_manager.db
        self.db_manager = db_manager
        self.imap_client = imap_client
        self.mode = mode
        self.checkpoint = CheckpointManager(mode, mailbox)
        self.last_status_id = None
        self.failed_uids = self.checkpoint.get_failed_uids()
        self.emails_since_commit = 0
        self.pbar = None

    async def start_sync(self, message):
        """Start sync operation and log it"""
        self.checkpoint.mark_start()
        self.last_status_id = await self.db_manager.log_sync_start(message)
        
    async def finish_sync(self, status, message):
        """Finish sync operation and log it"""
        await self.db_manager.log_sync_end(self.last_status_id, status, message)
        self.checkpoint.mark_complete()
        
    async def process_headers(self, uid, mailbox):
        """Process email headers for a single UID"""
        # Fetch headers
        status, data = await self.imap_client.fetch(uid, 'headers')
        if status != 'OK':
            debug_print(f"Failed to fetch headers for UID {uid}: {status}")
            self.checkpoint.add_failed_uid(uid)
            return 'fail'
            
        # Extract header data
        header_data = None
        for item in data:
            if isinstance(item, tuple) and len(item) > 1:
                header_data = item[1]
                break
        
        if not header_data:
            debug_print(f"No header data found for UID {uid}")
            self.checkpoint.add_failed_uid(uid)
            return 'fail'
            
        # Parse email headers
        msg = email.message_from_bytes(header_data if isinstance(header_data, bytes) else header_data.encode('utf-8'))
        date_str = decode_field(msg.get('Date', ''))
        iso_date = parse_email_date(date_str)
        
        # Save headers to database
        await self.db.execute(
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
        
        # Update checkpoint and return success
        self.checkpoint.update_progress(uid)
        if uid in self.failed_uids:
            self.checkpoint.clear_failed_uid(uid)
        return 'saved'
        
    async def process_full_email(self, uid, mailbox):
        """Process full email content for a single UID"""
        debug_print(f"Fetching full email for UID {uid} in mailbox {mailbox}...")
        
        # Fetch full email
        status, data = await self.imap_client.fetch(uid, 'full')
        if status != 'OK' or not data:
            print(f"[ERROR] Failed to fetch UID {uid}: status={status}, data_len={len(data) if data else 0}")
            self.checkpoint.add_failed_uid(uid)
            return 'fail'
            
        debug_print(f"Received data for UID {uid}, processing...")
        
        # Extract raw email data
        raw_email = None
        for item in data:
            if isinstance(item, tuple) and len(item) > 1:
                raw_email = item[1]
                break
                
        if not raw_email:
            if self.pbar and self.pbar.n < 10:
                debug_print(f"[DEBUG] No raw email data found for UID {uid} in mailbox {mailbox}. Data: {data}")
            self.checkpoint.add_failed_uid(uid)
            return 'fail'
            
        # Save full email to database
        await self.db.execute(
            'INSERT OR REPLACE INTO full_emails(uid, mailbox, raw_email, fetched_at) VALUES(?,?,?,?)',
            (uid, mailbox, raw_email, datetime.datetime.now().isoformat())
        )
        
        # Update checkpoint and return success
        if self.pbar and self.pbar.n < 10:
            print(f"[DEBUG] Successfully saved full email for UID {uid} in mailbox {mailbox}.")
            
        self.checkpoint.update_progress(uid)
        if uid in self.failed_uids:
            self.checkpoint.clear_failed_uid(uid)
        return 'saved'
        
    async def run(self, uids_to_fetch, total_count, fetch_mode_desc):
        """Run the sync process for a list of UIDs"""
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
                        # Select the correct mailbox before fetching
                        if prev_mailbox != mbox:
                            if not await self.imap_client.select_mailbox(mbox):
                                self.checkpoint.add_failed_uid(uid)
                                skipped_count += 1
                                self.pbar.update(1)
                                continue
                            prev_mailbox = mbox
                            
                        # Process email based on mode
                        if self.mode == 'headers':
                            result = await self.process_headers(uid, mbox)
                        else:  # full mode
                            result = await self.process_full_email(uid, mbox)
                            
                        if result == 'fail':
                            skipped_count += 1
                        elif result == 'saved':
                            saved_count += 1
                            processed_count += 1
                            
                        self.pbar.update(1)
                        self.emails_since_commit += 1
                        
                        # Commit periodically
                        if self.emails_since_commit >= EMAILS_PER_COMMIT:
                            self.checkpoint.save_state()
                            await self.db_manager.commit_with_retry()
                            self.emails_since_commit = 0
                            
                    except Exception as e:
                        debug_print(f"Error processing UID {uid}: {e}")
                        self.checkpoint.add_failed_uid(uid)
                        skipped_count += 1
                        self.pbar.update(1)
                        
                # Commit after each chunk
                if chunk_idx % 1 == 0:  # Commit after every chunk
                    self.checkpoint.save_state()
                    await self.db_manager.commit_with_retry()
                    
            # Final commit
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
            if self.pbar:
                self.pbar.n = processed_count
                self.pbar.refresh()
                self.pbar.close()
                print(f"Successfully processed {processed_count} messages")
                print(f"Saved {saved_count} {fetch_mode_desc} to database")
                print(f"Skipped {skipped_count} messages")
                
        return processed_count, saved_count, skipped_count

async def sync_email_headers(db_manager, imap_client, mailbox='INBOX'):
    """Synchronize email headers from IMAP server to database"""
    # Create syncer instance
    syncer = EmailSyncer(db_manager, imap_client, 'headers', mailbox)
    
    # Start sync operation
    await syncer.start_sync(f'Starting headers sync for {mailbox}')
    
    try:
        # Select mailbox
        if not await imap_client.select_mailbox(mailbox):
            await syncer.finish_sync('ERROR', f'Failed to select mailbox {mailbox}')
            return
            
        # Get the last processed UID from checkpoint
        last_uid = syncer.checkpoint.get_last_uid()
        
        # Check if we have a last UID in the database
        async with db_manager.db.execute("SELECT MAX(CAST(uid AS INTEGER)) FROM emails WHERE mailbox = ?", (mailbox,)) as cursor:
            row = await cursor.fetchone()
        last_uid_db = int(row[0]) if row and row[0] else 0
        
        # Use the minimum of the two to ensure we don't miss any emails
        if last_uid > 0 and last_uid_db > 0:
            last_uid = min(last_uid, last_uid_db)
        else:
            last_uid = max(last_uid, last_uid_db)
            
        print(f"Resuming from UID > {last_uid}")
        
        # Get failed UIDs that need to be retried
        failed_uids = syncer.checkpoint.get_failed_uids()
        if failed_uids:
            print(f"Found {len(failed_uids)} failed UIDs from previous runs. Will retry these.")
            
        # For the exceptionally large "[Gmail]/All Mail" mailbox, use date-based search
        if mailbox == '[Gmail]/All Mail':
            print("Using date-based search for [Gmail]/All Mail mailbox")
            all_uids = await imap_client.search_by_date_chunks(start_year=2000)
        # For other large mailboxes, use chunked search
        elif mailbox in ['[Gmail]/Mail'] or 'All Mail' in mailbox:
            try:
                all_uids = await imap_client.search_chunked()
            except Exception as e:
                print(f"Chunked search failed: {e}. Falling back to date-based search.")
                all_uids = await imap_client.search_by_date_chunks(start_year=2000)
        else:
            all_uids = await imap_client.search_all()
            
        if not all_uids:
            print("No emails found in mailbox.")
            await syncer.finish_sync('COMPLETED', 'No emails found in mailbox')
            return
            
        # Filter for new UIDs (greater than the last processed UID)
        new_uids = [uid for uid in all_uids if int(uid) > last_uid]
        
        # Add failed UIDs to be retried
        if failed_uids:
            failed_uids_str = [str(uid) for uid in failed_uids]
            retry_uids = [uid for uid in failed_uids_str if uid in all_uids]
            retry_uids = [uid for uid in retry_uids if uid not in new_uids]
            new_uids.extend(retry_uids)
            new_uids.sort(key=int)
            
        if not new_uids:
            print('No new messages to fetch.')
            await syncer.finish_sync('COMPLETED', 'No new messages to fetch')
            return
            
        # Process the UIDs
        total_count = len(new_uids)
        print(f"Found {total_count} new emails to process")
        
        # Run the syncer
        await syncer.run([(uid, mailbox) for uid in new_uids], total_count, 'headers')
        
    except Exception as e:
        print(f"Error in sync_email_headers: {e}")
        await syncer.finish_sync('ERROR', str(e)[:200])
        raise

async def sync_full_emails(db_manager, imap_client, mailbox='INBOX'):
    """Synchronize full email content from IMAP server to database"""
    # Create syncer instance
    syncer = EmailSyncer(db_manager, imap_client, 'full', mailbox)
    
    # Start sync operation
    await syncer.start_sync(f'Starting full email sync for {mailbox}')
    
    try:
        # Get all email UIDs from the headers table for this mailbox
        async with db_manager.db.execute('SELECT uid, mailbox FROM emails WHERE mailbox = ?', (mailbox,)) as cursor:
            all_rows = await cursor.fetchall()
        all_uids = [(str(row[0]), row[1]) for row in all_rows]
        
        print(f"Found {len(all_uids)} total emails in database for mailbox {mailbox}")
        
        # Get already fetched UIDs
        async with db_manager.db.execute('SELECT uid FROM full_emails WHERE mailbox = ?', (mailbox,)) as cursor:
            fetched_rows = await cursor.fetchall()
        fetched_uids = set(str(row[0]) for row in fetched_rows)
        
        print(f"Already fetched {len(fetched_uids)} full emails")
        
        # Get failed UIDs from previous runs
        failed_uids = syncer.checkpoint.get_failed_uids()
        if failed_uids:
            print(f"Found {len(failed_uids)} failed UIDs from previous full-email fetches. Will retry these.")
            print(f"First 5 failed UIDs: {failed_uids[:5] if len(failed_uids) > 5 else failed_uids}")
        
        # Determine which UIDs need to be fetched
        uids_to_fetch = [(uid, mbox) for (uid, mbox) in all_uids if uid not in fetched_uids]
        retry_uids = [(uid, mbox) for (uid, mbox) in all_uids if uid in failed_uids and uid not in fetched_uids]
        
        print(f"UIDs needing full email fetch: {len(uids_to_fetch)}")
        print(f"Failed UIDs to retry: {len(retry_uids)}")
        
        # Add retry UIDs if they're not already in the fetch list
        for item in retry_uids:
            if item not in uids_to_fetch:
                uids_to_fetch.append(item)
                
        total_count = len(uids_to_fetch)
        print(f"Total UIDs to fetch: {total_count}")
        
        if not uids_to_fetch:
            print('No new full emails to fetch.')
            await syncer.finish_sync('COMPLETED', 'No new full emails to fetch')
            return
            
        # Sample a few UIDs for debugging
        if len(uids_to_fetch) > 0:
            print(f"Sample UIDs to fetch (first 5): {uids_to_fetch[:5]}")
        
        # Limit number of UIDs for debugging if needed
        if DEBUG and total_count > 100:
            print(f"DEBUG mode: Limiting to first 100 UIDs for testing")
            uids_to_fetch = uids_to_fetch[:100]
            total_count = len(uids_to_fetch)
            
        # Run the syncer
        await syncer.run(uids_to_fetch, total_count, 'full emails')
        
    except Exception as e:
        print(f"Error in sync_full_emails: {e}")
        await syncer.finish_sync('ERROR', str(e)[:200])
        raise

async def display_mailboxes(imap_client):
    """Display available mailboxes"""
    mailboxes = await imap_client.list_mailboxes()
    
    print("\nAvailable mailboxes:")
    print("--------------------")
    for i, mailbox in enumerate(mailboxes, 1):
        print(f"{i}. {mailbox}")
    print("\nUse any of these names with the --mailbox argument")

async def execute_query(db, query_name, **query_params):
    """Execute a predefined query with parameters"""
    if query_name not in QUERIES:
        print(f"Error: Query '{query_name}' not found. Available queries:")
        for name, details in QUERIES.items():
            print(f"  - {name}: {details['description']}")
        return
    
    query_info = QUERIES[query_name]
    print(f"\n=== {query_info['name']} ===")
    print(f"{query_info['description']}")
    
    # Run setup query if present (for views)
    if 'setup' in query_info:
        try:
            await db.execute(query_info['setup'])
        except Exception as e:
            print(f"Setup error: {e}")
    
    # Prepare parameters
    params = []
    if 'params' in query_info:
        # Merge default params with user-provided params
        merged_params = query_info['params'].copy()
        merged_params.update(query_params)
        
        # Convert param dict to ordered list based on parameter positions in query
        query_text = query_info['query']
        params = [merged_params[name] for name in merged_params]
    
    try:
        # Execute the query
        async with db.execute(query_info['query'], params) as cursor:
            # Get column names from cursor description
            columns = [col[0] for col in cursor.description] if cursor.description else []
            
            # Fetch all rows
            rows = await cursor.fetchall()
            
            if not rows:
                print("\nNo results found.")
                return
            
            # Format and print results
            print(f"\nFound {len(rows)} results:")
            print(tabulate.tabulate(rows, headers=columns, tablefmt='psql'))
            
            # For long result sets, summarize
            if len(rows) >= 20:
                print(f"\nDisplayed {len(rows)} results.")
    
    except Exception as e:
        print(f"Query execution error: {e}")

async def list_available_queries():
    """List all available queries"""
    print("\nAvailable queries:")
    print("==================")
    
    for name, details in QUERIES.items():
        print(f"\n{name}: {details['name']}")
        print(f"  {details['description']}")
        
        # Show parameters if any
        if 'params' in details:
            print("  Parameters:")
            for param_name, default_value in details['params'].items():
                print(f"    --{param_name}={default_value}")

async def sync_attachments(db_manager, mailbox='INBOX'):
    """Extract attachments from full_emails and populate normalized attachment tables."""
    import email as pyemail
    from email import policy
    import hashlib

    # Create a checkpoint manager for tracking progress
    checkpoint = CheckpointManager('attachments', mailbox)
    checkpoint.mark_start()

    # Log start of sync
    sync_status_id = await db_manager.log_sync_start(f'Starting attachments extraction for {mailbox}')

    # Create normalized tables
    await db_manager.db.execute('''
        CREATE TABLE IF NOT EXISTS attachment_blobs (
            sha256 TEXT PRIMARY KEY,
            content BLOB NOT NULL,
            size INTEGER NOT NULL,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    await db_manager.db.execute('''
        CREATE TABLE IF NOT EXISTS email_attachments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            uid TEXT NOT NULL,
            mailbox TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            filename TEXT,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (sha256) REFERENCES attachment_blobs(sha256),
            UNIQUE(uid, mailbox, sha256, filename)
        )
    ''')

    # Create indexes
    await db_manager.db.execute('CREATE INDEX IF NOT EXISTS idx_email_attachments_uid_mailbox ON email_attachments(uid, mailbox)')
    await db_manager.db.execute('CREATE INDEX IF NOT EXISTS idx_email_attachments_sha256 ON email_attachments(sha256)')
    await db_manager.db.execute('CREATE INDEX IF NOT EXISTS idx_attachment_blobs_size ON attachment_blobs(size)')

    # Create a view for easy querying
    await db_manager.db.execute('''
        CREATE VIEW IF NOT EXISTS attachment_info AS
        SELECT 
            ea.id, 
            ea.uid, 
            ea.mailbox, 
            ea.filename, 
            ab.size, 
            ab.sha256,
            ea.fetched_at,
            e.msg_date,
            e.msg_from,
            e.msg_to,
            e.subject
        FROM 
            email_attachments ea
        JOIN 
            attachment_blobs ab ON ea.sha256 = ab.sha256
        JOIN
            emails e ON ea.uid = e.uid AND ea.mailbox = e.mailbox
    ''')
    
    await db_manager.db.commit()
    
    # Get the last processed UID from checkpoint
    last_uid = checkpoint.get_last_uid()
    
    print(f"Processing attachments for mailbox '{mailbox}', starting from UID > {last_uid}")

    # Query to get all emails that have attachments
    query = '''
        SELECT uid, mailbox, full_content 
        FROM full_emails
        WHERE mailbox = ? AND CAST(uid AS INTEGER) > ?
        ORDER BY CAST(uid AS INTEGER)
    '''
    async with db_manager.db.execute(query, (mailbox, last_uid)) as cursor:
        rows = await cursor.fetchall()

    # Process emails
    count = 0
    pbar = tqdm.tqdm(total=len(rows), desc=f"Extracting attachments from {mailbox}")
    
    for row in rows:
        uid, mailbox, content = row
        try:
            # Parse the email
            msg = pyemail.message_from_bytes(content, policy=policy.default)
            
            # Find attachments
            for part in msg.iter_attachments():
                filename = part.get_filename()
                if not filename:
                    continue
                    
                # Get content
                content = part.get_content()
                size = len(content)
                
                # Skip empty attachments
                if size == 0:
                    continue
                    
                # Compute SHA-256
                sha = hashlib.sha256(content).hexdigest()
                
                # Try to insert into blob table (will be ignored if exists)
                try:
                    await db_manager.db.execute('''
                        INSERT OR IGNORE INTO attachment_blobs (sha256, content, size)
                        VALUES (?, ?, ?)
                    ''', (sha, content, size))
                except Exception as e:
                    print(f"Error storing blob {sha}: {e}")
                    continue
                
                # Insert into mapping table
                try:
                    await db_manager.db.execute('''
                        INSERT OR IGNORE INTO email_attachments (uid, mailbox, sha256, filename)
                        VALUES (?, ?, ?, ?)
                    ''', (uid, mailbox, sha, filename))
                    
                    count += 1
                    
                    # Commit every 100 attachments
                    if count % 100 == 0:
                        await db_manager.db.commit()
                        
                except Exception as e:
                    print(f"Error mapping attachment {filename} to email {uid}: {e}")
                    continue
            
        except Exception as e:
            print(f"Error processing email {uid}: {e}")
            checkpoint.add_failed_uid(uid)
            continue
            
        pbar.update(1)
        
    pbar.close()
    await db_manager.db.commit()
    print(f"Extracted {count} attachments (including duplicates across emails).")

    # Print deduplication stats
    async with db_manager.db.execute('SELECT COUNT(DISTINCT sha256) FROM attachment_blobs') as cursor:
        unique_attachments = (await cursor.fetchone())[0]
    async with db_manager.db.execute('SELECT COUNT(*) FROM email_attachments') as cursor:
        total_mappings = (await cursor.fetchone())[0]
    duplicates = total_mappings - unique_attachments
    print(f"Unique attachments: {unique_attachments}")
    print(f"Total email-attachment mappings: {total_mappings}")
    print(f"Duplicate mappings (same attachment in multiple emails): {duplicates}")
    
    if total_mappings > 0:
        print(f"Deduplication ratio: {duplicates/total_mappings:.1%}")
        print(f"Storage savings: {duplicates/total_mappings:.1%} of attachment data")
        
    # Mark sync as complete
    checkpoint.mark_complete()
    await db_manager.log_sync_end(sync_status_id, 'COMPLETED', f"Extracted {count} attachments ({unique_attachments} unique)")

async def analytics_email_density(db_manager, year=None, metric='emails'):
    """Show a monthly density chart for a given year and metric using termgraph."""
    if year is None:
        year = datetime.datetime.now().year
    metric_info = METRIC_QUERIES.get(metric, METRIC_QUERIES['emails'])
    sql = metric_info['monthly_sql']
    # Query monthly counts
    async with db_manager.db.execute(sql, (str(year),)) as cursor:
        data = await cursor.fetchall()
    # Ensure all months are present
    counts = [0]*12
    for period, count in data:
        counts[int(period)-1] = count if count is not None else 0
    labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    # Prepare data for termgraph
    import tempfile
    with tempfile.NamedTemporaryFile('w+', delete=False) as f:
        for label, count in zip(labels, counts):
            f.write(f"{label} {count}\n")
        temp_path = f.name
    if not shutil.which('termgraph'):
        print("termgraph is not installed. Please install it with 'pip install termgraph'.")
        return
    print(f"\n{metric_info['label']} for {year} (monthly):")
    subprocess.run(['termgraph', temp_path, '--color', 'blue', '--width', '50', '--format', '{:.0f}'])
    print()
    os.unlink(temp_path)

async def analytics_email_calendar_heatmap(db_manager, year=None, metric='emails'):
    import tempfile, subprocess, shutil, os
    if year is None:
        year = datetime.datetime.now().year
    metric_info = METRIC_QUERIES.get(metric, METRIC_QUERIES['emails'])
    sql = metric_info['calendar_sql']
    async with db_manager.db.execute(sql, (str(year),)) as cursor:
        data = await cursor.fetchall()
    with tempfile.NamedTemporaryFile('w+', delete=False) as f:
        for period, count in data:
            f.write(f"{period} {count}\n")
        temp_path = f.name
    if not shutil.which('termgraph'):
        print("termgraph is not installed. Please install it with 'pip install termgraph'.")
        return
    print(f"\n{metric_info['label']} calendar heatmap for {year}:")
    subprocess.run([
        'termgraph', temp_path, '--calendar', '--start-dt', f'{year}-01-01', '--color', 'blue'
    ])
    print()
    os.unlink(temp_path)

async def run_analytics(db_manager, args):
    metric = getattr(args, 'metric', 'emails')
    if getattr(args, 'calendar', False):
        await analytics_email_calendar_heatmap(db_manager, year=args.year, metric=metric)
    else:
        await analytics_email_density(db_manager, year=args.year, metric=metric)

async def main():
    parser = argparse.ArgumentParser(description='Fetch Gmail emails to SQLite using OAuth2')
    parser.add_argument('--db', default='mail.sqlite3', help='Path to SQLite database')
    parser.add_argument('--creds', default="creds.json", help='Path to OAuth2 client secrets JSON')
    parser.add_argument('--host', default='imap.gmail.com', help='IMAP host')
    parser.add_argument('--user', help='Gmail address')
    parser.add_argument('--mailbox', default='INBOX', help='Mailbox name')
    parser.add_argument('--all-mailboxes', action='store_true', help='Sync all mailboxes (headers, full, attachments modes)')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--list-mailboxes', action='store_true', help='List available mailboxes and exit')
    parser.add_argument('--mode', choices=['headers', 'full', 'query', 'attachments', 'analytics'], default='headers', 
                        help='Execution mode: headers (default), full (fetch full emails), attachments (extract and normalize attachments), analytics (run analytics), or query (run queries)')
    
    # Query mode arguments
    parser.add_argument('--query', help='Name of query to execute (use --list-queries to see available queries)')
    parser.add_argument('--list-queries', action='store_true', help='List available queries')
    
    # Dynamic query parameters
    parser.add_argument('--limit', type=int, help='Limit for query results')
    parser.add_argument('--start-date', help='Start date for date range queries (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date for date range queries (YYYY-MM-DD)')
    parser.add_argument('--message-id', help='Message ID for thread queries')
    parser.add_argument('--year', type=int, help='Year for analytics (default: current year)')
    parser.add_argument('--calendar', action='store_true', help='Show calendar heatmap for analytics mode')
    parser.add_argument('--metric', choices=['emails', 'attachments', 'attachment_size', 'unique_attachments', 'avg_attachment_size'], default='emails', help='Metric to visualize: emails, attachments, attachment_size, unique_attachments, avg_attachment_size')
    
    args = parser.parse_args()
    
    global DEBUG
    DEBUG = args.debug or DEBUG

    # Query mode doesn't require credentials
    if args.mode == 'query' or args.list_queries:
        # Create database manager
        db_manager = DatabaseManager(args.db)
        db = await db_manager.connect()
        
        try:
            if args.list_queries:
                await list_available_queries()
                return
                
            if not args.query:
                print("Error: --query parameter is required in query mode")
                await list_available_queries()
                return
                
            # Extract query parameters from args
            query_params = {}
            if args.limit is not None:
                query_params['limit'] = args.limit
            if args.start_date:
                query_params['start_date'] = args.start_date
            if args.end_date:
                query_params['end_date'] = args.end_date
            if args.message_id:
                query_params['message_id'] = args.message_id
                
            await execute_query(db, args.query, **query_params)
            
        finally:
            await db_manager.close()
        return
    
    # Only require creds and user for modes that need IMAP (headers, full, list-mailboxes)
    needs_imap = args.mode in ['headers', 'full'] or args.list_mailboxes
    creds = None
    if needs_imap:
        if not args.creds:
            sys.exit("Error: --creds parameter is required for sync modes")
        if not args.user:
            sys.exit("Error: --user parameter is required for sync modes")
        creds = get_credentials(args.creds)

    # Create database manager
    db_manager = DatabaseManager(args.db)
    db = await db_manager.connect()

    try:
        imap_client = None
        # If the user wants to list mailboxes, do that and exit
        if args.list_mailboxes:
            imap_client = ImapClient(args.host, args.user, creds)
            await imap_client.connect()
            await display_mailboxes(imap_client)
            return
        # Sync emails based on mode
        if args.mode == 'headers':
            imap_client = ImapClient(args.host, args.user, creds)
            await imap_client.connect()
            if args.all_mailboxes:
                mailboxes = await imap_client.list_mailboxes()
                print('[DEBUG] Mailboxes returned by imap.list_mailboxes():')
                for mbox in mailboxes:
                    print(f'  - "{mbox}"')
                for mailbox in mailboxes:
                    print(f'[DEBUG] About to select mailbox: "{mailbox}"')
                    try:
                        await imap_client.select_mailbox(mailbox)
                    except Exception as e:
                        print(f"[WARNING] Skipping mailbox '{mailbox}': {e}")
                        continue
                    await sync_email_headers(db_manager, imap_client, mailbox)
            else:
                await sync_email_headers(db_manager, imap_client, args.mailbox)
        elif args.mode == 'full':
            imap_client = ImapClient(args.host, args.user, creds)
            await imap_client.connect()
            if args.all_mailboxes:
                mailboxes = await imap_client.list_mailboxes()
                print('[DEBUG] Mailboxes returned by imap.list_mailboxes():')
                for mbox in mailboxes:
                    print(f'  - "{mbox}"')
                for mailbox in mailboxes:
                    print(f'[DEBUG] About to select mailbox: "{mailbox}"')
                    try:
                        await imap_client.select_mailbox(mailbox)
                    except Exception as e:
                        print(f"[WARNING] Skipping mailbox '{mailbox}': {e}")
                        continue
                    await sync_full_emails(db_manager, imap_client, mailbox)
            else:
                await sync_full_emails(db_manager, imap_client, args.mailbox)
        elif args.mode == 'attachments':
            if args.all_mailboxes:
                # For attachments, get mailboxes from full_emails table
                mailboxes = await db_manager.get_mailboxes_from_full_emails()
                for mailbox in mailboxes:
                    try:
                        # No IMAP select needed, but check if mailbox is valid in DB
                        await sync_attachments(db_manager, mailbox)
                    except Exception as e:
                        print(f"[WARNING] Skipping mailbox '{mailbox}': {e}")
                        continue
            else:
                await sync_attachments(db_manager, args.mailbox)
        elif args.mode == 'analytics':
            await run_analytics(db_manager, args)
        print('Done.')
    finally:
        # Close connections
        if 'imap_client' in locals() and imap_client is not None:
            await imap_client.close()
        await db_manager.close()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram terminated by user")
    except Exception as e:
        print(f"Program terminated due to error: {e}")
        sys.exit(1)
