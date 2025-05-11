import asyncio
import datetime
import aiosqlite
import json

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
        
        # --- Attachment-related schema ---
        await self.db.execute('''
            CREATE TABLE IF NOT EXISTS attachment_blobs (
                sha256 TEXT PRIMARY KEY,
                content BLOB NOT NULL,
                size INTEGER NOT NULL,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await self.db.execute('''
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
        await self.db.execute('CREATE INDEX IF NOT EXISTS idx_email_attachments_uid_mailbox ON email_attachments(uid, mailbox)')
        await self.db.execute('CREATE INDEX IF NOT EXISTS idx_email_attachments_sha256 ON email_attachments(sha256)')
        await self.db.execute('CREATE INDEX IF NOT EXISTS idx_attachment_blobs_size ON attachment_blobs(size)')
        
        await self.db.execute('''
            CREATE VIEW IF NOT EXISTS attachment_info AS
            SELECT 
                ea.id, ea.uid, ea.mailbox, ea.filename, 
                ab.size, ab.sha256, ea.fetched_at,
                e.msg_date, e.msg_from, e.msg_to, e.subject
            FROM email_attachments ea
            JOIN attachment_blobs ab ON ea.sha256 = ab.sha256
            JOIN emails e ON ea.uid = e.uid AND ea.mailbox = e.mailbox
        ''')
        # --- End of attachment-related schema ---

        # --- Checkpoint State Table ---
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS checkpoint_states (
                mode TEXT NOT NULL,
                mailbox TEXT NOT NULL,
                last_uid INTEGER DEFAULT 0,
                -- failed_uids_json TEXT DEFAULT '{}', -- Removed
                in_progress INTEGER DEFAULT 0, -- Using INTEGER for BOOLEAN (0 or 1)
                timestamp TEXT,
                PRIMARY KEY (mode, mailbox)
            )
        """)

        # --- New Checkpoint Failed UIDs Table ---
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS checkpoint_failed_uids (
                mode TEXT NOT NULL,
                mailbox TEXT NOT NULL,
                uid TEXT NOT NULL,
                retry_count INTEGER DEFAULT 1,
                PRIMARY KEY (mode, mailbox, uid)
                -- Optional: FOREIGN KEY (mode, mailbox) REFERENCES checkpoint_states(mode, mailbox) ON DELETE CASCADE
                -- SQLite handles composite foreign keys but let's keep it simple for now
                -- and manage integrity at the application layer or via triggers if complex scenarios arise.
            )
        """)
        await self.db.execute("CREATE INDEX IF NOT EXISTS idx_checkpoint_failed_uids_mode_mailbox ON checkpoint_failed_uids(mode, mailbox)")
        # --- End of Checkpoint State Table ---
        
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

    async def get_mailboxes_from_full_emails(self):
        """Retrieve a list of unique mailbox names from the full_emails table."""
        mailboxes = []
        async with self.db.execute("SELECT DISTINCT mailbox FROM full_emails ORDER BY mailbox") as cursor:
            async for row in cursor:
                mailboxes.append(row[0])
        return mailboxes

    # --- Methods for Sync Operations ---
    async def save_email_header(self, uid: str, msg_from: str, msg_to: str, msg_cc: str, subject: str, msg_date: str, mailbox: str):
        """Saves email header information to the emails table."""
        await self.db.execute(
            'INSERT OR REPLACE INTO emails(uid, msg_from, msg_to, msg_cc, subject, msg_date, mailbox) VALUES(?,?,?,?,?,?,?)',
            (uid, msg_from, msg_to, msg_cc, subject, msg_date, mailbox)
        )

    async def save_full_email(self, uid: str, mailbox: str, raw_email: bytes, fetched_at: str):
        """Saves full email content to the full_emails table."""
        await self.db.execute(
            'INSERT OR REPLACE INTO full_emails(uid, mailbox, raw_email, fetched_at) VALUES(?,?,?,?)',
            (uid, mailbox, raw_email, fetched_at)
        )

    async def get_max_synced_header_uid(self, mailbox: str) -> int:
        """Gets the maximum UID for synced headers in a specific mailbox."""
        async with self.db.execute("SELECT MAX(CAST(uid AS INTEGER)) FROM emails WHERE mailbox = ?", (mailbox,)) as cursor:
            row = await cursor.fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    async def get_all_header_uids_for_mailbox(self, mailbox: str) -> list[str]:
        """Gets all UIDs from the emails table for a specific mailbox, ordered by UID."""
        async with self.db.execute('SELECT uid FROM emails WHERE mailbox = ? ORDER BY CAST(uid AS INTEGER)', (mailbox,)) as cursor:
            return [str(row[0]) for row in await cursor.fetchall()]

    async def get_synced_full_email_uids(self, mailbox: str) -> set[str]:
        """Gets a set of UIDs for which full emails have been synced for a specific mailbox."""
        async with self.db.execute('SELECT uid FROM full_emails WHERE mailbox = ?', (mailbox,)) as cursor:
            return {str(row[0]) for row in await cursor.fetchall()}

    async def get_full_emails_for_attachment_processing(self, mailbox: str) -> list:
        """Retrieves full emails that have attachments and haven't been processed yet."""
        query = '''
            SELECT fe.uid, fe.mailbox, fe.raw_email 
            FROM full_emails fe
            WHERE fe.mailbox = ? 
              AND fe.has_attachments = 1 
              AND NOT EXISTS (
                  SELECT 1 FROM email_attachments ea 
                  WHERE ea.uid = fe.uid AND ea.mailbox = fe.mailbox
              )
            ORDER BY CAST(fe.uid AS INTEGER)
        '''
        async with self.db.execute(query, (mailbox,)) as cursor:
            return await cursor.fetchall()

    async def save_attachment_blob(self, sha256: str, content_bytes: bytes, size: int):
        """Saves an attachment blob if it doesn't already exist."""
        await self.db.execute(
            'INSERT OR IGNORE INTO attachment_blobs (sha256, content, size) VALUES (?, ?, ?)',
            (sha256, content_bytes, size)
        )

    async def map_email_to_attachment(self, uid: str, mailbox: str, sha256: str, filename: str):
        """Creates a mapping between an email and an attachment blob, ignoring if it exists."""
        await self.db.execute(
            'INSERT OR IGNORE INTO email_attachments (uid, mailbox, sha256, filename) VALUES (?, ?, ?, ?)',
            (uid, mailbox, sha256, filename)
        )

    async def get_unique_attachment_blob_count(self) -> int:
        """Gets the count of unique attachment blobs stored in the database."""
        async with self.db.execute('SELECT COUNT(DISTINCT sha256) FROM attachment_blobs') as cursor:
            row = await cursor.fetchone()
            return row[0] if row and row[0] is not None else 0

    async def get_attachment_mappings_count_for_mailbox(self, mailbox: str) -> int:
        """Gets the total count of email-to-attachment mappings for a specific mailbox."""
        async with self.db.execute('SELECT COUNT(*) FROM email_attachments WHERE mailbox = ?', (mailbox,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row and row[0] is not None else 0

    # --- Checkpoint State DB Access Methods ---
    async def get_checkpoint_failed_uids(self, mode: str, mailbox: str) -> dict[str, int]:
        """Retrieves failed UIDs and their retry counts for a given mode and mailbox."""
        failed_uids_dict = {}
        query = "SELECT uid, retry_count FROM checkpoint_failed_uids WHERE mode = ? AND mailbox = ?"
        async with self.db.execute(query, (mode, mailbox)) as cursor:
            async for row in cursor:
                failed_uids_dict[row[0]] = row[1]
        return failed_uids_dict

    async def load_checkpoint_state(self, mode: str, mailbox: str) -> dict | None:
        """Loads checkpoint state for a given mode and mailbox from the database."""
        query = "SELECT last_uid, in_progress, timestamp FROM checkpoint_states WHERE mode = ? AND mailbox = ?"
        state_core = None
        async with self.db.execute(query, (mode, mailbox)) as cursor:
            row = await cursor.fetchone()
            if row:
                state_core = {
                    'last_uid': row[0],
                    'in_progress': bool(row[1]), # Convert INTEGER to BOOLEAN
                    'timestamp': row[2]
                }
        
        if state_core:
            # Now, fetch the failed UIDs from the new table
            state_core['failed_uids'] = await self.get_checkpoint_failed_uids(mode, mailbox)
            return state_core
        
        return None # No core state found

    async def save_checkpoint_state(self, mode: str, mailbox: str, last_uid: int, in_progress: bool, timestamp: str):
        """Saves (INSERT OR REPLACE) core checkpoint state (excluding failed UIDs) to the database."""
        query = """
            INSERT OR REPLACE INTO checkpoint_states (mode, mailbox, last_uid, in_progress, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """
        await self.db.execute(query, (mode, mailbox, last_uid, int(in_progress), timestamp))
        await self.db.commit()

    async def add_or_update_checkpoint_failed_uid(self, mode: str, mailbox: str, uid: str, retry_count: int):
        """Adds a new failed UID or updates the retry count of an existing one."""
        query = """
            INSERT OR REPLACE INTO checkpoint_failed_uids (mode, mailbox, uid, retry_count)
            VALUES (?, ?, ?, ?)
        """
        await self.db.execute(query, (mode, mailbox, uid, retry_count))
        await self.db.commit()

    async def remove_checkpoint_failed_uid(self, mode: str, mailbox: str, uid: str):
        """Removes a specific failed UID for a given mode and mailbox."""
        query = "DELETE FROM checkpoint_failed_uids WHERE mode = ? AND mailbox = ? AND uid = ?"
        await self.db.execute(query, (mode, mailbox, uid))
        await self.db.commit() 