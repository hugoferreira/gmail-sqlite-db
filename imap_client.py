import asyncio
import datetime
import imaplib
import re
import sys
import os

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

# Import constants from config.py
from config import SCOPES, TOKEN_PATH, CLIENT_SECRET_PATH
from utils import async_retry # Import the decorator

class ImapClient:
    def __init__(self, host, user, creds):
        self.host = host
        self.user = user
        self.creds = creds
        self.imap = None
        self.current_mailbox = None
        self.loop = asyncio.get_event_loop()
        
    @async_retry()
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
        try:
            imap.authenticate('XOAUTH2', lambda x: auth_string.encode('utf-8'))
        except imaplib.IMAP4.error as e:
            if "Invalid credentials" in str(e) or "AUTHENTICATIONFAILED" in str(e):
                print("OAuth2 authentication failed. Token might be invalid or expired.")
                raise ConnectionRefusedError("OAuth2 authentication failed. Check token.") from e
            raise 
        return imap
        
    def _quote_mailbox_if_needed(self, mailbox):
        """Add double quotes around mailbox names that contain spaces or slashes"""
        if not (mailbox.startswith('"') and mailbox.endswith('"')):
            if " " in mailbox or "/" in mailbox:
                return f'"{mailbox}"'
        return mailbox
        
    @async_retry()
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
            
    @async_retry()
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
        
    @async_retry()
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
        """Search for messages in chunks to avoid response size limits"""
        try:
            status, data = await self.loop.run_in_executor(
                None, lambda: self.imap.status(self.current_mailbox, '(MESSAGES)')
            )
            if status != 'OK':
                print(f"Warning: Could not get message count for {self.current_mailbox}")
                return await self.search_all()
            
            match = re.search(r'MESSAGES\s+(\d+)', data[0].decode())
            if not match:
                print(f"Warning: Could not parse message count for {self.current_mailbox}")
                return await self.search_all()
                
            message_count = int(match.group(1))
            if message_count < chunk_size:
                return await self.search_all()
            
            print(f"Large mailbox detected ({message_count} messages). Using chunked fetch.")
            all_uids = []
            for start in range(1, message_count + 1, chunk_size):
                end = min(start + chunk_size - 1, message_count)
                sequence_set = f"{start}:{end}"
                status, data = await self.loop.run_in_executor(
                    None, lambda: self.imap.fetch(sequence_set, '(UID)')
                )
                if status != 'OK':
                    print(f"Warning: Failed to fetch UIDs for chunk {start}-{end}")
                    continue
                for item in data:
                    if isinstance(item, tuple) and len(item) == 2:
                        match = re.search(r'UID\s+(\d+)', item[0].decode())
                        if match:
                            all_uids.append(match.group(1))
            return all_uids
        except Exception as e:
            print(f"Error in search_chunked: {e}")
            return await self.search_all()

    async def search_by_date_chunks(self, start_year=None, end_year=None):
        """Search for messages by date range chunks"""
        try:
            if not start_year:
                start_year = 2004
            if not end_year:
                end_year = datetime.datetime.now().year
            print(f"Searching emails from {start_year} to {end_year} in date chunks...")
            all_uids = []
            total_chunks = (end_year - start_year + 1) * 12
            current_chunk = 0
            for year in range(start_year, end_year + 1):
                for month in range(1, 13):
                    current_chunk += 1
                    if year == end_year and month > datetime.datetime.now().month:
                        continue
                    date_start = f"01-{['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'][month-1]}-{year}"
                    last_day = 31
                    if month in [4, 6, 9, 11]: last_day = 30
                    elif month == 2: last_day = 29 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 28
                    date_end = f"{last_day}-{['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'][month-1]}-{year}"
                    print(f"Searching chunk {current_chunk}/{total_chunks}: {date_start} to {date_end}")
                    try:
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
                        continue
            return all_uids
        except Exception as e:
            print(f"Error in search_by_date_chunks: {e}")
            return []
        
    @async_retry()
    async def list_mailboxes(self):
        """List all available mailboxes"""
        status, mailboxes_data = await self.loop.run_in_executor(
            None, lambda: self.imap.list()
        )
        if status != 'OK': return []
        result = []
        for mailbox_entry in mailboxes_data:
            if isinstance(mailbox_entry, bytes):
                mailbox_entry = mailbox_entry.decode('utf-8', errors='replace')
            parts = mailbox_entry.split(' "')
            if len(parts) > 1:
                name = parts[-1].rstrip('"')
                result.append(name)
            elif mailbox_entry.strip(): # Handle cases where split doesn't work as expected but entry is not empty
                 result.append(mailbox_entry.strip('() /"').split(' "')[-1]) # More aggressive cleaning
        return result
        
    async def close(self):
        """Close the IMAP connection"""
        if self.imap:
            print("Closing IMAP connection...")
            try:
                if self.current_mailbox:
                    await self.loop.run_in_executor(None, self.imap.close)
                await self.loop.run_in_executor(None, self.imap.logout)
                print("IMAP connection closed.")
            except imaplib.IMAP4.error as e:
                print(f"Error closing IMAP connection: {e}")
            except Exception as e: 
                print(f"Unexpected error during IMAP close/logout: {e}")
            finally:
                self.imap = None
                self.current_mailbox = None

def get_credentials(client_secret_file_path: str):
    """Obtain or refresh OAuth2 credentials."""
    # client_secret_file_path is the path to the client_secret.json (or equivalent)
    # TOKEN_PATH is where the user's token is stored.
    # SCOPES defines the permissions.

    creds = None
    if os.path.exists(TOKEN_PATH):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
        except Exception as e:
            print(f"Error loading token from {TOKEN_PATH}: {e}. Will attempt to re-authenticate.")
            creds = None # Ensure creds is None if loading fails

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                print("Credentials expired. Refreshing token...")
                creds.refresh(Request())
                print("Token refreshed successfully.")
            except Exception as e:
                print(f"Error refreshing token: {e}. Proceeding to full authentication flow.")
                # Fall through to full authentication flow if refresh fails
                creds = None # Reset creds to trigger full flow
        
        # This block executes if creds is None (initial run, failed load, or failed refresh)
        if not creds: 
            print("No valid credentials, attempting to authenticate...")
            if not os.path.exists(client_secret_file_path):
                sys.exit(f"Error: Client secret file not found at '{client_secret_file_path}'. Please ensure it's correctly specified and accessible.")
            try:
                flow = InstalledAppFlow.from_client_secrets_file(client_secret_file_path, SCOPES)
                creds = flow.run_local_server(port=0)
                print("Authentication successful.")
            except Exception as e:
                sys.exit(f"Failed to authenticate: {e}. Please check your client secret file and network connection.")
        
        # Save the (newly obtained or refreshed) credentials
        try:
            with open(TOKEN_PATH, 'w') as token_file:
                token_file.write(creds.to_json())
            print(f"Credentials saved to {TOKEN_PATH}")
        except Exception as e:
            print(f"Error saving token to {TOKEN_PATH}: {e}")
            # Proceed with current creds in memory if saving fails, but warn user.

    return creds

async def display_mailboxes(imap_client: ImapClient):
    """Display available mailboxes retrieved using the ImapClient."""
    mailboxes = await imap_client.list_mailboxes()
    print("\nAvailable mailboxes:")
    print("--------------------")
    if mailboxes:
        for i, mailbox in enumerate(mailboxes, 1):
            print(f"{i}. {mailbox}")
    else:
        print("No mailboxes found or error retrieving them.")
    print("\nUse any of these names with the --mailbox argument in main.py") 