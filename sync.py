import asyncio
import datetime
import email
import hashlib # For sync_attachments
from tqdm import tqdm # For EmailSyncer and sync_attachments

# Local imports
import config # Import the config module
from config import MAX_UID_FETCH_RETRIES # Import the new retry limit
from db import DatabaseManager
from imap_client import ImapClient
from checkpoint import CheckpointManager
from utils import CHUNK_SIZE, EMAILS_PER_COMMIT, debug_print, parse_email_date, decode_field

class EmailSyncer:
    def __init__(self, db_manager: DatabaseManager, imap_client: ImapClient, mode='headers', mailbox=None):
        """Initialize the EmailSyncer"""
        self.db_manager = db_manager
        self.imap_client = imap_client
        self.mode = mode
        self.checkpoint = CheckpointManager(db_manager, mode, mailbox) 
        self.last_status_id = None
        self.failed_uids_counts: dict[str, int] = {} 
        self.emails_since_commit = 0
        self.pbar = None

    async def _initialize_failed_uids_cache(self):
        """Async helper to initialize the failed UIDs cache after CheckpointManager is ready."""
        self.failed_uids_counts = await self.checkpoint.get_failed_uids_with_counts()

    async def start_sync(self, message):
        """Start sync operation and log it"""
        await self._initialize_failed_uids_cache()
        await self.checkpoint.mark_start()
        self.last_status_id = await self.db_manager.log_sync_start(message)
        
    async def finish_sync(self, status, message):
        """Finish sync operation and log it"""
        await self.db_manager.log_sync_end(self.last_status_id, status, message)
        await self.checkpoint.mark_complete()
        
    async def process_headers(self, uid, mailbox):
        """Process email headers for a single UID"""
        uid_str = str(uid) 
        status, data = await self.imap_client.fetch(uid_str, 'headers')
        if status != 'OK':
            debug_print(f"Failed to fetch headers for UID {uid_str}: {status}") 
            await self.checkpoint.add_failed_uid(uid_str) 
            self.failed_uids_counts[uid_str] = self.failed_uids_counts.get(uid_str, 0) + 1
            return 'fail'
            
        header_data = None
        for item in data:
            if isinstance(item, tuple) and len(item) > 1:
                header_data = item[1]
                break
        
        if not header_data:
            debug_print(f"No header data found for UID {uid_str}") 
            await self.checkpoint.add_failed_uid(uid_str)
            self.failed_uids_counts[uid_str] = self.failed_uids_counts.get(uid_str, 0) + 1
            return 'fail'
            
        msg = email.message_from_bytes(header_data if isinstance(header_data, bytes) else header_data.encode('utf-8'))
        date_str = decode_field(msg.get('Date', '')) 
        iso_date = parse_email_date(date_str) 
        
        await self.db_manager.save_email_header(
            uid=uid_str,
            msg_from=decode_field(msg.get('From', '')), 
            msg_to=decode_field(msg.get('To', '')), 
            msg_cc=decode_field(msg.get('Cc', '')), 
            subject=decode_field(msg.get('Subject', '')), 
            msg_date=iso_date,
            mailbox=mailbox
        )
        
        await self.checkpoint.update_progress(uid_str)
        if uid_str in self.failed_uids_counts: 
            await self.checkpoint.clear_failed_uid(uid_str)
            if uid_str in self.failed_uids_counts: del self.failed_uids_counts[uid_str]
        return 'saved'
        
    async def process_full_email(self, uid, mailbox):
        """Process full email content for a single UID"""
        uid_str = str(uid) 
        debug_print(f"Fetching full email for UID {uid_str} in mailbox {mailbox}...") 
        
        status, data = await self.imap_client.fetch(uid_str, 'full')
        if status != 'OK' or not data:
            print(f"[ERROR] Failed to fetch UID {uid_str}: status={status}, data_len={len(data) if data else 0}")
            await self.checkpoint.add_failed_uid(uid_str) 
            self.failed_uids_counts[uid_str] = self.failed_uids_counts.get(uid_str, 0) + 1
            return 'fail'
            
        debug_print(f"Received data for UID {uid_str}, processing...") 
        
        raw_email = None
        for item in data:
            if isinstance(item, tuple) and len(item) > 1:
                raw_email = item[1]
                break
                
        if not raw_email:
            if self.pbar and self.pbar.n < 10: 
                debug_print(f"[DEBUG] No raw email data found for UID {uid_str} in mailbox {mailbox}. Data: {data}") 
            await self.checkpoint.add_failed_uid(uid_str)
            self.failed_uids_counts[uid_str] = self.failed_uids_counts.get(uid_str, 0) + 1
            return 'fail'
            
        await self.db_manager.save_full_email(
            uid=uid_str, 
            mailbox=mailbox, 
            raw_email=raw_email, 
            fetched_at=datetime.datetime.now().isoformat()
        )
        
        if self.pbar and self.pbar.n < 10: 
            if config.DEBUG_MODE: 
                 print(f"[DEBUG] Successfully saved full email for UID {uid_str} in mailbox {mailbox}.")
            
        await self.checkpoint.update_progress(uid_str)
        if uid_str in self.failed_uids_counts: 
            await self.checkpoint.clear_failed_uid(uid_str)
            if uid_str in self.failed_uids_counts: del self.failed_uids_counts[uid_str]
        return 'saved'
        
    async def run(self, uids_to_fetch_tuples, total_initial_count, fetch_mode_desc):
        """Run the sync process for a list of UIDs with their mailboxes."""
        if not self.failed_uids_counts and self.mode in ['headers','full']:
             await self._initialize_failed_uids_cache()

        processed_count = 0
        saved_count = 0
        self.emails_since_commit = 0
        
        actual_uids_to_attempt_processing = uids_to_fetch_tuples
        actual_processing_count = len(actual_uids_to_attempt_processing)

        if config.DEBUG_MODE: 
            sample_uids = [uid_mbox[0] for uid_mbox in actual_uids_to_attempt_processing[:min(10, actual_processing_count)]]
            print(f"[DEBUG] First {len(sample_uids)} UIDs to attempt processing in this run: {sample_uids}")
            print(f"[DEBUG] Total UIDs to attempt processing in this run: {actual_processing_count}")
        
        self.pbar = tqdm(total=actual_processing_count, desc=f'Fetching {fetch_mode_desc}')
        prev_mailbox = None
        
        try:
            for chunk_idx, i in enumerate(range(0, actual_processing_count, CHUNK_SIZE)): 
                chunk = actual_uids_to_attempt_processing[i:i + CHUNK_SIZE] 
                if not chunk:
                    continue
                
                for uid_str, mbox in chunk:
                    try:
                        if prev_mailbox != mbox:
                            if not await self.imap_client.select_mailbox(mbox):
                                await self.checkpoint.add_failed_uid(uid_str)
                                self.failed_uids_counts[uid_str] = self.failed_uids_counts.get(uid_str, 0) + 1
                                self.pbar.update(1) 
                                continue
                            prev_mailbox = mbox
                            
                        if self.mode == 'headers':
                            result = await self.process_headers(uid_str, mbox)
                        else:  
                            result = await self.process_full_email(uid_str, mbox)
                            
                        if result == 'saved':
                            saved_count += 1
                            processed_count += 1
                            
                        self.pbar.update(1)
                        self.emails_since_commit += 1
                        
                        if self.emails_since_commit >= EMAILS_PER_COMMIT: 
                            await self.checkpoint.save_state()
                            await self.db_manager.commit_with_retry()
                            self.emails_since_commit = 0
                            
                    except Exception as e:
                        debug_print(f"Error processing UID {uid_str} in {mbox}: {e}") 
                        await self.checkpoint.add_failed_uid(uid_str)
                        self.failed_uids_counts[uid_str] = self.failed_uids_counts.get(uid_str, 0) + 1
                        self.pbar.update(1) 
                        
                if chunk_idx % 1 == 0:  
                    await self.checkpoint.save_state()
                    await self.db_manager.commit_with_retry()
                    
            await self.db_manager.commit_with_retry() 
            if saved_count > 0:
                await self.finish_sync('COMPLETED', f'Successfully processed {saved_count} {fetch_mode_desc}')
            else:
                # If we processed UIDs but didn't save any, something might be wrong
                await self.finish_sync('COMPLETED_NO_CHANGES', f'No new {fetch_mode_desc} saved despite processing {processed_count} UIDs')
            
        except KeyboardInterrupt:
            print("\nOperation interrupted by user. Saving progress...")
            try:
                await self.db_manager.commit_with_retry() 
                await self.finish_sync('INTERRUPTED', 'Interrupted by user')
            except Exception as e:
                print(f"Error saving progress: {e}")
            raise
            
        except Exception as e:
            print(f"\nUnexpected error during sync run: {e}")
            import traceback
            traceback.print_exc()
            try:
                await self.db_manager.commit_with_retry() 
                await self.finish_sync('ERROR', str(e)[:200])
            except Exception as commit_err:
                print(f"Error saving progress after error: {commit_err}")
            raise
            
        finally:
            if self.pbar:
                self.pbar.n = processed_count 
                self.pbar.refresh()
                self.pbar.close()
                print(f"Run Summary for {fetch_mode_desc}:")
                print(f"  Successfully processed and saved: {saved_count} messages")
                                
        return processed_count, saved_count

async def sync_email_headers(db_manager: DatabaseManager, imap_client: ImapClient, mailbox='INBOX'):
    syncer = EmailSyncer(db_manager, imap_client, 'headers', mailbox)
    await syncer.start_sync(f'Starting headers sync for {mailbox}')
    
    try:
        if not await imap_client.select_mailbox(mailbox):
            await syncer.finish_sync('ERROR', f'Failed to select mailbox {mailbox}')
            return
            
        last_uid_checkpoint = await syncer.checkpoint.get_last_uid()
        last_uid_db = await db_manager.get_max_synced_header_uid(mailbox)
        last_uid = max(last_uid_checkpoint, last_uid_db) 
            
        print(f"Resuming header sync for {mailbox} from UID > {last_uid}")
        
        uids_to_retry_list = await syncer.checkpoint.get_uids_to_retry(MAX_UID_FETCH_RETRIES)
        permanently_failed_uids = await syncer.checkpoint.get_permanently_failed_uids(MAX_UID_FETCH_RETRIES)

        if uids_to_retry_list:
            print(f"Found {len(uids_to_retry_list)} UIDs for {mailbox} to retry (failed < {MAX_UID_FETCH_RETRIES} times).")
        if permanently_failed_uids:
            print(f"Skipping {len(permanently_failed_uids)} UIDs for {mailbox} (failed >= {MAX_UID_FETCH_RETRIES} times). First few: {permanently_failed_uids[:5]}")
            
        all_uids_server = []
        try:
            if mailbox == '[Gmail]/All Mail':
                all_uids_server = await imap_client.search_by_date_chunks(start_year=2000)
            elif mailbox in ['[Gmail]/Mail'] or 'All Mail' in mailbox: 
                try:
                    all_uids_server = await imap_client.search_chunked()
                except Exception as e:
                    print(f"Chunked search failed for {mailbox}: {e}. Falling back to date-based search.")
                    all_uids_server = await imap_client.search_by_date_chunks(start_year=2000)
            else:
                all_uids_server = await imap_client.search_all()
        except Exception as e:
            # If any search operation fails completely, report the error
            print(f"Search failed for {mailbox}: {e}")
            await syncer.finish_sync('ERROR', str(e)[:200])
            return
            
        final_uids_to_attempt_tuples = []
        if not all_uids_server:
            print(f"No emails found on server for mailbox {mailbox}.")
            if not uids_to_retry_list:
                await syncer.finish_sync('COMPLETED_NO_NEW_HEADERS', f'No emails found on server or to retry in {mailbox}')
                return
            final_uids_to_attempt_tuples = [(uid, mailbox) for uid in sorted(list(set(uids_to_retry_list)), key=int)]
        else:
            uids_to_process_server = [uid for uid in all_uids_server if int(uid) > last_uid]
            combined_uids = set(uids_to_process_server) | set(uids_to_retry_list)
            final_uids_to_attempt_sorted = sorted(list(combined_uids), key=int)
            final_uids_to_attempt_tuples = [(uid, mailbox) for uid in final_uids_to_attempt_sorted]
            
        if not final_uids_to_attempt_tuples:
            print(f'No new headers to fetch or retry for {mailbox}.')
            await syncer.finish_sync('COMPLETED_NO_NEW_HEADERS', f'No new headers to fetch or retry for {mailbox}')
            return
            
        total_to_attempt_count = len(final_uids_to_attempt_tuples)
        print(f"Found {total_to_attempt_count} headers to attempt processing for {mailbox}")
        
        await syncer.run(final_uids_to_attempt_tuples, total_to_attempt_count, 'headers')
        
    except Exception as e:
        print(f"Error in sync_email_headers for {mailbox}: {e}")
        import traceback; traceback.print_exc();
        await syncer.finish_sync('ERROR', str(e)[:200])

async def sync_full_emails(db_manager: DatabaseManager, imap_client: ImapClient, mailbox='INBOX'):
    syncer = EmailSyncer(db_manager, imap_client, 'full', mailbox)
    await syncer.start_sync(f'Starting full email sync for {mailbox}') 
    
    try:
        all_header_uids_db = await db_manager.get_all_header_uids_for_mailbox(mailbox)
        if not all_header_uids_db:
            print(f"No email headers found in DB for mailbox {mailbox}. Sync headers first.")
            await syncer.finish_sync('SKIPPED', f'No headers in DB for {mailbox}')
            return

        print(f"Found {len(all_header_uids_db)} total email headers in DB for {mailbox}")
        
        fetched_full_uids_db = await db_manager.get_synced_full_email_uids(mailbox)
        print(f"Already fetched {len(fetched_full_uids_db)} full emails for {mailbox}")
        
        uids_to_retry_list = await syncer.checkpoint.get_uids_to_retry(MAX_UID_FETCH_RETRIES)
        permanently_failed_uids = await syncer.checkpoint.get_permanently_failed_uids(MAX_UID_FETCH_RETRIES)

        if uids_to_retry_list:
            uids_to_retry_list = [uid for uid in uids_to_retry_list if uid in all_header_uids_db]
            print(f"Found {len(uids_to_retry_list)} UIDs for {mailbox} to retry for full content (failed < {MAX_UID_FETCH_RETRIES} times).")
        if permanently_failed_uids:
            permanently_failed_uids_for_log = [uid for uid in permanently_failed_uids if uid in all_header_uids_db]
            if permanently_failed_uids_for_log:
                print(f"Skipping {len(permanently_failed_uids_for_log)} UIDs for {mailbox} full content (failed >= {MAX_UID_FETCH_RETRIES} times). First few: {permanently_failed_uids_for_log[:5]}")
        
        uids_needing_fetch_full = [uid for uid in all_header_uids_db if uid not in fetched_full_uids_db]
        combined_uids_for_full_sync = set(uids_needing_fetch_full) | set(uids_to_retry_list)
        final_uids_to_attempt_sorted = sorted(list(combined_uids_for_full_sync), key=int)
        final_uids_to_attempt_tuples = [(uid, mailbox) for uid in final_uids_to_attempt_sorted]

        total_to_attempt_count = len(final_uids_to_attempt_tuples)
        print(f"Total full emails to attempt fetching/retrying for {mailbox}: {total_to_attempt_count}")
        
        if not final_uids_to_attempt_tuples:
            print(f'No new full emails to fetch or retry for {mailbox}.')
            await syncer.finish_sync('COMPLETED_NO_NEW_EMAILS', f'No new full emails to fetch or retry for {mailbox}')
            return
            
        if config.DEBUG_MODE and total_to_attempt_count > 100:
            print(f"DEBUG mode: Limiting to first 100 UIDs for full email testing in {mailbox}")
            final_uids_to_attempt_tuples = final_uids_to_attempt_tuples[:100]
            total_to_attempt_count = len(final_uids_to_attempt_tuples)
            
        await syncer.run(final_uids_to_attempt_tuples, total_to_attempt_count, 'full emails')
        
    except Exception as e:
        print(f"Error in sync_full_emails for {mailbox}: {e}")
        import traceback; traceback.print_exc();
        await syncer.finish_sync('ERROR', str(e)[:200])

async def sync_attachments(db_manager: DatabaseManager, mailbox='INBOX'):
    from email import policy as email_policy 

    checkpoint = CheckpointManager(db_manager, 'attachments', mailbox)

    await checkpoint.mark_start()
    sync_status_id = await db_manager.log_sync_start(f'Starting attachments extraction for {mailbox}')

    print(f"Processing attachments for mailbox '{mailbox}'.")

    rows = await db_manager.get_full_emails_for_attachment_processing(mailbox)

    count = 0
    attachments_processed_this_run = 0
    pbar = tqdm(total=len(rows), desc=f"Extracting attachments from {mailbox}")
    
    for row_data in rows:
        uid_str, mbox, raw_content = str(row_data[0]), row_data[1], row_data[2]
        current_uid_processed_attachments = 0
        try:
            msg = email.message_from_bytes(raw_content, policy=email_policy.default)
            
            for part in msg.iter_attachments():
                filename = part.get_filename()
                if not filename: continue
                    
                content_bytes = part.get_content()
                size = len(content_bytes)
                if size == 0: continue
                    
                sha = hashlib.sha256(content_bytes).hexdigest()
                
                try:
                    await db_manager.save_attachment_blob(sha, content_bytes, size)
                except Exception as e:
                    debug_print(f"Error storing blob {sha} for email {uid_str}: {e}")
                    continue 
                
                try:
                    await db_manager.map_email_to_attachment(uid_str, mbox, sha, filename)
                    attachments_processed_this_run += 1
                    current_uid_processed_attachments +=1
                    count += 1 

                    if attachments_processed_this_run % EMAILS_PER_COMMIT == 0: 
                        await db_manager.commit_with_retry()
                        await checkpoint.save_state() 
                        
                except Exception as e:
                    debug_print(f"Error mapping attachment {filename} to email {uid_str}: {e}")
                    continue 
            
            if current_uid_processed_attachments > 0:
                await checkpoint.clear_failed_uid(uid_str) 

        except Exception as e:
            debug_print(f"Error parsing email {uid_str} for attachments: {e}")
            await checkpoint.add_failed_uid(uid_str) 
            continue
        finally:
            pbar.update(1)
        
    pbar.close()
    await db_manager.commit_with_retry() 
    await checkpoint.save_state() 
    print(f"Extracted {count} new attachment mappings for {mailbox}.")

    unique_attachments = await db_manager.get_unique_attachment_blob_count()
    total_mappings_mailbox = await db_manager.get_attachment_mappings_count_for_mailbox(mailbox)
    
    print(f"Unique attachments in DB: {unique_attachments}")
    print(f"Total email-attachment mappings for {mailbox}: {total_mappings_mailbox}")
        
    await checkpoint.mark_complete()
    await db_manager.log_sync_end(sync_status_id, 'COMPLETED', f"Extracted {count} attachments ({unique_attachments} unique) for {mailbox}")
