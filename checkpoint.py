import json
import os
import datetime

# Forward declaration for type hinting if db.DatabaseManager is in a separate file not yet fully defined/imported
# from typing import TYPE_CHECKING
# if TYPE_CHECKING:
#     from db import DatabaseManager # Assuming db.py contains DatabaseManager

class CheckpointManager:
    def __init__(self, db_manager, mode: str, mailbox: str):
        """Initialize checkpoint manager. Relies on DatabaseManager for persistence.
        
        Args:
            db_manager: An instance of DatabaseManager.
            mode: Sync mode - 'headers', 'full', or 'attachments'.
            mailbox: Current mailbox name.
        """
        self.db_manager = db_manager
        self.mode = mode
        self.mailbox = mailbox if mailbox else 'INBOX' # Default mailbox if None
        self._state_loaded_for_current_mailbox = False # ADDED: Flag to track if state is loaded

        # Internal state attributes, loaded/initialized from DB
        self.last_uid: int = 0
        self.failed_uids: dict[str, int] = {}
        self.in_progress: bool = False
        self.timestamp: str | None = None

    async def _ensure_state_loaded(self):
        """Ensures state is loaded from DB. Call at the beginning of public async methods."""
        if self._state_loaded_for_current_mailbox: # ADDED: Check the flag
            return

        state_from_db = await self.db_manager.load_checkpoint_state(self.mode, self.mailbox)
        if state_from_db:
            self.last_uid = state_from_db['last_uid']
            # failed_uids are now loaded by load_checkpoint_state itself from the separate table
            self.failed_uids = state_from_db['failed_uids'] 
            self.in_progress = state_from_db['in_progress']
            self.timestamp = state_from_db['timestamp']
        else:
            # Defaults if no state found in DB for this mode/mailbox
            self.last_uid = 0
            self.failed_uids = {}
            self.in_progress = False
            self.timestamp = None
        self._state_loaded_for_current_mailbox = True # ADDED: Set the flag
    
    async def save_state(self):
        """Save current core internal state (last_uid, in_progress, timestamp) to the database."""
        self.timestamp = datetime.datetime.now().isoformat()
        await self.db_manager.save_checkpoint_state(
            mode=self.mode,
            mailbox=self.mailbox,
            last_uid=self.last_uid,
            # failed_uids_dict is no longer passed here; managed separately
            in_progress=self.in_progress,
            timestamp=self.timestamp
        )
    
    async def set_mailbox(self, mailbox: str):
        """Set current mailbox and load/initialize its state from the DB."""
        self.mailbox = mailbox if mailbox else 'INBOX'
        self._state_loaded_for_current_mailbox = False # ADDED: Reset the flag
        await self._ensure_state_loaded() # Load state for the new mailbox

    async def mark_start(self):
        """Mark the start of a sync operation."""
        await self._ensure_state_loaded()
        self.in_progress = True
        await self.save_state()
    
    async def mark_complete(self):
        """Mark the completion of a sync operation."""
        await self._ensure_state_loaded()
        self.in_progress = False
        await self.save_state()
    
    async def update_progress(self, uid: str):
        """Update the last processed UID."""
        await self._ensure_state_loaded()
        uid_int = int(uid) 
        if uid_int > self.last_uid:
            self.last_uid = uid_int
            # Periodic save for progress can be less frequent or even removed if save_state is called by callers
            # For now, only save if it's a significant jump or specific interval.
            if uid_int % 250 == 0: 
                await self.save_state()
            # Caller should decide if a final save_state is needed after a loop of updates.
    
    async def add_failed_uid(self, uid: str):
        """Add a UID to the failed list or increment its retry count in the DB and in memory."""
        await self._ensure_state_loaded() # Ensure current state is loaded
        uid_str = str(uid)
        current_retry_count = self.failed_uids.get(uid_str, 0) + 1
        self.failed_uids[uid_str] = current_retry_count
        
        # Persist this specific failed UID to its dedicated table
        await self.db_manager.add_or_update_checkpoint_failed_uid(
            mode=self.mode,
            mailbox=self.mailbox,
            uid=uid_str,
            retry_count=current_retry_count
        )
        # Also update the main checkpoint record's timestamp etc.
        await self.save_state()
    
    async def get_last_uid(self) -> int:
        """Get the last successfully processed UID for the current mailbox."""
        await self._ensure_state_loaded()
        return self.last_uid
    
    async def get_failed_uids_with_counts(self) -> dict[str, int]:
        """Get the dictionary of failed UIDs and their retry counts for the current mailbox."""
        await self._ensure_state_loaded()
        return self.failed_uids.copy() # Return a copy to prevent external modification

    async def get_uids_to_retry(self, max_retries: int) -> list[str]:
        """Get UIDs that have failed less than max_retries times."""
        await self._ensure_state_loaded()
        uids_for_retry = []
        for uid, count in self.failed_uids.items():
            if count < max_retries:
                uids_for_retry.append(uid)
        return uids_for_retry

    async def get_permanently_failed_uids(self, max_retries: int) -> list[str]:
        """Get UIDs that have failed max_retries or more times."""
        await self._ensure_state_loaded()
        permanently_failed = []
        for uid, count in self.failed_uids.items():
            if count >= max_retries:
                permanently_failed.append(uid)
        return permanently_failed
    
    async def clear_failed_uid(self, uid: str):
        """Remove a UID from the failed list in the DB and in memory if it's been processed successfully."""
        await self._ensure_state_loaded() # Ensure current state is loaded
        uid_str = str(uid)
        if uid_str in self.failed_uids:
            del self.failed_uids[uid_str]
            # Remove this specific failed UID from its dedicated table
            await self.db_manager.remove_checkpoint_failed_uid(
                mode=self.mode,
                mailbox=self.mailbox,
                uid=uid_str
            )
            # Also update the main checkpoint record's timestamp etc.
            await self.save_state()
            
    async def was_interrupted(self) -> bool:
        """Check if a previous sync was interrupted for the current mailbox."""
        await self._ensure_state_loaded()
        return self.in_progress