import asyncio
import datetime
import uvicorn

from mcp.server.fastmcp import FastMCP

# Assuming these are in the project structure
from db import DatabaseManager
from config import DEFAULT_DB_PATH
# Placeholder for rendering module, will be created later
from rendering import render_email_to_markdown, extract_clean_text, get_attachment_manifest

# For trigger_sync tool
from sync import sync_email_headers, sync_full_emails, sync_attachments
from imap_client import ImapClient, get_credentials # For on-demand client in sync
from config import DEFAULT_IMAP_HOST # Assuming we add this to config or have a way to get it

# Global FastMCP app instance
# We will attach db_manager to mcp.state during startup
mcp = FastMCP(
    title="Project Epsilon MCP Server",
    description="AI-First Local Email Intelligence Hub",
    version="0.1.0"
)

async def startup_event():
    """Handles application startup: connect to database."""
    print("MCP Server starting up...")
    db_manager = DatabaseManager(DEFAULT_DB_PATH)
    await db_manager.connect()
    mcp.state.db_manager = db_manager
    print("Database connected and ready.")

async def shutdown_event():
    """Handles application shutdown: close database connection."""
    print("MCP Server shutting down...")
    if hasattr(mcp.state, 'db_manager') and mcp.state.db_manager:
        await mcp.state.db_manager.close()
        print("Database connection closed.")

# Assign startup and shutdown events
mcp.router.lifespan_context = None # Not directly using lifespan_context for db like low-level, using app.state
mcp.add_event_handler("startup", startup_event)
mcp.add_event_handler("shutdown", shutdown_event)


@mcp.tool()
async def health_check() -> dict:
    """
    Checks the health of the MCP server.
    Returns a dictionary with the server status and current timestamp.
    """
    return {
        "status": "healthy",
        "message": "Project Epsilon MCP Server is running.",
        "timestamp": datetime.datetime.now().isoformat()
    }

@mcp.tool()
async def list_mailboxes() -> list[str]:
    """
    Lists all unique mailbox names found in the synchronized emails.
    """
    if not hasattr(mcp.state, 'db_manager') or not mcp.state.db_manager:
        raise RuntimeError("Database manager not initialized. Server might be starting up or an error occurred.")
    db_mngr: DatabaseManager = mcp.state.db_manager
    try:
        mailboxes = await db_mngr.get_mailboxes_from_full_emails()
        return mailboxes
    except Exception as e:
        print(f"Error in list_mailboxes: {e}")
        # Consider more specific MCP error responses in the future
        return {"error": f"Failed to list mailboxes: {str(e)}"}

@mcp.tool()
async def get_email_content_rendered(uid: str, mailbox: str, format: str = "markdown") -> dict:
    """
    Retrieves email content, rendered in the specified format.
    Currently, 'markdown' and 'clean_text' are placeholders for future rendering.
    'raw' will return the raw email content as a string (UTF-8 decoded, errors replaced).
    """
    if not hasattr(mcp.state, 'db_manager') or not mcp.state.db_manager:
        # Consider raising a specific MCP error if the SDK supports it
        raise RuntimeError("Database manager not initialized.")
    
    db_mngr: DatabaseManager = mcp.state.db_manager
    
    raw_email_bytes = await db_mngr.get_raw_email_content(uid, mailbox)

    if raw_email_bytes is None:
        return {"error": f"Email with UID {uid} in mailbox {mailbox} not found."}

    content = ""
    if format == "markdown":
        content = await render_email_to_markdown(raw_email_bytes)
        print(f"Rendered Markdown for UID {uid}")
    elif format == "clean_text":
        content = await extract_clean_text(raw_email_bytes)
        print(f"Rendered Clean Text for UID {uid}")
    elif format == "raw":
        # Return raw content, decoded to string for JSON compatibility
        content = raw_email_bytes.decode('utf-8', errors='replace')
        print(f"Providing Raw content for UID {uid}")
    else:
        return {"error": f"Unsupported format: {format}. Supported formats: markdown, clean_text, raw."}
        
    return {
        "uid": uid,
        "mailbox": mailbox,
        "format": format,
        "rendered_content": content 
    }

@mcp.tool()
async def get_email_attachments(uid: str, mailbox: str) -> dict:
    """
    Retrieves a manifest of attachments for a given email.
    """
    if not hasattr(mcp.state, 'db_manager') or not mcp.state.db_manager:
        raise RuntimeError("Database manager not initialized.")
    db_mngr: DatabaseManager = mcp.state.db_manager
    try:
        raw_email_bytes = await db_mngr.get_raw_email_content(uid, mailbox)
        if raw_email_bytes is None:
            return {"error": f"Email with UID {uid} in mailbox {mailbox} not found, cannot get attachments."}

        attachments = await get_attachment_manifest(raw_email_bytes) # Pass raw_email_bytes
        return {"uid": uid, "mailbox": mailbox, "attachments": attachments}
    except Exception as e:
        print(f"Error in get_email_attachments: {e}")
        return {"error": f"Failed to get attachments for UID {uid} in {mailbox}: {str(e)}"}

@mcp.tool()
async def trigger_sync(sync_mode: str, mailbox_name: str = "INBOX", all_mailboxes: bool = False, user_email: str = None) -> dict:
    """
    Triggers an email synchronization task. This is currently a blocking operation.
    Requires IMAP credentials to be configured if sync_mode is 'headers' or 'full'.
    'user_email' parameter is crucial for IMAP operations.
    """
    if not hasattr(mcp.state, 'db_manager') or not mcp.state.db_manager:
        raise RuntimeError("Database manager not initialized.")
    db_mngr: DatabaseManager = mcp.state.db_manager

    # For IMAP-dependent syncs, we need credentials and user_email.
    # This is a simplified approach; a more robust server might manage credentials securely.
    if sync_mode in ["headers", "full"] and not user_email:
        return {"error": "user_email is required for 'headers' or 'full' sync mode."}

    print(f"Triggering sync: mode={sync_mode}, mailbox={mailbox_name}, all_mailboxes={all_mailboxes}, user={user_email}")
    
    # Assume CLIENT_SECRET_PATH is available via config import, or passed differently
    # For simplicity, using DEFAULT_CREDS_JSON_PATH and DEFAULT_IMAP_HOST from config.py
    # These should ideally be configurable for the MCP server instance itself.
    creds = get_credentials(config.DEFAULT_CREDS_JSON_PATH) if sync_mode in ["headers", "full"] else None
    if sync_mode in ["headers", "full"] and not creds:
        return {"error": "Failed to get IMAP credentials for sync."}

    imap_client = None
    status_message = ""
    error_message = None

    try:
        if sync_mode in ['headers', 'full']:
            imap_client = ImapClient(config.DEFAULT_IMAP_HOST, user_email, creds)
            await imap_client.connect()

        if sync_mode == 'headers':
            if all_mailboxes:
                mailboxes_to_sync = await imap_client.list_mailboxes()
                for mbx in mailboxes_to_sync:
                    print(f"Syncing headers for {mbx}...")
                    await sync_email_headers(db_mngr, imap_client, mbx)
                status_message = f"Header sync completed for all mailboxes: {', '.join(mailboxes_to_sync)}"
            else:
                await sync_email_headers(db_mngr, imap_client, mailbox_name)
                status_message = f"Header sync completed for mailbox: {mailbox_name}"
        elif sync_mode == 'full':
            if all_mailboxes:
                mailboxes_to_sync = await imap_client.list_mailboxes()
                for mbx in mailboxes_to_sync:
                    print(f"Syncing full emails for {mbx}...")
                    await sync_full_emails(db_mngr, imap_client, mbx)
                status_message = f"Full email sync completed for all mailboxes: {', '.join(mailboxes_to_sync)}"
            else:
                await sync_full_emails(db_mngr, imap_client, mailbox_name)
                status_message = f"Full email sync completed for mailbox: {mailbox_name}"
        elif sync_mode == 'attachments':
            if all_mailboxes:
                # Attachment sync can get mailboxes from DB
                mailboxes_to_sync = await db_mngr.get_mailboxes_from_full_emails()
                if not mailboxes_to_sync:
                     return {"status": "SKIPPED", "message": "No mailboxes with full emails found in DB to process attachments."}
                for mbx in mailboxes_to_sync:
                    print(f"Syncing attachments for {mbx}...")
                    await sync_attachments(db_mngr, mbx)
                status_message = f"Attachment sync completed for all mailboxes in DB: {', '.join(mailboxes_to_sync)}"
            else:
                await sync_attachments(db_mngr, mailbox_name)
                status_message = f"Attachment sync completed for mailbox: {mailbox_name}"
        else:
            return {"error": f"Unsupported sync_mode: {sync_mode}"}
        
        print(status_message)
        return {"status": "COMPLETED", "message": status_message}

    except Exception as e:
        import traceback
        error_message = f"Error during sync operation ({sync_mode}): {str(e)}"
        print(f"{error_message}\n{traceback.format_exc()}")
        return {"status": "ERROR", "message": error_message}
    finally:
        if imap_client:
            await imap_client.close()

# --- Placeholder AI Tools (to be implemented with semantic_engine_ollama.py) ---

@mcp.tool()
async def summarize_email(uid: str, mailbox: str, style: str = "bullet_points", length: str = "short") -> dict:
    """(Placeholder) Summarizes the content of a specific email."""
    # 1. Fetch email content (e.g., using get_email_content_rendered or raw via db_manager)
    # 2. Call semantic_engine_ollama.ollama_summarize(content, style, length)
    print(f"[AI STUB] summarize_email called for UID {uid}, Mailbox {mailbox}")
    return {
        "uid": uid,
        "mailbox": mailbox,
        "summary": f"Placeholder summary for UID {uid} ({style}, {length}). Depends on semantic_engine_ollama.py."
    }

@mcp.tool()
async def extract_from_email(uid: str, mailbox: str, extraction_schema: dict) -> dict:
    """(Placeholder) Extracts structured information from an email based on a schema."""
    # 1. Fetch email content.
    # 2. Call semantic_engine_ollama.ollama_extract_info(content, extraction_schema)
    print(f"[AI STUB] extract_from_email called for UID {uid} with schema: {extraction_schema}")
    return {
        "uid": uid,
        "mailbox": mailbox,
        "extracted_data": {"placeholder_field": "Data for schema...", "notes": "Depends on semantic_engine_ollama.py"}
    }

@mcp.tool()
async def classify_email(uid: str, mailbox: str, categories: list[str]) -> dict:
    """(Placeholder) Classifies an email into one of the provided categories."""
    # 1. Fetch email content.
    # 2. Call semantic_engine_ollama.ollama_classify(content, categories)
    print(f"[AI STUB] classify_email called for UID {uid} with categories: {categories}")
    return {
        "uid": uid,
        "mailbox": mailbox,
        "classification": f"Placeholder classification (e.g., {categories[0] if categories else 'N/A'}). Depends on semantic_engine_ollama.py"
    }

@mcp.tool()
async def draft_reply_to_email(uid: str, mailbox: str, reply_prompt: str) -> dict:
    """(Placeholder) Drafts a reply to a specific email based on a prompt."""
    # 1. Fetch original email content for context.
    # 2. Call semantic_engine_ollama.ollama_generate_reply_draft(original_content, reply_prompt)
    print(f"[AI STUB] draft_reply_to_email called for UID {uid} with prompt: {reply_prompt}")
    return {
        "uid": uid,
        "mailbox": mailbox,
        "draft_reply": f"Placeholder draft reply for UID {uid}: \"Thanks for your email regarding...\" (based on prompt: {reply_prompt}). Depends on semantic_engine_ollama.py"
    }

# Placeholder for the get_raw_email_content in DatabaseManager
# This will be moved to db.py
# async def get_raw_email_content_placeholder(db_manager: DatabaseManager, uid: str, mailbox: str):
#     # Simulate fetching from DB. In reality, this would be an async DB query.
#     # Example:
#     # async with db_manager.db.execute("SELECT raw_email FROM full_emails WHERE uid = ? AND mailbox = ?", (uid, mailbox)) as cursor:
#     #     row = await cursor.fetchone()
#     #     return row[0] if row else None
#     print(f"[DB Placeholder] Fetching raw_email for UID {uid}, Mailbox {mailbox}")
#     if uid == "123_test" and mailbox == "INBOX_test":
#         return b"From: test@example.com\nTo: user@example.com\nSubject: Test Email\n\nThis is a test email body."
#     return None


if __name__ == "__main__":
    print("Starting Project Epsilon MCP Server...")
    # Note: For FastMCP, it should be `uvicorn fastmcp_server:mcp --reload`
    # The first argument to uvicorn.run should be the import string or the app instance.
    uvicorn.run(mcp, host="0.0.0.0", port=8001, log_level="info") 