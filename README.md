# SyncEmail: Your Gmail to SQLite Powerhouse

SyncEmail is a Python-based command-line tool designed to efficiently download, store, and analyze your Gmail emails by synchronizing them to a local SQLite database. It features robust OAuth 2.0 authentication, flexible sync modes, powerful querying capabilities, and insightful analytics, all accessible through a clear and modular interface.

> _A Note on Origins: This project benefited from AI-assisted development. While refined and functional, it began as a collaborative effort with an LLM. We've aimed for robust, human-quality code and documentation._

*A Note from the Author*: The above paragraph was written by the LLM. ROTFL!!!

## Key Features

*   **Secure Gmail Access:** Utilizes OAuth 2.0 for authentication, never storing your password.
*   **Flexible Synchronization:**
    *   **Headers-Only Sync:** Quickly download email metadata (sender, recipient, subject, date).
    *   **Full Email Sync:** Download complete email content, including body and attachments.
    *   **Attachment Extraction:** Process downloaded full emails to extract, normalize, and deduplicate attachments.
*   **Efficient & Resilient:**
    *   **Incremental Updates:** Only fetches new or changed data since the last sync.
    *   **Checkpoint System:** Saves progress regularly per mailbox and sync mode, allowing resumption after interruptions.
    *   **Error Tracking:** Identifies and logs emails that failed to process, with options for retrying.
*   **Local SQLite Database:**
    *   **Portable & Fast:** Stores all data in a single file, requiring no external database server.
    *   **Rich Schema:** Includes generated columns for quick insights (e.g., `has_attachments`, `message_size_kb`, `message_id`).
    *   **Automatic Schema Migrations:** The tool can often detect and apply necessary schema updates.
*   **Powerful Data Interaction:**
    *   **Query Mode:** Execute predefined and custom SQL queries to explore your email data.
    *   **Analytics Mode:** Generate text-based charts and calendar heatmaps for email and attachment statistics directly in your terminal.
*   **Advanced Functionality:**
    *   **Attachment Deduplication:** Saves storage by storing each unique attachment (by SHA256 hash) only once.
    *   **Large Mailbox Handling:** Employs chunking strategies for robust syncing of very large mailboxes (e.g., "[Gmail]/All Mail").
    *   **Multi-Mailbox Operations:** Sync all or selected mailboxes with a single command.

## Prerequisites

*   Python 3.7 or higher.
*   [uv](https://github.com/astral-sh/uv): A fast Python package installer and resolver (recommended for environment and package management).

## Installation

1.  **Clone the Repository:**
    ```bash
    git clone https://github.com/hugoferreira/gmail-sqlite-db.git
    cd gmail-sqlite-db
    ```

2.  **Install `uv` (if not already installed):**
    ```bash
    pip install uv
    ```
    (Or follow official `uv` installation instructions for your system.)

3.  **Create a Virtual Environment and Install Dependencies:**
    ```bash
    uv venv
    source .venv/bin/activate  # On Windows: .venv\\Scripts\\activate
    uv pip install -r requirements.txt
    ```

## Google OAuth 2.0 Setup

To allow SyncEmail to access your Gmail account, you need to create OAuth 2.0 credentials:

1.  Go to the [Google Cloud Console](https://console.cloud.google.com/).
2.  **Create a new project** or select an existing one.
3.  **Enable the Gmail API:**
    *   Navigate to "APIs & Services" > "Library".
    *   Search for "Gmail API" and enable it for your project.
4.  **Configure OAuth consent screen:**
    *   Navigate to "APIs & Services" > "OAuth consent screen".
    *   Choose "External" (unless you have a Google Workspace account and want to limit it internally).
    *   Fill in the required application details (app name, user support email, developer contact).
    *   Scopes: You don't need to add scopes here; the application will request them.
    *   Test users: Add your Gmail address(es) as test users while the app is in "testing" status.
5.  **Create OAuth 2.0 Client ID credentials:**
    *   Navigate to "APIs & Services" > "Credentials".
    *   Click "Create Credentials" > "OAuth client ID".
    *   Select "Desktop app" as the Application type.
    *   Give it a name (e.g., "SyncEmail Client").
6.  **Download Credentials:**
    *   After creation, download the JSON file.
    *   Save this file as `creds.json` in the SyncEmail project directory, or use a custom path via the `--creds` argument.

The first time you run a command requiring Gmail access (like `sync` or `list-mailboxes`), SyncEmail will open a browser window, asking you to log in to your Google account and grant permission. After successful authorization, a `token.json` file will be created in your project directory to store your access and refresh tokens for future sessions.

## Usage

SyncEmail is controlled via command-line arguments and sub-commands.

**Basic Command Structure:**
```bash
uv run main.py [GLOBAL_OPTIONS] COMMAND [COMMAND_OPTIONS]
```
Activate your virtual environment (`source .venv/bin/activate`) before running.

### Global Options

These options can be used with any command:

*   `--db DB_PATH`: Path to the SQLite database file.
    *   Default: `mail.sqlite3` (as defined in `config.py`)
*   `--creds CREDS_PATH`: Path to your OAuth2 client secrets JSON file.
    *   Default: `creds.json` (as defined in `config.py`)
*   `--host IMAP_HOST`: IMAP host address.
    *   Default: `imap.gmail.com`
*   `--user YOUR_GMAIL_ADDRESS`: Your Gmail address. **Required for commands that access Gmail.**
*   `--debug`: Enable detailed debug output.

### Commands

#### 1. `list-mailboxes`

Lists all available mailboxes on your Gmail account.

**Required Global Options:** `--user`, `--creds` (if not default), `--host` (if not default).

**Example:**
```bash
uv run main.py --user your.email@gmail.com list-mailboxes
```

#### 2. `sync`

Synchronizes emails with the local database. This command has several sub-modes:

**Common Sync Options (for `headers`, `full`, `attachments` modes):**

*   `--mailbox MAILBOX_NAME`: Specific mailbox to target (e.g., "INBOX", "Sent").
    *   Default: `INBOX`
*   `--all-mailboxes`: Apply the sync operation to all accessible mailboxes.

**Required Global Options for `sync headers` and `sync full`:** `--user`, `--creds`, `--host`.
**Required Global Options for `sync attachments`:** Only `--db` (IMAP access is not needed as it works from the local DB).

**Sub-modes for `sync`:**

*   **`headers`**: Syncs only email headers (metadata).
    ```bash
    # Sync headers for INBOX
    uv run main.py --user your.email@gmail.com sync headers
    # Sync headers for a specific mailbox
    uv run main.py --user your.email@gmail.com sync headers --mailbox "Sent Items"
    # Sync headers for all mailboxes
    uv run main.py --user your.email@gmail.com sync headers --all-mailboxes
    ```

*   **`full`**: Syncs full email content (body and attachments). Headers should ideally be synced first.
    ```bash
    # Sync full emails for INBOX
    uv run main.py --user your.email@gmail.com sync full
    # Sync full emails for all mailboxes
    uv run main.py --user your.email@gmail.com sync full --all-mailboxes
    ```

*   **`attachments`**: Extracts and normalizes attachments from already downloaded full emails.
    ```bash
    # Extract attachments for INBOX (from emails in mail.sqlite3)
    uv run main.py sync attachments --mailbox INBOX
    # Extract attachments for all mailboxes
    uv run main.py sync attachments --all-mailboxes
    ```

#### 3. `query`

Executes predefined SQL queries against the email database.

**Required Global Options:** `--db` (if not default).

**Query Options:**

*   `--list-queries`: List all available predefined queries.
*   `query_name`: (Positional argument) The name of the query to execute.
*   `--limit N`: Limit the number of results.
*   `--start-date YYYY-MM-DD`: Start date for date-sensitive queries.
*   `--end-date YYYY-MM-DD`: End date for date-sensitive queries.
*   `--message-id MSG_ID`: Message-ID for email thread queries.

**Examples:**
```bash
# List available queries
uv run main.py query --list-queries

# Get top 10 senders
uv run main.py query top_senders --limit 10

# View emails from a specific date range
uv run main.py query date_range --start-date 2023-01-01 --end-date 2023-01-31

# Find emails in a conversation thread
uv run main.py query thread --message-id "<message-id@example.com>"
```

#### 4. `analytics`

Generates and displays analytics from the email database using text-based charts.

**Required Global Options:** `--db` (if not default).

**Analytics Options:**

*   `--year YYYY`: Year for which to generate analytics (default: current year).
*   `--calendar`: Display a calendar heatmap instead of a monthly density chart.
*   `--metric METRIC_NAME`: Metric to visualize. Default: `emails`.
    Available metrics (defined in `queries.py` under `METRIC_QUERIES`):
    *   `emails`: Number of emails.
    *   `attachments`: Number of attachments (by email date).
    *   `attachment_size`: Total attachment size in bytes (by email date).
    *   `unique_attachments`: Unique attachments by hash (by email date).
    *   `avg_attachment_size`: Average attachment size in bytes (by email date).

**Examples:**
```bash
# Monthly email density bar chart for 2023
uv run main.py analytics --year 2023

# Calendar heatmap of email activity for 2023
uv run main.py analytics --year 2023 --calendar

# Monthly number of attachments for 2023
uv run main.py analytics --year 2023 --metric attachments

# Calendar heatmap of total attachment size for 2023
uv run main.py analytics --year 2023 --calendar --metric attachment_size
```
Example output:
```
$> uv run main.py analytics --year 2023 --metric emails          

Number of Emails for 2023 (monthly):

Jan: ▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇ 427
Feb: ▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇ 455
...
```

## Database Schema

The SQLite database (`mail.sqlite3` by default) stores your email data in several tables:

1.  **`emails`**: Core metadata for each email.
    *   `uid`: Email UID (unique per mailbox).
    *   `msg_from`, `msg_to`, `msg_cc`: Sender, recipient(s), CC recipient(s).
    *   `subject`: Email subject.
    *   `msg_date`: Date in ISO format.
    *   `mailbox`: Source mailbox name.

2.  **`full_emails`**: Stores complete raw email content and derived metadata.
    *   `uid`, `mailbox`: Foreign keys to the `emails` table.
    *   `raw_email`: Complete raw email content (BLOB).
    *   `fetched_at`: Timestamp of when the full email was downloaded.
    *   Generated columns (automatically populated by SQLite if supported, or by the application during insertion if not):
        *   `has_attachments`: (Boolean) Whether the email contains attachments.
        *   `message_size_kb`: (Integer) Size of the email in kilobytes.
        *   `is_html`, `is_plain_text`: (Boolean) Indicates content types.
        *   `has_images`: (Boolean) Whether the email has embedded images.
        *   `in_reply_to`, `message_id`: Standard email header fields for threading.

3.  **`attachment_blobs`**: Stores unique attachment binary data.
    *   `sha256`: SHA-256 hash of the attachment content (Primary Key). Ensures deduplication.
    *   `content`: Binary content of the attachment (BLOB).
    *   `size`: Size of the attachment in bytes.
    *   `fetched_at`: Timestamp of when the blob was first stored.

4.  **`email_attachments`**: Links emails to their attachments.
    *   `id`: Auto-incrementing primary key.
    *   `uid`, `mailbox`: Identifies the email.
    *   `sha256`: Foreign key to `attachment_blobs.sha256`.
    *   `filename`: Original filename of the attachment.
    *   `fetched_at`: Timestamp of when this mapping was created.

5.  **`attachment_info` (View)**: A pre-defined SQL view for convenient querying of attachment details along with email metadata. It joins `email_attachments`, `attachment_blobs`, and `emails`.

6.  **`sync_status`**: Logs metadata about synchronization operations.
    *   `id`: Sync operation ID.
    *   `last_uid`: Last processed UID during the sync operation (context-dependent).
    *   `start_time`, `end_time`: Timestamps for the operation.
    *   `status`: e.g., `STARTED`, `COMPLETED`, `ERROR`, `INTERRUPTED`.
    *   `message`: Additional information about the sync status.

## Key Concepts and Advanced Details

*   **Checkpoint System:**
    SyncEmail uses JSON files (in a `.checkpoints` directory) to save the progress of sync operations. This is done separately for each mailbox and for each sync mode (`headers`, `full`, `attachments`). If a sync is interrupted, it can resume from the last successfully processed point, significantly improving efficiency for large mailboxes or unstable connections. Failed UIDs are also tracked for potential retries.

*   **Handling Large Mailboxes:**
    For exceptionally large mailboxes (like Gmail's "[Gmail]/All Mail"), fetching all UIDs at once can be problematic. SyncEmail employs strategies like date-based chunking (searching for emails in monthly segments) or sequence-based chunking to manage these scenarios more reliably.

*   **Attachment Deduplication:**
    When extracting attachments (`sync attachments` mode), SyncEmail calculates the SHA-256 hash of each attachment's content. The binary data of an attachment is stored only once in the `attachment_blobs` table, even if the same file was attached to multiple emails. The `email_attachments` table then links emails to these unique blobs. This saves significant disk space.

## Useful SQLite Queries

Here are some examples of SQL queries you can run directly against your SQLite database (e.g., using `sqlite3 mail.sqlite3` or a GUI tool). Many of these are also available through the `query` command.

*   **Top Senders by Count:**
    ```sql
    SELECT msg_from, COUNT(*) as count 
    FROM emails 
    GROUP BY msg_from 
    ORDER BY count DESC 
    LIMIT 20;
    ```

*   **Emails by Date Range:**
    ```sql
    SELECT subject, msg_from, msg_date FROM emails 
    WHERE msg_date BETWEEN '2023-01-01' AND '2023-12-31' 
    ORDER BY msg_date DESC;
    ```

*   **Count Emails per Mailbox:**
    ```sql
    SELECT mailbox, COUNT(*) as email_count 
    FROM emails 
    GROUP BY mailbox 
    ORDER BY email_count DESC;
    ```

*   **Find Large Emails with Attachments (using `full_emails` generated columns):**
    ```sql
    SELECT e.subject, f.message_size_kb, e.msg_date
    FROM emails e
    JOIN full_emails f ON e.uid = f.uid AND e.mailbox = f.mailbox
    WHERE f.has_attachments = 1
    ORDER BY f.message_size_kb DESC
    LIMIT 20;
    ```

*   **Find Largest Attachments (using the `attachment_info` view):**
    ```sql
    SELECT filename, size / 1024 / 1024.0 AS size_mb, msg_date, subject
    FROM attachment_info
    ORDER BY size DESC
    LIMIT 20;
    ```

*   **Find Emails with PDF Attachments:**
    ```sql
    SELECT DISTINCT e.subject, e.msg_from, e.msg_date, ai.filename
    FROM attachment_info ai
    JOIN emails e ON ai.uid = e.uid AND ai.mailbox = e.mailbox
    WHERE LOWER(ai.filename) LIKE '%.pdf'
    ORDER BY e.msg_date DESC;
    ```

*   **Attachment Types by Count and Size:**
    ```sql
    SELECT 
      LOWER(SUBSTR(filename, INSTR(filename, '.') + 1)) as extension,
      COUNT(*) as count,
      SUM(size) / 1024 / 1024.0 as total_size_mb
    FROM attachment_info
    WHERE INSTR(filename, '.') > 0
    GROUP BY extension
    ORDER BY count DESC;
    ```

## Troubleshooting

*   **`ModuleNotFoundError`**: Ensure your virtual environment is active (`source .venv/bin/activate`) and all dependencies are installed (`uv pip install -r requirements.txt`). Run the script using `uv run main.py ...`.
*   **OAuth2 Errors (`invalid_grant`, etc.)**:
    *   Ensure your `creds.json` file is correctly configured and points to the credentials for a "Desktop app".
    *   Verify the Gmail API is enabled in your Google Cloud Project.
    *   If you recently changed your Google password or revoked access, delete `token.json` and re-authenticate.
    *   Ensure your system clock is accurate.
*   **Database Issues (`no such column`, `database locked`):**
    *   `no such column`: The tool attempts to perform schema migrations. If errors persist, especially after an update, backing up and then deleting the database file to let SyncEmail recreate it might be a solution (data will need tobe re-synced).
    *   `database is locked`: Ensure no other application or process is actively using the SQLite database file.
*   **IMAP Issues (Connection timeouts, errors with specific mailboxes):**
    *   Check your internet connection.
    *   Gmail has rate limits. While SyncEmail tries to be respectful, very intensive operations on huge mailboxes might hit them. Try again later or sync smaller subsets of mailboxes if issues persist.
    *   Some special mailboxes might have non-standard behavior.

## How It Works (Simplified)

1.  **Authentication:** Connects to Gmail using OAuth 2.0 via the `imap_client` module.
2.  **Command Parsing:** `main.py` parses command-line arguments and dispatches to the appropriate handler function.
3.  **Synchronization (`sync` module):**
    *   Fetches email UIDs from the specified mailbox.
    *   Compares with UIDs in the local database and checkpoint files to determine new/changed emails.
    *   For `headers` mode: Fetches only header information.
    *   For `full` mode: Fetches the entire raw email.
    *   For `attachments` mode: Parses raw emails from the DB, extracts attachments, calculates hashes, and stores them.
    *   Data is saved to the SQLite database via the `db` module.
    *   Progress is tracked by the `checkpoint` module.
4.  **Querying (`queries` module):** Executes SQL queries against the database.
5.  **Analytics (`analytics` module):** Runs aggregation queries and uses `termgraph` (if data is suitable) for visualization.

## AI-Powered Email Intelligence Hub (MCP Server)

This project can be transformed into an AI-First Local Email Intelligence Hub by running as a Model Context Protocol (MCP) server. This allows Large Language Models (LLMs), AI agents, and other MCP-compatible clients (like Cursor or Claude Desktop) to securely and intelligently interact with your email data, locally.

**Why Use the MCP Server?**

*   **Local & Private:** Your email data and any LLM interactions (if you integrate a local LLM like Ollama) remain on your machine, ensuring maximum privacy.
*   **Intelligent Access:** Exposes your email data through a structured set of tools, enabling sophisticated queries, summarization, and analysis via LLMs.
*   **Extensible:** Start with core email functions and pave the way for advanced AI capabilities like semantic search, automated classification, and draft generation.

### Running the SyncEmail MCP Server

1.  **Ensure Dependencies are Installed:**
    Make sure your virtual environment is active and all requirements, including `modelcontextprotocol` and `uvicorn`, are installed:
    ```bash
    # If you haven't already, activate your virtual environment
    # source .venv/bin/activate
    uv pip install -r requirements.txt
    ```

2.  **Start the Server:**
    Use the `serve-mcp` command. You'll need to provide your email address for context, even if just listing mailboxes from the local DB initially.
    ```bash
    uv run main.py --user your.email@gmail.com serve-mcp --mcp-host 0.0.0.0 --mcp-port 8001
    ```
    *   `--user YOUR_GMAIL_ADDRESS`: Your email address. Essential for the server to know which email account's data to surface, even if it's only from the local DB for some tools.
    *   `--mcp-host HOST_IP`: The IP address the server will listen on. Use `0.0.0.0` to allow connections from any device on your local network, or `127.0.0.1` (localhost) for same-machine access only (default).
    *   `--mcp-port PORT`: The port number the server will use (default: `8001`).

    The server will start, and you should see output indicating it's running, for example:
    ```
    INFO:     Uvicorn running on http://0.0.0.0:8001 (Press CTRL+C to quit)
    MCP Server starting up...
    Database connected and ready.
    ```

### Connecting MCP Clients (e.g., Claude Desktop, Cursor)

To use the SyncEmail MCP server with a client application, you typically need to tell the client how to find and run your local server. Many clients, like Claude Desktop, use a JSON configuration file.

**Example Client Configuration (Claude Desktop `claude_desktop_config.json`):**

Create or open your client's MCP configuration file. For Claude Desktop on macOS, this is usually at `~/Library/Application Support/Claude/claude_desktop_config.json` (Windows: `%APPDATA%\Claude\claude_desktop_config.json`).

Add an entry for the SyncEmail server. **Remember to replace `/ABSOLUTE/PATH/TO/YOUR/gmail-sqlite-db` with the actual absolute path to your cloned project directory.**

```json
{
  "mcpServers": {
    "syncEmail": { // This is the name that will appear in your MCP client
      "command": "uv",
      "args": [
        "run",
        "--project-dir", "/ABSOLUTE/PATH/TO/YOUR/gmail-sqlite-db", // Crucial: Absolute path!
        "main.py",
        "--user", "your.email@gmail.com", // Match the email used to start the server
        "serve-mcp",
        "--mcp-port", "8001" // Ensure this port matches how you run the server
      ],
      "env": { // Optional: if you need to set environment variables for the server
        // "YOUR_ENV_VAR": "value"
      },
      "startupTimeoutSeconds": 60 // Optional: time to wait for server to start
    }
    // You can add other MCP servers here
  }
}
```

**Key points for client configuration:**

*   **`syncEmail`**: This is a label. You can name it whatever you like (e.g., "LocalEmailHub"). This name will appear in your MCP client's list of available tools/servers.
*   **`command`**: The executable to run. We use `uv`.
*   **`args`**: The arguments to pass to the command.
    *   `--project-dir /ABSOLUTE/PATH/TO/YOUR/gmail-sqlite-db`: **This is critical.** Provide the full, absolute path to the root of your `gmail-sqlite-db` project directory. Relative paths usually don't work here.
    *   `main.py`: The script to run.
    *   `--user your.email@gmail.com`: **Important!** Provide the same email address you intend to use with the server. This ensures the server context is correct.
    *   `serve-mcp`: The command for `main.py` to start the MCP server.
    *   `--mcp-port 8001`: Match the port you use. If you change the port when running `uv run main.py ... serve-mcp`, change it here too.
*   **Restart your MCP client** after saving the configuration file for changes to take effect.

Once configured, your MCP client should list "syncEmail" (or your chosen name) as an available context/tool provider. You can then interact with its tools.

### Available MCP Tools

The SyncEmail MCP server exposes the following tools:

*   **`health_check()`**
    *   **Description:** Verifies that the MCP server is running and responsive.
    *   **Parameters:** None.
    *   **Returns:** A dictionary with server status and timestamp.
    *   *Example LLM Interaction:* "Is the SyncEmail server online?"

*   **`list_mailboxes()`**
    *   **Description:** Lists all unique mailbox names found in the synchronized emails in your local database.
    *   **Parameters:** None.
    *   **Returns:** A list of mailbox name strings.
    *   *Example LLM Interaction:* "What mailboxes have I synced with SyncEmail?"

*   **`get_email_content_rendered(uid: str, mailbox: str, format: str = "markdown")`**
    *   **Description:** Retrieves the content of a specific email, rendered in the desired format.
    *   **Parameters:**
        *   `uid` (str): The UID of the email.
        *   `mailbox` (str): The mailbox where the email resides.
        *   `format` (str, optional): Desired output format. Options: `"markdown"`, `"clean_text"`, `"raw"`. Defaults to `"markdown"`.
    *   **Returns:** A dictionary containing the email content.
    *   *Example LLM Interaction:* "Get the markdown content of email UID 12345 from INBOX."

*   **`get_email_attachments(uid: str, mailbox: str)`**
    *   **Description:** Retrieves a manifest (list of metadata) for attachments associated with a specific email.
    *   **Parameters:**
        *   `uid` (str): The UID of the email.
        *   `mailbox` (str): The mailbox where the email resides.
    *   **Returns:** A dictionary containing a list of attachment details (filename, MIME type, size, SHA256 hash, content_id).
    *   *Example LLM Interaction:* "What attachments does email UID 67890 in 'Project Alpha' have?"

*   **`trigger_sync(sync_mode: str, mailbox_name: str = "INBOX", all_mailboxes: bool = False, user_email: str = None)`**
    *   **Description:** Initiates an email synchronization task. **Note:** This is currently a blocking operation on the server.
    *   **Parameters:**
        *   `sync_mode` (str): Type of sync. Options: `"headers"`, `"full"`, `"attachments"`.
        *   `mailbox_name` (str, optional): The specific mailbox to sync. Defaults to `"INBOX"`.
        *   `all_mailboxes` (bool, optional): If `true`, syncs all accessible mailboxes. Defaults to `false`.
        *   `user_email` (str, optional): Your Gmail address. **Required** if `sync_mode` is `"headers"` or `"full"` to perform IMAP operations. Should match the `--user` the server was started with for consistency, but passed here for explicit sync operations.
    *   **Returns:** A dictionary indicating the status of the sync operation.
    *   *Example LLM Interaction:* "Trigger a 'headers' sync for the 'INBOX' using my.email@example.com with SyncEmail."

**(Future AI Tools)**

*As development progresses, tools for semantic search, email summarization, classification, and draft generation using local Ollama models will be added here.* This will further enhance SyncEmail as a private, AI-powered email assistant.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
