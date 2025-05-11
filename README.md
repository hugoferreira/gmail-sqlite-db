# SyncEmail (a.k.a. GMail to SQLite)

A Python tool to efficiently sync Gmail emails to a local SQLite database using OAuth 2.0 authentication. But before you go...
A Python tool to efficiently sync Gmail email headers and content to a local SQLite database using OAuth 2.0 authentication. But before you go...

## Responsible AI usage

This project is yet another product of AI-assisted development (or as the hipsters call it, "Vibe Coding"). I needed a quick solution and let an LLM do the heavy lifting. While it (seemingly) works, the code quality might make your eyes water (and I've seen some pretty gnarly code in my time). Consider this your official warning: proceed with caution and maybe keep a bot^H^H^H some eye drops handy.

## Features

- **OAuth 2.0 Authentication**: Secure access to Gmail without storing your password
- **Header-Only Sync**: Efficiently stores only email metadata, not full content
- **Full Email Mode**: Option to download and store complete emails including attachments
- **Incremental Updates**: Resumes where it left off for efficient syncing
- **Checkpoint System**: Saves progress regularly to prevent data loss
- **Error Handling**: Tracks failed emails for retry in subsequent runs
- **SQLite Storage**: Fast, portable database that requires no separate server
- **Query Mode**: Built-in analytical queries for exploring your email data
- **Smart Schema Migration**: Automatically updates database schema when needed
- **Attachments Extraction**: Extract, normalize, and deduplicate email attachments
- **Attachment Deduplication**: Store each unique attachment only once using SHA-256 hashing
- **Date-Based Chunking**: Smart handling of very large mailboxes like "[Gmail]/All Mail"
- **Mailbox-Specific Tracking**: Maintain separate sync state for each mailbox
- **Multi-Mailbox Sync**: Sync all available mailboxes in one command
- **Analytics Visualizations**: Text-based charts and calendar heatmaps for email data

## Requirements

- Python 3.7 or higher
- [uv](https://github.com/astral-sh/uv) - Fast Python package installer and resolver (recommended)
- Required packages (installed via `uv pip install -r requirements.txt`):
  - aiosqlite: Asynchronous SQLite database access
  - google-auth, google-auth-oauthlib: OAuth2 authentication with Google
  - tqdm: Progress bars for sync operations
  - tabulate: Formatted table output for query results
  - termgraph: Text-based visualizations for analytics mode

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/hugoferreira/gmail-sqlite-db.git
   cd gmail-sqlite-db
   ```

2. Install `uv` if you don't have it yet:
   ```
   pip install uv
   ```

3. Create a virtual environment and install dependencies:
   ```
   uv venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   uv pip install -r requirements.txt
   ```

4. Create OAuth 2.0 credentials:
   - Go to the [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project
   - Enable the Gmail API
   - Create OAuth 2.0 Client ID credentials (Desktop application)
   - Download the credentials JSON file and save it as `creds.json` in the project directory

## Usage

Run the tool with the following command:

```
uv run --active main.py --db ./mail.sqlite3 --creds creds.json --user your.email@gmail.com
```

> **Note:** The `--creds` and `--user` arguments are only required for sync modes (`headers`, `full`, and `--list-mailboxes`). For analytics, attachments, and query modes, you do **not** need to provide these arguments.

### Command Line Arguments

- `--db`: Path to SQLite database (default: `emails.db`)
- `--creds`: Path to OAuth2 client secrets JSON (**required only for sync modes**)
- `--host`: IMAP host (default: `imap.gmail.com`)
- `--user`: Gmail address (**required only for sync modes**)
- `--mailbox`: Mailbox name (default: `INBOX`)
- `--all-mailboxes`: Sync all available mailboxes (for `headers`, `full`, and `attachments` modes)
- `--debug`: Enable debug mode
- `--list-mailboxes`: List available mailboxes and exit
- `--mode`: Execution mode: `headers` (default), `full` (fetch full emails), `attachments` (extract and normalize attachments), `analytics` (run analytics), or `query` (run queries)
- `--year`: Year for analytics (default: current year)
- `--calendar`: Show calendar heatmap for analytics mode
- `--metric`: Metric to visualize in analytics mode (see below)

### Syncing Email Headers

To sync only email headers (default mode):

```
uv run --active main.py --creds creds.json --user your.email@gmail.com
```

To sync headers from all mailboxes:

```
uv run --active main.py --creds creds.json --user your.email@gmail.com --all-mailboxes
```

### Syncing Full Emails

To download complete emails including attachments:

```
uv run --active main.py --mode full --creds creds.json --user your.email@gmail.com
```

Note: Always sync headers first before syncing full emails.

### Extracting and Normalizing Attachments

After syncing full emails, you can extract and normalize attachments:

```
uv run --active main.py --mode attachments --db ./mail.sqlite3
```

This will:
- Extract all attachments from the full emails
- Store unique attachments only once (deduplication via SHA-256 hashing)
- Create normalized tables relating emails to attachments
- Show statistics about attachment deduplication

For all mailboxes:

```
uv run --active main.py --mode attachments --db ./mail.sqlite3 --all-mailboxes
```

### Listing Available Mailboxes

To see all available mailboxes in your email account, use the `--list-mailboxes` argument:

```
uv run --active main.py --creds creds.json --user your.email@gmail.com --list-mailboxes
```

This will display a list of all mailboxes you can access, which you can then use with the `--mailbox` argument to sync emails from a specific mailbox.

### Analytics Mode

The analytics mode allows you to visualize your email and attachment data using text-based charts and heatmaps in the terminal. It uses [termgraph](https://github.com/mkaz/termgraph) for visualization (make sure it is installed).

#### Example Usage

**Monthly email density bar chart:**
```
python main.py --mode analytics --db mail.sqlite3 --year 2023
```

**Calendar heatmap of email activity:**
```
python main.py --mode analytics --db mail.sqlite3 --year 2023 --calendar
```

**Monthly number of attachments:**
```
python main.py --mode analytics --db mail.sqlite3 --year 2023 --metric attachments
```

**Calendar heatmap of total attachment size:**
```
python main.py --mode analytics --db mail.sqlite3 --year 2023 --calendar --metric attachment_size
```

#### Available Metrics

| Metric                | Description                                      |
|-----------------------|--------------------------------------------------|
| emails                | Number of emails                                 |
| attachments           | Number of attachments (by email date)            |
| attachment_size       | Total attachment size in bytes (by email date)   |
| unique_attachments    | Unique attachments (by hash, by email date)      |
| avg_attachment_size   | Average attachment size in bytes (by email date) |

You can use `--metric` with any analytics visualization. The default is `emails`.

#### Requirements

- [termgraph](https://github.com/mkaz/termgraph) must be installed (see requirements.txt).

#### Examples

Here's an example:

```
$> uv run main.py --mode analytics --year 2011 --metric emails --calendar 

Number of Emails calendar heatmap for 2011:

     Dec Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec 
Mon:  ▒░░▒ ▒▒▒ ░▒▒▒▒░░░░░▓▒▓▓▒░▒▒▒▒▒░░░▒░░▒▒▓▓▒▓▒ ▒▓▓▒▓▓▓▒
Tue:  ▒░▒░░░░▓▒░▓▒▒▒░▒▓▒▒▒▒░▓▒▒▓▓▒▒▒░▒░▒░▒▒▒▒▓▒▓▒░▒▓▓▓▓█▓▒
Wed:  ░░▒▒░▒░▒▒▒▒░░░░▒░░▒▓▓▒▒▒░▒▓▒░▒▒▒░░ ░▒▒░▒▓▒▒▒▒▓▓░▒▓█░
Thu:  ░░░░░▒░▓▒░▒▒░░▒░░▒▒▒▒▒░▒░ ▓▒▒▓░░░░░▒░▒▒██▒░▒▒▒▒░▒▓▓░
Fri:  ░▒▒░▒▒▒▒▒░░▒░▒▒░░░▒▒░▓░▓░▒▒░░▒▒░░░░░▒▒░▒▒▒▒▒▒▓▒█▓▓▒░
Sat: ░░░░░░░░░░░░▒░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░▒░ 
Sun: ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ ░░░░░░░░░░░░░░░░░░░░░ 
```

And another:

```
$> uv run main.py --mode analytics --year 2023 --metric emails          

Number of Emails for 2023 (monthly):

Jan: ▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇ 427
Feb: ▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇ 455
Mar: ▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇ 571
Apr: ▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇ 761
May: ▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇ 679
Jun: ▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇ 497
Jul: ▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇ 510
Aug: ▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇ 546
Sep: ▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇ 1K
Oct: ▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇ 1K
Nov: ▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇ 1K
Dec: ▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇ 1K
```

### Query Mode

The query mode allows you to run predefined analytical queries on your email database:

#### List Available Queries

```
uv run --active main.py --mode query --list-queries
```

#### Running Queries

To run a specific query:

```
uv run --active main.py --mode query --db ./mail.sqlite3 --query top_senders
```

#### Query Parameters

The following parameters can be used to customize queries:

- `--limit`: Maximum number of results to return
- `--start-date`: Start date for date range queries (YYYY-MM-DD format)
- `--end-date`: End date for date range queries (YYYY-MM-DD format)
- `--message-id`: Message ID for thread queries

#### Query Examples

1. Get top email senders:
   ```
   uv run --active main.py --mode query --query top_senders --limit 20
   ```

2. View emails from a specific date range:
   ```
   uv run --active main.py --mode query --query date_range --start-date 2023-01-01 --end-date 2023-12-31
   ```

3. Find large emails with attachments:
   ```
   uv run --active main.py --mode query --query large_attachments
   ```

4. See recent emails:
   ```
   uv run --active main.py --mode query --query recent --limit 10
   ```

5. Get a summary of your email database:
   ```
   uv run --active main.py --mode query --query summary
   ```

6. See distribution of emails by domain:
   ```
   uv run --active main.py --mode query --query email_domains
   ```

7. View emails with images:
   ```
   uv run --active main.py --mode query --query emails_with_images
   ```

8. See emails in a conversation thread (requires message ID):
   ```
   uv run --active main.py --mode query --query thread --message-id "<message-id@example.com>"
   ```

## Database Schema

The SQLite database contains the following main tables:

1. `emails` - Stores email metadata:
   - `uid` - Email UID (primary key)
   - `msg_from` - Sender information
   - `msg_to` - Recipient information
   - `msg_cc` - CC recipients
   - `subject` - Email subject
   - `msg_date` - Date in ISO format
   - `mailbox` - Source mailbox name (e.g., "INBOX", "Sent", etc.)

2. `full_emails` - Stores complete email content with generated columns:
   - `uid` - Email UID (primary key, matches `emails` table)
   - `mailbox` - Mailbox name
   - `raw_email` - Complete raw email content (BLOB)
   - `fetched_at` - Timestamp when the email was fetched
   - Generated columns (calculated automatically):
     - `has_attachments` - Boolean indicating if the email has attachments
     - `message_size_kb` - Size of the email in KB
     - `is_html` - Boolean indicating if the email has HTML content
     - `is_plain_text` - Boolean indicating if the email has plain text content
     - `has_images` - Boolean indicating if the email has embedded images
     - `in_reply_to` - Message ID this email is replying to
     - `message_id` - Unique message ID

3. `sync_status` - Tracks sync operations:
   - `id` - Sync operation ID
   - `last_uid` - Last processed UID
   - `start_time` - When sync started
   - `end_time` - When sync completed
   - `status` - Current status (STARTED, COMPLETED, ERROR, INTERRUPTED)
   - `message` - Additional status information

4. `attachment_blobs` - Stores unique attachment content:
   - `sha256` - SHA-256 hash of the content (primary key)
   - `content` - Binary content of the attachment (BLOB)
   - `size` - Size of the attachment in bytes
   - `fetched_at` - Timestamp when attachment was extracted

5. `email_attachments` - Maps emails to attachments:
   - `id` - Primary key
   - `uid` - Email UID (references emails table)
   - `mailbox` - Mailbox name
   - `sha256` - SHA-256 hash (references attachment_blobs table)
   - `filename` - Original filename of the attachment
   - `fetched_at` - Timestamp when mapping was created

6. `attachment_info` - A view joining email_attachments with emails and attachment_blobs:
   - Provides a unified view of attachments with email metadata
   - Includes sender, recipient, subject, date, filename, size, etc.

## Checkpoint System

The tool uses a sophisticated checkpoint system to track progress:

- **Mailbox-specific tracking**: Each mailbox has its own independent sync state
- **Resumable operations**: The tool can resume from where it left off for each mailbox
- **Failed email tracking**: UIDs that failed to sync are retried in subsequent runs
- **Progress persistence**: Checkpoint files save the state between runs

## Handling Large Mailboxes

The tool has special handling for very large mailboxes like "[Gmail]/All Mail":

- **Date-based chunking**: Breaks down searches into monthly chunks to avoid IMAP response size limits
- **Sequence-based chunking**: Alternative approach that fetches emails in small batches
- **Error resilience**: Can continue even if individual chunks fail
- **Progress tracking**: Shows detailed progress during large mailbox operations

## Attachment Deduplication

The tool implements efficient attachment storage:

- **Content-based deduplication**: Uses SHA-256 hashing to identify identical attachments
- **Storage efficiency**: Each unique attachment is stored only once
- **Normalized schema**: Maintains the relationship between emails and attachments
- **Statistics reporting**: Shows deduplication ratio and storage savings

## Useful SQLite Queries

### Top Email Senders by Count

```sql
SELECT msg_from, COUNT(*) as count 
FROM emails 
GROUP BY msg_from 
ORDER BY count DESC 
LIMIT 10;
```

### Extract Email Addresses from Sender Field

To extract just the email address from within angle brackets (`<email@domain.com>`):

```sql
CREATE VIEW email_senders AS 
SELECT 
    SUBSTR(msg_from, INSTR(msg_from, '<') + 1, INSTR(msg_from, '>') - INSTR(msg_from, '<') - 1) AS email_address, 
    COUNT(*) as count 
FROM emails 
WHERE INSTR(msg_from, '<') > 0 AND INSTR(msg_from, '>') > INSTR(msg_from, '<') 
GROUP BY email_address 
ORDER BY count DESC;
```

Then query the view:

```sql
SELECT * FROM email_senders LIMIT 20;
```

### Extract Email Domains from Sender Field

To extract just the domain portion of email addresses in the sender field:

```sql
CREATE VIEW domain_senders AS 
SELECT 
    SUBSTR(msg_from, INSTR(msg_from, '@') + 1, INSTR(msg_from, '>') - INSTR(msg_from, '@') - 1) AS domain, 
    COUNT(*) as count 
FROM emails 
WHERE INSTR(msg_from, '@') > 0 AND INSTR(msg_from, '>') > INSTR(msg_from, '@') 
GROUP BY domain 
ORDER BY count DESC;
```

Query the view to see the top domains:

```sql
SELECT * FROM domain_senders LIMIT 20;
```

### Emails by Date Range

```sql
SELECT * FROM emails 
WHERE msg_date BETWEEN '2023-01-01' AND '2023-12-31' 
ORDER BY msg_date DESC;
```

### Count Emails by Mailbox

```sql
SELECT 
    mailbox, 
    COUNT(*) as email_count 
FROM emails 
GROUP BY mailbox 
ORDER BY email_count DESC;
```

### New Queries Using Generated Columns

#### Find Large Emails with Attachments

```sql
SELECT e.subject, f.message_size_kb, e.msg_date
FROM emails e
JOIN full_emails f ON e.uid = f.uid AND e.mailbox = f.mailbox
WHERE f.has_attachments = 1
ORDER BY f.message_size_kb DESC
LIMIT 20;
```

#### Emails with Images

```sql
SELECT e.subject, e.msg_from, e.msg_date
FROM emails e
JOIN full_emails f ON e.uid = f.uid AND e.mailbox = f.mailbox
WHERE f.has_images = 1
ORDER BY e.msg_date DESC
LIMIT 50;
```

#### Find Message Threads

```sql
-- Find all emails in a thread using message_id and in_reply_to
WITH RECURSIVE thread(uid, mailbox, message_id, level) AS (
    -- Start with a specific message ID
    SELECT uid, mailbox, message_id, 0
    FROM full_emails
    WHERE message_id = '<specific-message-id@example.com>'
    
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
```

### Queries Using the Attachment Tables

#### Find Duplicate Attachments

```sql
SELECT sha256, COUNT(*) as occurrences
FROM email_attachments
GROUP BY sha256
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;
```

#### Find Largest Attachments

```sql
SELECT e.subject, ea.filename, ab.size/1024/1024 as size_mb, e.msg_date
FROM attachment_info ai
JOIN emails e ON ai.uid = e.uid AND ai.mailbox = e.mailbox
JOIN email_attachments ea ON ai.id = ea.id
JOIN attachment_blobs ab ON ea.sha256 = ab.sha256
ORDER BY ab.size DESC
LIMIT 20;
```

#### Find Emails with PDF Attachments

```sql
SELECT e.subject, e.msg_from, e.msg_date, ea.filename
FROM email_attachments ea
JOIN emails e ON ea.uid = e.uid AND ea.mailbox = e.mailbox
WHERE ea.filename LIKE '%.pdf'
ORDER BY e.msg_date DESC;
```

#### Find Attachments by File Extension

```sql
SELECT 
  LOWER(SUBSTR(filename, INSTR(filename, '.', -1) + 1)) as extension,
  COUNT(*) as count,
  SUM(ab.size)/1024/1024 as total_size_mb
FROM email_attachments ea
JOIN attachment_blobs ab ON ea.sha256 = ab.sha256
WHERE INSTR(filename, '.') > 0
GROUP BY extension
ORDER BY count DESC;
```

## Troubleshooting

### Running with uv

- **Virtual Environment Issues**: If you see `ModuleNotFoundError` for packages that should be installed, make sure to:
  1. Verify the package is installed with `uv pip list`
  2. Always use `uv run --active` to run the script to ensure it uses the active virtual environment
  3. If packages are missing, reinstall them with `uv pip install -r requirements.txt`

- **Environment Conflicts**: If you see warnings about environment paths not matching, use the `--active` flag with uv run to explicitly use the active environment.

### Database Issues

- **Missing Columns Error**: If you see errors like `no such column: has_images`, run the tool again. It will automatically detect and fix the database schema.
- **Database Locked**: If the database is locked, ensure no other process is using it and try again.
- **Performance Issues**: For large databases, consider using the `--limit` parameter with queries to reduce result size.

### OAuth2 Issues

- **Authentication Errors**: Ensure your `creds.json` file is valid and you've enabled the Gmail API
- **Token Expiration**: If your token expires, the tool will attempt to refresh it automatically. If that fails, delete `token.json` and run the tool again.

### IMAP Issues

- **Connection Timeout**: Check your internet connection or try again later
- **Rate Limiting**: Gmail has rate limits - the tool applies small delays but might still hit limits with large mailboxes
- **Large Mailbox Issues**: For problems with "[Gmail]/All Mail":
  - The tool uses special date-based chunking for this mailbox
  - If you encounter "Could not parse command" errors, try using a specific mailbox instead
  - You can reduce chunk size by modifying the code if needed

## How It Works

1. The tool authenticates with Gmail using OAuth 2.0
2. It fetches UIDs of all emails in the specified mailbox
3. It determines which emails are new since the last sync
4. It downloads just the header information for new emails
5. Headers are parsed and stored in the SQLite database
6. (Optional) Full email content can be downloaded in a second pass
7. Progress is saved in checkpoints for resumable operation
8. Attachments can be extracted and normalized with deduplication
9. Analytics can be generated from the email and attachment data

## License

MIT
