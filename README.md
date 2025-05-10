# SyncEmMail

A Python tool to efficiently sync Gmail email headers to a local SQLite database using OAuth 2.0 authentication.

## Features

- **OAuth 2.0 Authentication**: Secure access to Gmail without storing your password
- **Header-Only Sync**: Efficiently stores only email metadata, not full content
- **Incremental Updates**: Resumes where it left off for efficient syncing
- **Checkpoint System**: Saves progress regularly to prevent data loss
- **Error Handling**: Tracks failed emails for retry in subsequent runs
- **SQLite Storage**: Fast, portable database that requires no separate server

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/syncemail.git
   cd syncemmail
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Create OAuth 2.0 credentials:
   - Go to the [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project
   - Enable the Gmail API
   - Create OAuth 2.0 Client ID credentials (Desktop application)
   - Download the credentials JSON file and save it as `creds.json` in the project directory

## Usage

Run the tool with the following command:

```
python main.py --db ./mail.sqlite3 --creds creds.json --user your.email@gmail.com
```

### Command Line Arguments

- `--db`: Path to SQLite database (default: `emails.db`)
- `--creds`: Path to OAuth2 client secrets JSON (required)
- `--host`: IMAP host (default: `imap.gmail.com`)
- `--user`: Gmail address (required)
- `--mailbox`: Mailbox name (default: `INBOX`)
- `--debug`: Enable debug mode
- `--list-mailboxes`: List available mailboxes and exit

### Listing Available Mailboxes

To see all available mailboxes in your email account, use the `--list-mailboxes` argument:

```
python main.py --creds creds.json --user your.email@gmail.com --list-mailboxes
```

This will display a list of all mailboxes you can access, which you can then use with the `--mailbox` argument to sync emails from a specific mailbox.

## Database Schema

The SQLite database contains two main tables:

1. `emails` - Stores email metadata:
   - `uid` - Email UID (primary key)
   - `msg_from` - Sender information
   - `msg_to` - Recipient information
   - `msg_cc` - CC recipients
   - `subject` - Email subject
   - `msg_date` - Date in ISO format
   - `mailbox` - Source mailbox name (e.g., "INBOX", "Sent", etc.)

2. `sync_status` - Tracks sync operations:
   - `id` - Sync operation ID
   - `last_uid` - Last processed UID
   - `start_time` - When sync started
   - `end_time` - When sync completed
   - `status` - Current status (STARTED, COMPLETED, ERROR, INTERRUPTED)
   - `message` - Additional status information

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

## How It Works

1. The tool authenticates with Gmail using OAuth 2.0
2. It fetches UIDs of all emails in the specified mailbox
3. It determines which emails are new since the last sync
4. It downloads just the header information for new emails
5. Headers are parsed and stored in the SQLite database
6. Progress is saved in checkpoints for resumable operation

## Troubleshooting

- **Authentication Issues**: Ensure your `creds.json` file is valid and you've enabled the Gmail API
- **Permission Errors**: You may need to allow "Less secure app access" in your Google account
- **Database Locked**: If the database is locked, ensure no other process is using it
- **Rate Limiting**: Gmail has rate limits - the tool automatically applies small delays

## License

MIT
