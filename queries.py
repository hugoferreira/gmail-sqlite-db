import tabulate
import datetime # For default date parameters in QUERIES

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
        # This part assumes parameters in the query string are simple '?' placeholders
        # and their order in the 'params' dict definition matches the '?' order.
        # A more robust solution might involve named parameters in SQL if supported, or careful ordering.
        params = [merged_params[name] for name in query_info['params']] # Uses original order from QUERIES definition
        # If user provides params not in original definition, they are ignored here unless query string uses more `?`
        # For now, we'll rely on the number of `?` matching len(query_info['params'])
        
        # More flexible parameter handling:
        # If the query string has ? and we need to fill them in order from merged_params based on the original definition's keys
        # This requires knowing the order of '?' placeholders in the query string.
        # The current approach is simpler: it takes values from merged_params in the order defined in query_info['params'].
        # This is usually fine if query_params only overrides existing default keys.

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