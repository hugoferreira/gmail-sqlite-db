# config.py - Centralized configuration for the application

# --- OAuth2 Configuration ---
# SCOPES: Defines the access scope for Google API. Typically for reading mail.
SCOPES = ['https://mail.google.com/']

# TOKEN_PATH: Path to the stored OAuth2 token file.
# This file stores user's access and refresh tokens, and is created
# automatically when the authorization flow completes for the first time.
TOKEN_PATH = 'token.json'

# CLIENT_SECRET_PATH: Path to the client secret JSON file downloaded from Google Cloud Console.
# This file is required for the OAuth2 flow to identify the application.
CLIENT_SECRET_PATH = 'creds.json' # Standard name, user must provide this file.

# --- Sync Behavior Configuration ---
# CHUNK_SIZE: Number of UIDs to process in a single batch during email fetching.
# Helps manage memory and API request sizes, especially for header sync.
CHUNK_SIZE = 250

# EMAILS_PER_COMMIT: Number of emails to process before committing changes to the database.
# Frequent commits reduce data loss rischio in case of interruption but can be slightly slower.
EMAILS_PER_COMMIT = 20

# --- Application Defaults ---
# DEFAULT_DB_PATH: Default path for the SQLite database file.
DEFAULT_DB_PATH = 'mail.sqlite3'

# DEFAULT_CREDS_JSON_PATH: Default path for the OAuth2 client secrets JSON file.
# This is distinct from TOKEN_PATH; this is the application's credential file, not the user token.
# Corresponds to the --creds argument in main.py, and should point to CLIENT_SECRET_PATH by default.
DEFAULT_CREDS_JSON_PATH = CLIENT_SECRET_PATH 

# --- Debugging --- 
# DEBUG: Global flag to enable or disable debug print statements and behaviors.
# Can be overridden by the --debug command-line argument.
DEBUG_MODE = False # Default to False, can be set by CLI 

# --- Sync Behavior Configuration ---
# MAX_UID_FETCH_RETRIES: Maximum number of times to retry fetching a specific UID
# before marking it as permanently failed for the current session.
MAX_UID_FETCH_RETRIES = 3 