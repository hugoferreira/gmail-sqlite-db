import re
from email.header import decode_header
from email.utils import parsedate_to_datetime

# Import constants from config.py
import config # Import the config module
from config import CHUNK_SIZE as CONFIG_CHUNK_SIZE, EMAILS_PER_COMMIT as CONFIG_EMAILS_PER_COMMIT

# Constants for use within this module, initialized from config
# These are distinct in case this module wants to use a different value internally
# but for now, they are the same.
CHUNK_SIZE = CONFIG_CHUNK_SIZE
EMAILS_PER_COMMIT = CONFIG_EMAILS_PER_COMMIT

def debug_print(*args, **kwargs):
    # Access DEBUG_MODE directly from the config module
    if config.DEBUG_MODE:
        print(*args, **kwargs)

# Parse email date into ISO format for better querying
def parse_email_date(date_str):
    if not date_str:
        return None
    
    try:
        return parsedate_to_datetime(date_str).isoformat()
    except:
        return date_str

# Decode MIME headers with better error handling
def decode_field(field):
    if not field:
        return ''
    parts = decode_header(field)
    decoded = ''
    for part, encoding in parts:
        if isinstance(part, bytes):
            try:
                # Handle unknown encodings gracefully
                if encoding and encoding.lower() == 'unknown-8bit':
                    decoded += part.decode('utf-8', errors='replace')
                else:
                    decoded += part.decode(encoding or 'utf-8', errors='replace')
            except (LookupError, UnicodeDecodeError):
                # Fallback to utf-8 with error replacement
                decoded += part.decode('utf-8', errors='replace')
        else:
            decoded += part
    return decoded

# Extract UID from FETCH response
def extract_uid(response_line):
    if isinstance(response_line, bytes):
        response_line = response_line.decode('utf-8', errors='replace')
    
    # Use debug_print from this module
    debug_print(f"Parsing response line: {response_line}")
    
    # Try different regex patterns to extract UID
    patterns = [
        r'UID (\d+)',         # Standard format
        r'\(UID (\d+)',       # Parenthesized format
        r'UID=(\d+)',         # Key-value format
        r'[^\d](\d+)[^\d]'    # Any number surrounded by non-digits
    ]
    
    for pattern in patterns:
        match = re.search(pattern, response_line)
        if match:
            uid = match.group(1)
            debug_print(f"  - Extracted UID: {uid}")
            return uid
    
    debug_print("  - No UID found")
    return None

# Parse IMAP response for UIDs and headers
def parse_imap_response(data):
    """Parse the IMAP FETCH response to extract UIDs and header data."""
    # Use debug_print from this module
    debug_print(f"Response data has {len(data)} elements")
    
    # Debug first few elements
    for i in range(min(2, len(data))):
        debug_print(f"Data[{i}] type: {type(data[i])}")
        if isinstance(data[i], bytes):
            try:
                debug_print(f"Data[{i}] (first 100 bytes): {data[i][:100]}")
            except:
                debug_print(f"Data[{i}]: Unable to print")
        elif isinstance(data[i], tuple):
            debug_print(f"Data[{i}] is tuple of length {len(data[i])}")
    
    messages = []
    i = 0
    
    while i < len(data):
        # For normal IMAP responses, the pattern is usually:
        # 1. A bytes object with message metadata (including UID)
        # 2. A tuple containing the message data
        uid = None
        header_data = None
        
        # Try to extract UID and header data
        if i < len(data) and isinstance(data[i], bytes):
            # Try to extract UID from this line
            uid = extract_uid(data[i]) # extract_uid now uses local debug_print
            i += 1
        
        # Try to get header data from the next element
        if i < len(data):
            if isinstance(data[i], tuple) and len(data[i]) > 1:
                # This is likely the header data tuple
                header_data = data[i][1]  # Second element is typically the data
            elif isinstance(data[i], bytes):
                # Sometimes header data might be directly in bytes
                header_data = data[i]
            
            # If we found header data, process it only if we have a UID
            if header_data and uid:
                messages.append((uid, header_data))
            
            i += 1
        else:
            i += 1
    
    debug_print(f"Extracted {len(messages)} message pairs")
    return messages 

import asyncio
import functools
import time
import random # Added import for random
import imaplib # Added import for imaplib

def async_retry(attempts=3, delay_seconds=1, backoff_factor=2, jitter_range=(0, 1)):
    """
    A decorator for retrying an async function if it raises an exception.

    Args:
        attempts: The maximum number of attempts.
        delay_seconds: The initial delay between retries in seconds.
        backoff_factor: The factor by which the delay increases after each retry.
        jitter_range: A tuple (min_jitter, max_jitter) to add random jitter to the delay.
                      Helps prevent thundering herd problem.
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay_seconds
            for attempt in range(attempts):
                try:
                    return await func(*args, **kwargs)
                except (asyncio.TimeoutError, ConnectionError, TimeoutError, imaplib.IMAP4.error) as e: # Common transient errors
                    # Could be more specific, e.g., imaplib.IMAP4.error for IMAP
                    last_exception = e
                    if attempt < attempts - 1:
                        actual_delay = current_delay
                        if jitter_range and jitter_range[0] < jitter_range[1]:
                            # Corrected jitter calculation
                            jitter_value = random.uniform(jitter_range[0], jitter_range[1]) 
                            actual_delay += jitter_value
                        
                        debug_print(f"Retry {attempt + 1}/{attempts} for {func.__name__} after error: {e}. Retrying in {actual_delay:.2f}s...")
                        await asyncio.sleep(actual_delay)
                        current_delay *= backoff_factor
                    else:
                        debug_print(f"Function {func.__name__} failed after {attempts} attempts.")
            if last_exception:
                raise last_exception
        return wrapper
    return decorator 