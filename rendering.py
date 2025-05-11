import html2text
import email
from email.message import Message
from typing import Optional, List, Dict, Tuple
import hashlib # Added for SHA256 calculation

# Placeholder for more sophisticated email cleaning libraries if needed in the future
# from bs4 import BeautifulSoup # Example for more advanced HTML parsing
# import re # For regex-based cleaning

def _decode_payload(part: Message) -> Optional[str]:
    """Decodes the payload of an email part."""
    payload = part.get_payload(decode=True)
    charset = part.get_content_charset() or 'utf-8'  # Default to utf-8
    try:
        return payload.decode(charset, errors='replace')
    except (LookupError, UnicodeDecodeError):
        # Fallback for unknown or bad charsets
        return payload.decode('latin-1', errors='replace')


def _get_best_body_part(msg: Message) -> Tuple[Optional[str], Optional[str]]:
    """
    Extracts the best available body part from an email message.
    Returns a tuple: (plain_text_content, html_content)
    """
    plain_text_content = None
    html_content = None

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disposition = str(part.get('Content-Disposition'))

            if part.is_multipart() or "attachment" in content_disposition:
                # For _get_best_body_part, we skip explicit attachments.
                # Attachments are handled by get_attachment_manifest.
                # However, if an HTML/plain part is marked "inline" but is not an attachment,
                # it could be part of the body. The key is not to skip parts that are
                # text/plain or text/html just because they are inline.
                # The original logic seems fine: if it's multipart, skip.
                # If it has "attachment" in Content-Disposition, skip.
                # Otherwise, consider based on content_type.
                if "attachment" in content_disposition and content_type not in ["text/plain", "text/html"]:
                    continue
                if part.is_multipart(): # Still skip deeper multipart boundaries
                    continue


            if content_type == "text/plain" and not plain_text_content:
                if "attachment" not in content_disposition: # Ensure it's not a .txt attachment
                    plain_text_content = _decode_payload(part)
            elif content_type == "text/html" and not html_content:
                if "attachment" not in content_disposition: # Ensure it's not an .html attachment
                    html_content = _decode_payload(part)
    else: # Not multipart, try to get the main content
        content_type = msg.get_content_type()
        if content_type == "text/plain":
            plain_text_content = _decode_payload(msg)
        elif content_type == "text/html":
            html_content = _decode_payload(msg)
        elif "attachment" not in str(msg.get('Content-Disposition')):
             payload = _decode_payload(msg)
             if payload:
                 try:
                     payload.encode('utf-8')
                     plain_text_content = payload
                 except UnicodeEncodeError:
                     pass

    return plain_text_content, html_content


async def render_email_to_markdown(raw_email_bytes: bytes) -> str:
    """
    Converts raw email content (bytes) to Markdown.
    Prioritizes HTML content if available, otherwise uses plain text.
    """
    try:
        msg = email.message_from_bytes(raw_email_bytes)
        plain_text, html_text = _get_best_body_part(msg)

        h = html2text.HTML2Text()
        # Configure html2text options if needed
        # h.ignore_links = False
        # h.ignore_images = False

        if html_text:
            markdown_content = h.handle(html_text)
            return markdown_content.strip()
        elif plain_text:
            # Plain text can be directly returned as it's often markdown-compatible
            # or at least doesn't need HTML-to-Markdown conversion.
            return plain_text.strip()
        else:
            # Fallback if no suitable text part is found
            # This could be an email with only attachments or an unusual structure
            return "*(No renderable text content found)*"

    except Exception as e:
        print(f"Error rendering email to markdown: {e}")
        # Fallback for severe parsing errors
        error_message = f"*(Error rendering email: {e}). First 1KB of raw content (approx):*\n"
        error_message += raw_email_bytes[:1024].decode('latin-1', errors='replace')
        return error_message


async def extract_clean_text(raw_email_bytes: bytes) -> str:
    """
    Extracts the primary textual content from raw email bytes.
    Prefers plain text. If only HTML is available, converts it to text, stripping formatting.
    """
    try:
        msg = email.message_from_bytes(raw_email_bytes)
        plain_text, html_text = _get_best_body_part(msg)

        if plain_text:
            # Further cleaning can be added here if needed (e.g., removing quoted replies)
            return plain_text.strip()
        elif html_text:
            h = html2text.HTML2Text()
            h.ignore_links = True
            h.ignore_images = True
            h.ignore_tables = False # Keep table structure if it's relevant
            h.ignore_emphasis = True
            h.body_width = 0 # Don't wrap lines
            cleaned_text = h.handle(html_text)
            return cleaned_text.strip()
        else:
            return "*(No extractable text content found)*"

    except Exception as e:
        print(f"Error extracting clean text: {e}")
        error_message = f"*(Error extracting text: {e}). First 1KB of raw content (approx):*\n"
        error_message += raw_email_bytes[:1024].decode('latin-1', errors='replace')
        return error_message


async def get_attachment_manifest(raw_email_bytes: bytes) -> List[Dict]:
    """
    Generates a structured list of attachments from raw email bytes.
    Parses the email to find attachments and extracts their metadata.

    Args:
        raw_email_bytes: The raw bytes of the email.

    Returns:
        A list of dictionaries, where each dictionary describes an attachment.
        Example: [{"filename": "report.pdf", "mime_type": "application/pdf", 
                   "size_bytes": 102400, "sha256": "...", "content_id": "cid_if_any"}]
    """
    manifest = []
    try:
        msg = email.message_from_bytes(raw_email_bytes)
        for part in msg.walk():
            content_disposition = str(part.get("Content-Disposition"))
            is_attachment = "attachment" in content_disposition
            is_inline_not_alternative = "inline" in content_disposition and "alternative" not in str(part.get("Content-Type"))

            if is_attachment or (is_inline_not_alternative and part.get_filename()):
                filename = part.get_filename()
                if not filename: 
                    # Try to generate a filename if missing, e.g. from content type
                    ext = part.get_content_maintype()
                    sub_ext = part.get_content_subtype()
                    if sub_ext:
                        ext = f"{ext}_{sub_ext}" # like image_jpeg
                    filename = f"attachment.{ext}"


                payload_bytes = part.get_payload(decode=True)
                mime_type = part.get_content_type()
                size_bytes = len(payload_bytes)
                
                sha256_hash = hashlib.sha256(payload_bytes).hexdigest()
                
                content_id_raw = part.get("Content-ID")
                content_id = None
                if content_id_raw:
                    content_id = content_id_raw.strip().lstrip('<').rstrip('>')

                manifest.append({
                    "filename": filename,
                    "mime_type": mime_type,
                    "size_bytes": size_bytes,
                    "sha256": sha256_hash,
                    "content_id": content_id
                })
        return manifest
    except Exception as e:
        print(f"Error generating attachment manifest: {e}")
        # Log raw bytes for debugging if error is severe
        # print(f"Problematic raw email bytes (first 1KB): {raw_email_bytes[:1024]}")
        return [{"error": f"Failed to parse attachments: {e}"}]

# Removed old placeholder code and comments for brevity
# ... 