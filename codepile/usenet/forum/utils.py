"""
A mix of UseNet specific and generally useful pre-processing and cleanup scripts
"""


import re


def discard_message(message, min_length=100, max_length=10000):
    """
    Returns True if we need to discard a message outside the min and max length params
    """
    message_length = len(message)
    return message_length < min_length or message_length > max_length


def process_raw_message(body, remove_empty_lines=True, remove_author_lines=True):
    """
    Steps we take to process a raw message body

    Some steps described here - http://www.psych.ualberta.ca/~westburylab/downloads/usenetcorpus.download.html

    1. Replace email addresses with the token <email_address>
    2. Replace HTTP urls with the token <url>
    3. Remove personal email signatures
    4. Replace quoted lines with <quote> (see the quote function)
    5. Remove excessive empty lines
    6. Remove lines "On <date>, <person> wrote:" at the top of messages and quoted text

    A lot of rules have been manually added based on mailboxes I have seen, edge cases I have not seen
    might not be included.

    :param body: Raw text message body
    :param remove_empty_lines: Toggles step 5
    :param remove_author_lines: Toggles step 6
    :return: Processed message text
    """

    # Using a simplified email regex that is also fairly performant
    body = re.sub(r'[\w.+-]+@[\w-]+\.[\w.-]+', '<email_address>', body)

    body = re.sub(r'(http:|https:|www\.)\S*', '<url>', body)

    # Personal signatures are separated from the body of the email by --
    body_parts = body.split('--')

    if len(body_parts) > 1:
        body = ''.join(body_parts)[:-1]
    else:
        body = body_parts[0]

    # Removing quotes and empty lines
    body = quote(body, preserve_quoted_text=False)

    # Removing empty lines and lines "On .... wrote:"
    if remove_empty_lines or remove_author_lines:
        lines = []
        for line in body.splitlines():
            line_l = line.lower()
            if remove_author_lines:
                if line_l.endswith(' wrote:') or line_l.endswith(' said:') or line_l.endswith(' said :'):
                    continue
                if 'in article ' in line_l:
                    continue

            if remove_empty_lines:
                if line_l.strip() == '':
                    continue

            lines.append(line)

        body = '\n'.join(lines)

    return body


def quote(body, preserve_quoted_text=False):
    """
    Parses the message body and detects blocks of quotes and replaces them with a <quote> tag
    Does not add additional quote tags for nested blocks

    If preserve_quoted_text is set to True, surrounds quoted text with <quote> tags
    For nested tags, the function needs to be called recursively
    """
    blocks = []
    was_quoted = False

    for line in body.splitlines():
        line_quoted = line.startswith('>')
        if line_quoted:
            line = line[1:].lstrip()

        if line_quoted and not was_quoted:
            # We're starting a new quote block
            blocks.append('<quote>')
            was_quoted = True

        if line_quoted and preserve_quoted_text:
            blocks.append(line)
        elif not line_quoted:
            blocks.append(line)

        if not line_quoted and was_quoted and preserve_quoted_text:
            # We need to end the quote block
            blocks.append('</quote>')
            was_quoted = False

    return '\n'.join(blocks)


def get_author_username(author_string):
    """
    Parses a raw mail archive style author string to get an author name
    There can be multiple different formats within a single mailbox for authors ('From:')
    This function tries its best to find the most obvious candidate for a username
    """
    email_part = author_string.split('<')[-1]
    # Get the username from the first part of the email
    username = email_part.split('@')[0]
    # Also try to split on characters that may be used to substitute @
    username = username.split('AT')[0]

    return re.sub(r'\W+', '', username)
