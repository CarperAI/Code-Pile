"""
UseNet specific mailbox processing
"""


from .classes import Thread, Message
from .utils import process_raw_message, discard_message, get_author_username

from datetime import datetime
import mailbox


def mbox_utf_reader(stream):
    # Custom utf reader, replaces utf-8 errors with the unicode error char "�"
    data = stream.read()
    text = data.decode(encoding='utf-8', errors='replace')
    return mailbox.mboxMessage(text)


def process_forum(mbox_file):
    """
    Processes a mbox_file and returns a list of classes.

    Some observations (for UseNet archives):

    Thread id (defined by X-Google-Thread) is not consistent, different threads might have the same id
    as well as the same thread might have multiple ids
    From in the first line (i.e. From <str>) is not consistent
    From: in the message body (i.e. From: Name (abc@xyz.com)) is consistent
    """
    # mbox = mailbox.mbox(mbox_file)
    mbox = mailbox.mbox(mbox_file, factory=mbox_utf_reader)
    threads = {}

    for message in mbox.itervalues():
        # We use the "subject" to determine threads and thread starts
        subject = str(message.get('Subject'))
        if not subject or '�' in subject:
            continue

        if len(subject) < 10:
            continue

        # Clean the message and check if it meets our minimum thresholds
        body = message.get_payload()
        if not isinstance(body, str):
            # NB: get_payload only works for text/plain content types
            # Use walk and call get_payload for individual parts in other cases
            continue
        if not body:
            continue

        body = process_raw_message(body)
        if discard_message(body):
            continue

        # Get the datetime
        datetime_str = message.get('Date')
        if not datetime_str:
            continue

        try:
            # Try to parse the datetime - we might need to use date utils to determine the format for other locales
            m_datetime = datetime.strptime(datetime_str, '%a, %d %b %Y %H:%M:%S %z')
        except ValueError:
            continue

        # Clean up the subject line
        if subject.lower().startswith('re: '):
            is_post = False
            subject = subject[4:]
        else:
            is_post = True

        if subject not in threads:
            # Create a new thread for this topic
            thread = Thread(title=subject, )
            threads.update({
                subject: thread,
            })

        # Get the author
        message_from = message.get('From')
        if message_from:
            username = get_author_username(message_from)
        else:
            username = ''

        # Add the post/reply to the thread
        message = Message(body=body, timestamp=m_datetime, author=username)
        if is_post and not threads[subject].has_post():
            threads[subject].add_post(message)
        else:
            threads[subject].add_reply(message)

    # Removing threads without a post or at least 1 reply
    final_threads = []
    for m_subject, m_thread in threads.items():
        if m_thread.has_post() and m_thread.has_replies():
            final_threads.append(m_thread)

    return final_threads
