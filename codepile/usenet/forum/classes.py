"""
Classes to represent forums and threads
"""


import hashlib
from operator import attrgetter

from xml.etree import ElementTree as ET
from datetime import datetime


class Thread:

    def __init__(self, title):
        # The title of the thread (for ex: the subject)
        self.title = title
        # The original email/message/post the started the thread
        self.post = None
        # Replies to the post in timestamp order
        self.replies = []
        self.reply_hashes = set()

    def add_reply(self, message):
        # Skipping duplicate message bodies (after cleaning)
        if message.body_hash not in self.reply_hashes:
            # Also making sure the reply does not match the post
            if self.post and not message.body_hash == self.post.body_hash:
                self.replies.append(message)
                self.reply_hashes.add(message.body_hash)

    def get_replies(self):
        # Replies are sorted by timestamp at the time of access
        self.replies.sort(key=attrgetter('timestamp'))
        return self.replies

    def add_post(self, message):
        if not self.post:
            # Make sure the post does not match any replies
            if message.body_hash not in self.reply_hashes:
                self.post = message

    def has_post(self):
        return self.post is not None

    def has_replies(self):
        return len(self.replies) > 0

    def get_id(self):
        return self.post.body_hash

    def export_xml(self):
        # Export this thread in xml, returns an ElementTree instance
        # https://docs.google.com/document/d/18bIWrF6b8eltKTw0Spn8A3geyuwh69jzCK9yjmBIucc/edit
        thread = ET.Element('thread')
        post = ET.SubElement(thread, 'post', date=datetime.strftime(self.post.timestamp, '%Y-%m-%d'))
        title = ET.SubElement(post, 'title')
        title.text = self.title
        body = ET.SubElement(post, 'body')
        body.text = self.post.body
        for reply in self.get_replies():
            m_reply = ET.SubElement(thread, 'reply', date=datetime.strftime(reply.timestamp, '%Y-%m-%d'))
            m_reply.text = reply.body

        return thread

    def get_metadata(self):
        # Returns a metadata dict
        return {
            'reply_count': len(self.replies),
        }


class Message:

    def __init__(self, timestamp, body):
        self.timestamp = timestamp
        self.body = body
        self.body_hash = hashlib.md5(body.encode()).hexdigest()

    def __eq__(self, other):
        return self.body_hash == other.body_hash
