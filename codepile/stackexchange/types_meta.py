import pyarrow as pa
from datetime import datetime

post_types = (
    ('Id', int, pa.uint32),
    ('PostTypeId', int, pa.uint8),
    ('AcceptedAnswerId', int, pa.uint32),
    ('OwnerUserId', int, pa.int32),
    ('OwnerDisplayName', str, pa.string),
    ('LastEditorUserId', int, pa.int32),
    ('ParentId', int, pa.uint32),
    ('Score', int, pa.int32), # can be negative
    ('Title', str, pa.large_string),
    ('Body', str, pa.large_string),
    ('Tags', str, pa.string),
    ('ContentLicense', str, pa.string),
    ('AnswerCount', int, pa.uint32),
    ('CommentCount', int, pa.uint32),
    ('ViewCount', int, pa.uint32),
    ('FavoriteCount', int, pa.uint32),
    ('CreationDate', datetime.fromisoformat, pa.date64),
    ('LastEditDate', datetime.fromisoformat, pa.date64),
    ('LastActivityDate', datetime.fromisoformat, pa.date64),
    ('ClosedDate', datetime.fromisoformat, pa.date64),
    ('CommunityOwnedDate', datetime.fromisoformat, pa.date64)
    )

user_types = (
    ('Id', int, pa.int32),
    ('Reputation', int, pa.int32),
    ('DisplayName', str, pa.string),
    ('AboutMe', str, pa.large_string),
    ('Location', str, pa.string),
    ('Views', int, pa.uint32),
    ('UpVotes', int, pa.uint32),
    ('DownVotes', int, pa.uint32),
    ('AccountId', int, pa.int32),
    ('ProfileImageUrl', str, pa.large_string),
    ('WebsiteUrl', str, pa.string),
    ('CreationDate', datetime.fromisoformat, pa.date64),
    ('LastAccessDate', datetime.fromisoformat, pa.date64),
    )

comment_types = (
    ('Id', int, pa.int32),
    ('PostId', int, pa.int32),
    ('Score', int, pa.int32),
    ('Text', str, pa.large_string),
    ('UserId', int, pa.int32), # can be negative
    ('ContentLicense', str, pa.string),
    ('CreationDate', datetime.fromisoformat, pa.date64),
    )
