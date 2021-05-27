import re

import feedparser
from dateutil.parser import parse

from db.database import get_connection, close_connection


def insert_mentioned_code(matches, mentioned_at, mentioned_by_id, mentioned_by, mentioned_in, message):
    if len(matches) > 0:
        conn, cursor = get_connection()
        sql_string = '''insert into tbl_mentions(symbol, mentioned_at, mentioned_by_id, mentioned_by, mentioned_in, message) VALUES (%s, %s, %s, %s, %s, %s)'''
        for x in matches:
            try:
                cursor.execute(sql_string, (x, mentioned_at, mentioned_by_id, mentioned_by, mentioned_in, message))
                conn.commit()
            except Exception as ex:
                print('[ERROR] Something went wrong %s' % ex)
        close_connection(conn)


def get_key_x(entry):
    if hasattr(entry, 'slash_comments'):
        return entry.id + entry.slash_comments
    else:
        return entry.id


async def social_counting(company_list_all, bl_words, f319x=None):
    f319FeedEntries = feedparser.parse('http://f319.com/forums/-/index.rss').entries
    f247FeedEntries = feedparser.parse('https://f247.com/posts.rss').entries
    feeds = [*f319FeedEntries, *f247FeedEntries]
    for entry in feeds:
        # Check if entry processed already
        key_x = get_key_x(entry)
        if f319x is not None and f319x.get(key_x) == 'processed':
            # print('Processed!')
            continue

        f319x.set(key_x, 'processed')
        f319_msg = entry.title + entry.summary
        for x in bl_words:
            f319_msg = f319_msg.replace(x, '')

        published = parse(entry.published)
        msg_a = re.split(r'[`\-=~!@#$%^&*()_+\[\]{};\'\\:"|<,./<>? ]', f319_msg)
        # Process message and insert to db for STAT here
        matches = [x for x in company_list_all if x in msg_a]
        if len(matches) > 0:
            # Insert into database
            print('### MATCH %s' % matches)
            insert_mentioned_code(matches, published, entry.author, entry.author, 'f319', entry.title)
        else:
            print('### NO MATCHES %s' % entry.title)