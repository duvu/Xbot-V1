from dateutil.parser import parse
import feedparser
from cachetools import TTLCache

# NewsFeed = feedparser.parse('http://f319.com/forums/-/index.rss')

f319x = TTLCache(maxsize=100, ttl=1200)
# Test
if __name__ == '__main__':
    NewsFeed = feedparser.parse('http://f319.com/forums/-/index.rss')
    print('Number of RSS posts: ', len(NewsFeed.entries))
    for entry in NewsFeed.entries:
        # print('------------------------XXX---------------------------')
        # if hasattr(entry, 'slash_comments'):
        #     key_x = entry.id + entry.slash_comments
        #     print('KEY_X', key_x)
        #     f319x[key_x] = 'processed'
        #
        #     print(f319x[key_x])
        # else:
        #     key_x = entry.id
        #     print('KEY_X', key_x)
        #     f319x[key_x] = 'processed'
        #
        #     print(f319x[key_x])
        # print(entry.keys())
        print(entry.published)
        print(parse(entry.published))
        print(parse(entry.published).timestamp())

        # print(entry.title)
        # print(entry.author)
        # print(entry.authors)
        # print(entry.author_detail)
        # print(entry.summary)
        # print(entry.content)
        print('ID: ', entry.id)
        if hasattr(entry, 'slash_comments'):
            print('slash_comments: ', entry.slash_comments)

        # print('------------------------------------------------------')
