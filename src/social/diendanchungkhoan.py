from dateutil.parser import parse
import feedparser
from cachetools import TTLCache

# URL = 'http://diendanchungkhoan.vn/external.php?type=RSS2'
URL = 'http://diendanchungkhoan.vn/external.php?type=RSS2&forumids=6'

# URL = 'https://f247.com/posts.rss'

f247 = TTLCache(maxsize=100, ttl=1200)


def main():
    print('F247')
    NewsFeed = feedparser.parse(URL)
    # print('LENGTH {}'.format(len(NewsFeed.entries)))
    # print(NewsFeed.entries)
    for entry in NewsFeed.entries:
        print('//---------------------XXX-----------------------//')

        print(entry.keys())
        print(entry.published)
        print(parse(entry.published))
        # print(parse(entry.published).timestamp())
        print(entry.title)
        print(entry.summary)
        print(entry.content)
        print('ID#', entry.id)
        # print(entry.link)
        # print(entry.guidislink)


if __name__ == '__main__':
    main()
