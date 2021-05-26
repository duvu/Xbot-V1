# https://f247.com/c/thi-truong-chung-khoan/5.rss
# https://f247.com/latest.rss --> Chủ đề mới nhất
# https://f247.com/posts.rss --> bài viết mới nhất

import feedparser
from cachetools import TTLCache


URL = 'https://f247.com/posts.rss'

f247 = TTLCache(maxsize=100, ttl=1200)


def main():
    print('F247')
    NewsFeed = feedparser.parse(URL)
    # print('LENGTH {}'.format(len(NewsFeed.entries)))
    # print(NewsFeed.entries)
    for entry in NewsFeed.entries:
        print('//---------------------XXX-----------------------//')
        print(entry.keys())
    #     print(entry.published)
    #     print(entry.title)
    #     print(entry.summary)
        print(entry.id)
    #     print(entry.link)



if __name__ == '__main__':
    main()
