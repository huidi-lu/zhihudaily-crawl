import requests
from calendar import Calendar
import sqlite3
import sys, time, random
from threading import Thread
from queue import Queue

DB = 'zhihudaily.db'
MAX_THREADS = 10
URL = 'http://news-at.zhihu.com/api/4/'
HEADERS = {'Host': 'news-at.zhihu.com',
           'Accept': '*/*',
           'X-Device': 'iPhone6,2/N53AP',
           'X-OS': 'iOS 10.1.1',
           'Accept-Encoding': 'gzip, deflate',
           'Accept-Language': 'en-us',
           'X-Api-Version': '4',
           'X-Bundle-ID': 'com.zhihu.daily',
           'User-Agent': 'daily/201607251035 CFNetwork/808.1.4 Darwin/16.1.0',
           'Connection': 'keep-alive',
           'X-App-Version': '2.6.7'
           }
START = time.time()


def get_news_list(dates, sids):
    s = requests.session()
    s.headers = HEADERS

    with sqlite3.connect(DB, timeout=5000) as sub_conn:
        while dates.qsize() > 0:
            date, trial_count = dates.get()
            try:
                r = s.get(URL+'news/before/'+date)
                if len(r.json()) == 0 or r.status_code == 404:
                    sub_conn.execute('INSERT INTO `unavail_dates` VALUES (?,?);', (date, '404'))
                elif r.status_code == 200:
                    stories = []
                    for item in r.json()['stories']:
                        if 'images' in item:
                            item['images'] = '|'.join(item['images'])
                        else:
                            item['images'] = None
                        stories.append((int(date)-1, item['title'], item['images'], item['type'], item['id']))
                        sids.put((item['id'], 0))
                    sub_conn.executemany('INSERT OR REPLACE INTO `date_stories` VALUES (?,?,?,?,?);', stories)
                else:
                    raise Exception
            except requests.ConnectionError:
                print('date', date, 'Connection Error')
                dates.put((date, trial_count))
                time.sleep(random.random() * 360)
            except:
                print('date', date, sys.exc_info())
                if trial_count < 2:
                    dates.put((date, trial_count+1))
                else:
                    sub_conn.execute('INSERT INTO `unavail_dates` VALUES (?,?);', (date, str(sys.exc_info())))
            finally:
                sub_conn.commit()
                time.sleep(random.random()*10)


def get_detail(dates, sids):
    s = requests.session()
    s.headers = HEADERS
    L = dates.qsize() + 1
    v = 0
    interval = 2.0
    with sqlite3.connect(DB, timeout=5000) as sub_conn:
        while True:
        	
            # progress estimate
            u = time.time() - START
            Ld = dates.qsize()
            Ls = sids.qsize()
            if Ld > 0:
                perc_dates = 1.0 - Ld / L
                perc_stories = 1.0 - Ld / L - Ls / (20.0 * L)
                est = (Ls + Ld * 20.0) * u / (MAX_THREADS * (L - Ld))
            else:
                interval = 0.5 * (u - v + interval) if u - v < 360 else interval
                v = time.time() - START
                perc_dates = 1.0
                perc_stories = 1.0 - Ls / (20.0 * L)
                est = Ls * interval / MAX_THREADS
                if Ls == 0:
                    break
            print('Dates: {:.2%}, Stories: {:.2%}, {}m{}s elapsed, estimated to finish in {}m{}s.     '
                  .format(perc_dates, perc_stories, int(u/60), int(u%60), int(est/60), int(est%60)), end='\r')
                  
            sid, trial_count = sids.get()
            try:
                # get all contents
                content = s.get(URL+'news/{}'.format(sid)).json()
                extra = s.get(URL+'story-extra/{}'.format(sid)).json()
                time.sleep(random.random()*6)

                comments = []
                if extra['long_comments'] > 0:
                    long_comments = s.get(URL+'story/{}/long-comments'.format(sid)).json()['comments']
                    for lc in long_comments:
                        lc['is_long'] = True
                        comments.append(lc)
                    time.sleep(random.random()*2)
                if extra['short_comments'] > 0:
                    short_comments = s.get(URL+'story/{}/short-comments'.format(sid)).json()['comments']
                    for sc in short_comments:
                        sc['is_long'] = False
                        comments.append(sc)
                    time.sleep(random.random()*2)

                # process the data and write into database
                ## content
                if 'recommenders' in content:
                    content['recommenders'] = '|'.join((i['avatar'] for i in content['recommenders']))
                else:
                    content['recommenders'] = None

                if not 'editor_name' in content:
                    content['editor_name'] = None

                for st in ['section', 'theme']:
                    for key in ['id', 'name', 'thumbnail']:
                        if st in content and key in content[st]:
                            content['{}_{}'.format(st, key)] = content[st][key]
                        else:
                            content['{}_{}'.format(st, key)] = None

                if content['type'] == 1:
                    try:
                        r = s.get(content['share_url']).text
                    except:
                        r = None
                    sub_conn.execute('INSERT OR REPLACE INTO `story_content` VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?);',
                                    (sid, r, None, None, content['share_url'], content['recommenders'],
                                     content['section_id'], content['section_name'], content['section_thumbnail'],
                                     content['theme_id'], content['theme_name'], content['theme_thumbnail'],
                                     content['editor_name'], content['type']))
                else:
                    if 'image' not in content:
                        content['image'] = None
                    if 'image_source' not in content:
                        content['image_source'] = None
                    sub_conn.execute('INSERT OR REPLACE INTO `story_content` VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?);',
                                    (sid, content['body'], content['image'], content['image_source'],
                                     content['share_url'], content['recommenders'],
                                     content['section_id'], content['section_name'], content['section_thumbnail'],
                                     content['theme_id'], content['theme_name'], content['theme_thumbnail'],
                                     content['editor_name'], content['type']))

                ## stats
                sub_conn.execute('INSERT OR REPLACE INTO `story_extra` VALUES (?,?,?,?,?);',
                                (sid, extra['long_comments'], extra['popularity'],
                                 extra['short_comments'], extra['comments']))

                ## comments
                if comments:
                    for c in comments:
                        for key in ['author', 'content', 'id', 'status']:
                            if 'reply_to' in c and key in c['reply_to']:
                                c['reply_to_{}'.format(key)] = c['reply_to'][key]
                            else:
                                c['reply_to_{}'.format(key)] = None
                    sub_conn.executemany('INSERT OR REPLACE INTO `comments` VALUES (?,?,?,?,?,?,?,?,?,?,?,?);',
                                        [(sid, c['author'], c['id'], c['content'], c['likes'], c['time'],
                                          c['avatar'], c['reply_to_id'], c['reply_to_author'],
                                          c['reply_to_content'], c['reply_to_status'], c['is_long'])
                                         for c in comments])

            except requests.ConnectionError:
                print('sid', sid, 'Connection Error')
                sids.put((sid, trial_count))
                time.sleep(random.random() * 360)
            except:
                print('sid', sid, sys.exc_info())
                if trial_count < 2:
                    sids.put((sid, trial_count + 1))
                else:
                    sub_conn.execute('INSERT INTO `unavail_sids` VALUES (?,?)', (sid, str(sys.exc_info())))
            finally:
                sub_conn.commit()

if __name__ == '__main__':

    CLD = Calendar()
    DATES = [(date.strftime('%Y%m%d'), 0)
             for y in range(2013, 2017) for m in range(1, 13)
             for weeks in CLD.monthdatescalendar(y, m)
             for date in weeks][161:-45]
    random.shuffle(DATES)

    ALL_DATES = Queue()
    ALL_SIDS = Queue()
    for i in DATES:
        ALL_DATES.put(i)

    # Initiate database
    with open(DB, 'w+'):
        with sqlite3.connect(DB) as conn:
            conn.execute('CREATE TABLE IF NOT EXISTS `date_stories`('
                         '`pub_date` TEXT NOT NULL,'
                         '`title` TEXT NOT NULL,'
                         '`images` TEXT,'
                         '`type` INTEGER,'
                         '`sid` INTEGER,'
                         'PRIMARY KEY(`before_date`, `sid`));')
            conn.execute('CREATE TABLE IF NOT EXISTS `story_content`('
                         '`sid` INTEGER NOT NULL,'
                         '`body` TEXT,'
                         '`image` TEXT,'
                         '`image_source` TEXT,'
                         '`share_url` TEXT,'
                         '`recommenders` TEXT,'
                         '`section_id` INTEGER,'
                         '`section_name` TEXT,'
                         '`section_thumbnail` TEXT,'
                         '`theme_name` TEXT,'
                         '`theme_id` INTEGER,'
                         '`theme_thumbnail` TEXT,'
                         '`editor_name` TEXT,'
                         '`type` INTEGER,'
                         'PRIMARY KEY(`sid`));')
            conn.execute('CREATE TABLE IF NOT EXISTS `story_extra`('
                         '`sid` INTEGER NOT NULL,'
                         '`long_comments` INTEGER,'
                         '`popularity` INTEGER,'
                         '`short_comments` INTEGER,'
                         '`comments` INTEGER,'
                         'PRIMARY KEY(`sid`));')
            conn.execute('CREATE TABLE IF NOT EXISTS `comments`('
                         '`sid` INTEGER NOT NULL,'
                         '`author` TEXT,'
                         '`aid` INTEGER,'
                         '`content` TEXT,'
                         '`likes` INTEGER,'
                         '`time` INTEGER,'
                         '`avatar` TEXT,'
                         '`reply_to_aid` INTEGER,'
                         '`reply_to_author` TEXT,'
                         '`reply_to_content` TEXT,'
                         '`reply_to_status` INTEGER,'
                         '`is_long` INTEGER,'
                         'PRIMARY KEY(`sid`, `aid`, `time`));')
            conn.execute('CREATE TABLE IF NOT EXISTS `unavail_dates`('
                         '`date` TEXT NOT NULL,'
                         '`cause` TEXT,'
                         'PRIMARY KEY(`date`));')
            conn.execute('CREATE TABLE IF NOT EXISTS `unavail_sids`('
                         '`sid` TEXT NOT NULL,'
                         '`cause` TEXT,'
                         'PRIMARY KEY(`sid`));')
            conn.commit()

    th_list = Thread(target=get_news_list, args=(ALL_DATES, ALL_SIDS))
    th_list.start()

    th_details = [Thread(target=get_detail, args=(ALL_DATES, ALL_SIDS)) for _ in range(MAX_THREADS)]
    [th.start() for th in th_details]
    [th.join() for th in th_details]

    # report
    with sqlite3.connect(DB) as conn:
        d_count = len(conn.execute('SELECT * FROM unavail_dates;').fetchall())
        s_count = len(conn.execute('SELECT * FROM unavail_sids;').fetchall())
        if d_count != 0 or s_count != 0:
            print('\n{} dates, {} articles failed, check manually!'
                  .format(d_count, s_count))
        else:
            print('\nAll done!')