import sqlite3
from spider import get_detail, get_news_list, DB, MAX_THREADS
from queue import Queue
from threading import Thread
import datetime


date_q = Queue()
sid_q = Queue()
with sqlite3.connect(DB) as conn:

    TODAY = datetime.date.today().strftime('%Y%m%d')
    (LAST_DAY, _) = conn.execute('SELECT max(pub_date) FROM date_stories;').fetchone()
    for i in range(int(LAST_DAY), int(TODAY)+1):
        date_q.put((str(i), 0))

    for date, in conn.execute('SELECT date FROM unavail_dates;'):
        date_q.put((date, 0))
    conn.execute('DELETE FROM unavail_dates;')

    for sid, in conn.execute('SELECT sid FROM unavail_sids;'):
        sid_q.put((sid, 0))
    conn.executescript('DELETE FROM unavail_sids; VACUUM;')
    conn.commit()

    th_list = Thread(target=get_news_list, args=(date_q, sid_q))
    th_list.start()
    th_details = [Thread(target=get_detail, args=(date_q, sid_q)) for _ in range(MAX_THREADS)]
    [th.start() for th in th_details]
    [th.join() for th in th_details]

    d_count = len(conn.execute('SELECT * FROM unavail_dates;').fetchall())
    s_count = len(conn.execute('SELECT * FROM unavail_sids;').fetchall())
    if d_count != 0 or s_count != 0:
        print('\n{} dates, {} articles failed, check manually!'
              .format(d_count, s_count))
    else:
        print('\nAll done!')