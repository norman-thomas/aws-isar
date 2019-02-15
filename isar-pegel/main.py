import json
import datetime
import logging
import urllib.request
from contextlib import contextmanager

import pymysql
from bs4 import BeautifulSoup as Soup

from config import *

LEVEL_URL = 'http://www.hnd.bayern.de/pegel/isar/muenchen-16005701/tabelle?setdiskr=15'
FLOW_URL = 'http://www.hnd.bayern.de/pegel/isar/muenchen-16005701/tabelle?methode=abfluss&setdiskr=15'
TEMPERATURE_URL = 'https://www.gkd.bayern.de/de/fluesse/wassertemperatur/isar/muenchen-16005701/messwerte/tabelle'
PARTICLE_URL = 'https://www.gkd.bayern.de/de/fluesse/schwebstoff/kelheim/muenchen-16005701/messwerte/tabelle?zr=woche&parameter=konzentration'

LEVEL_SELECTORS = (
    'tbody tr:nth-of-type(1) td:nth-of-type(1)',
    'tbody tr:nth-of-type(1) td:nth-of-type(2)'
)
FLOW_SELECTORS = (
    'tbody tr:nth-of-type(1) td:nth-of-type(1)',
    'tbody tr:nth-of-type(1) td:nth-of-type(2)'
)
TEMPERATURE_SELECTORS = (
    'table.tblsort tbody tr:nth-of-type(1) td:nth-of-type(1)',
    'table.tblsort tbody tr:nth-of-type(1) td:nth-of-type(2)'
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

@contextmanager
def connect_db():
    conn = None
    try:
        conn = pymysql.connect(MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PASS, db=MYSQL_DB, connect_timeout=5)
        yield conn
    except:
        logger.exception()
    finally:
        if conn:
            conn.close()

def store_db(db, values):
    with db.cursor() as cur:
        logger.info('Storing:', json.dumps(values))
        try:
            cur.execute('INSERT INTO isar_pegel (time, level, flow, temperature) VALUES (%s, %s, %s, %s)', (values['time'], values['level'], values['flow'], values['temperature']))
            db.commit()
        except:
            logger.exception()

def read_db(db):
    with db.cursor() as cur:
        cur.execute('SELECT time, level, flow, temperature FROM isar_pegel ORDER BY time DESC LIMIT 1')
        result = cur.fetchone()
        result = (result[0].strftime('%Y-%m-%d %H:%M'), *result[1:])
        result = dict(zip(('time', 'level', 'flow', 'temperature'), result))
    return result

def fetch_info():
    level = load_page(LEVEL_URL, LEVEL_SELECTORS)
    flow = load_page(FLOW_URL, FLOW_SELECTORS)
    temperature = load_page(TEMPERATURE_URL, TEMPERATURE_SELECTORS)

    return {
        'time': level['datetime'],
        'level': level['value'],
        'flow': flow['value'],
        'temperature': temperature['value']
    }

def load_page(url, selectors):
    content = urllib.request.urlopen(url).read()
    soup = Soup(content)

    datetime_str = soup.select(selectors[0])[0].text
    value = soup.select(selectors[1])[0].text

    dt = datetime.datetime.strptime(datetime_str, '%d.%m.%Y %H:%M')
    datetime_str = dt.isoformat()

    value = value.replace(',', '.').replace('\xa0', '')
    try:
        value = float(value)
    except ValueError:
        value = None

    return {
        'datetime': datetime_str,
        'value': value
    }

def lambda_handler(event, context):
    print(event)
    if isinstance(event, dict) and 'trigger' in event and event['trigger'] == 'cron':
        info = fetch_info()
        with connect_db() as db:
            store_db(db, info)
            return info

if __name__ == '__main__':
    info = fetch_info()
    print(info)
    with connect_db() as db:
        store_db(db, info)
        print(read_db(db))
