import urllib.request
from bs4 import BeautifulSoup as Soup
import pymysql
import datetime
import json
import logging

from config import *

LEVEL_URL = 'http://www.hnd.bayern.de/pegel/isar/muenchen-16005701/tabelle?setdiskr=15'
FLOW_URL = 'http://www.hnd.bayern.de/pegel/isar/muenchen-16005701/tabelle?methode=abfluss&setdiskr=15'
TEMPERATURE_URL = 'http://www.gkd.bayern.de/fluesse/wassertemperatur/stationen/diagramm/index.php?thema=gkd&rubrik=fluesse&produkt=wassertemperatur&gknr=4&msnr=783500823&zr=woche'

LEVEL_SELECTORS = (
    'tbody tr:nth-of-type(1) td:nth-of-type(1)',
    'tbody tr:nth-of-type(1) td:nth-of-type(2)'
)
FLOW_SELECTORS = (
    'tbody tr:nth-of-type(1) td:nth-of-type(1)',
    'tbody tr:nth-of-type(1) td:nth-of-type(2)'
)
TEMPERATURE_SELECTORS = (
    'table#pegel_tabelle tbody tr:nth-of-type(1) td:nth-of-type(1)',
    'table#pegel_tabelle tbody tr:nth-of-type(1) td:nth-of-type(2)'
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def connect_db():
    try:
        return pymysql.connect(MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PASS, db=MYSQL_DB, connect_timeout=5)
    except:
        logger.exception()
    return None

def store_db(db, values):
    with db.cursor() as cur:
        print('Storing:', json.dumps(values))
        try:
            cur.execute('INSERT INTO isar_pegel (time, level, flow, temperature) VALUES (%s, %s, %s, %s)', (values['time'], values['level'], values['flow'], values['temperature']))
            db.commit()
        except:
            print('Failed to store data.')

def read_db(db):
    with db.cursor() as cur:
        cur.execute('SELECT time, level, flow, temperature FROM isar_pegel ORDER BY time DESC LIMIT 1')
        result = cur.fetchone()
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
    value = float(value)

    return {
        'datetime': datetime_str,
        'value': value
    }

def lambda_handler(event, context):
    db = connect_db()
    if db is None:
        return None

    if 'trigger' in event and event.trigger == 'cron':
        info = fetch_info()
        store_db(db, info)
    info = read_db(db)
    db.close()
    return info

if __name__ == '__main__':
    db = connect_db()
    info = fetch_info()
    print(info)
    #store_db(db, info)
    print(read_db(db))
    db.close()

