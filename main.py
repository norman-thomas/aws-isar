import urllib.request
from bs4 import BeautifulSoup as Soup

import datetime

LEVEL_URL = 'http://www.hnd.bayern.de/pegel/isar/muenchen-16005701/tabelle?setdiskr=15'
FLOW_URL = 'http://www.hnd.bayern.de/pegel/isar/muenchen-16005701/tabelle?methode=abfluss&setdiskr=15'
TEMPERATURE_URL = 'http://www.gkd.bayern.de/fluesse/wassertemperatur/stationen/diagramm/index.php?tab=true&thema=gkd&rubrik=fluesse&produkt=wassertemperatur&msnr=783500823&gknr=4&beginn=25.07.2017&ende=01.08.2017&wertart=ezw'

def lambda_handler(event, context):
    info = fetch_info()
    return info

def fetch_info():
    level = load_page(LEVEL_URL)
    flow = load_page(FLOW_URL)
    temperature = load_page(TEMPERATURE_URL)

    return {
        'level': level,
        'flow': flow,
        'temperature': temperature
    }

def load_page(url):
    content = urllib.request.urlopen(url).read()
    soup = Soup(content)

    datetime_str = soup.select('tbody tr:nth-of-type(1) td:nth-of-type(1)')[0].text
    value = soup.select('tbody tr:nth-of-type(1) td:nth-of-type(2)')[0].text

    dt = datetime.datetime.strptime(datetime_str, '%d.%m.%Y %H:%M')
    datetime_str = dt.isoformat()

    value = value.replace(',', '.').replace('\xa0', '')
    value = float(value)

    return {
        'datetime': datetime_str,
        'value': value
    }

if __name__ == '__main__':
    print(fetch_info())
