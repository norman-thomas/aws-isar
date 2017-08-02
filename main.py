import urllib.request
from bs4 import BeautifulSoup as Soup

import datetime

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

def lambda_handler(event, context):
    info = fetch_info()
    return info

def fetch_info():
    level = load_page(LEVEL_URL, LEVEL_SELECTORS)
    flow = load_page(FLOW_URL, FLOW_SELECTORS)
    temperature = load_page(TEMPERATURE_URL, TEMPERATURE_SELECTORS)

    return {
        'level': level,
        'flow': flow,
        'temperature': temperature
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

if __name__ == '__main__':
    print(fetch_info())
