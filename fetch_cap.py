"""
Fetch CAP

simple stupid script to fecth cities' CAP data from
http://lab.comuni-italiani.it/files/listacomuni.zip

Usage:
    run
    $ python fetch_cap.py

Author: Francesco Pischedda
email: francesco.pischedda@gmail.com
github: fpischedda https://github.com/fpischedda

LICENSE: GPLv3 Public License
If a LICENSE file is not included with this script please refer to
https://www.gnu.org/licenses/gpl-3.0.en.html
"""
import csv
import zipfile
import io
import requests
from bs4 import BeautifulSoup


def fetch_cap_range(url):
    """return the CAP range from the specified url"""
    data = BeautifulSoup(requests.get(url).content, 'html.parser')
    label_col = data.find_all('td', text='CAP')[0]
    cap_col = label_col.next_sibling
    return cap_col.contents[0].contents[0]


def city_cap(row):
    """extracts city and cap from a csv row; if a cap contains the letter x it
    means that there are multiple CAP for a city, in this case fetch the range
    from the url specified by column 8 of the row
    returns a tuple with the city name and its CAP or CAP range
    """
    if 'x' in row[5]:
        url = row[8]
        cap = fetch_cap_range(url)
    else:
        cap = row[5]

    return (row[1], cap)


def read_csv(stream):
    """returns a list of cities and their CAP fetching data from a file
    stream"""
    reader = csv.reader(stream, delimiter=';')
    return [city_cap(row) for row in reader]


def fetch_city_cap_zip(url):
    """fetches cities' data and extract it from the zip file
    returns a stream of the content of the file listacomuni.txt"""
    r = requests.get(url)
    stream = io.BytesIO()
    stream.write(r.content)
    input_zip = zipfile.ZipFile(stream)
    return input_zip.open('listacomuni.txt')


if __name__ == '__main__':

    url = 'http://lab.comuni-italiani.it/files/listacomuni.zip'
    source = fetch_city_cap_zip(url)
    out = read_csv(io.TextIOWrapper(source, errors='ignore'))

    with open('city_caps.csv', 'wt') as f:
        writer = csv.writer(f)
        writer.writerows(out)
