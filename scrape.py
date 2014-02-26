# encoding: utf8

"""
Scraper für die Haltestellen des VRS.

Stationen werden anhand von verschiedenen Listen von
Suchbegriffen gefunden.

Stations-Einträge werden in der Regel mit Geokoordinaten
ausgegeben, die in WGS84-Koordinaten (Länge/Breite) umgerechnet
werden.

Es werden nur Einträge gespeichert, die innerhalb der Stadtgrenze
von Köln liegen.

"""

import requests
import time
from bs4 import BeautifulSoup
import sys
from pyproj import Proj
from pyproj import transform
import json
import sqlite3
from shapely.geometry import Polygon, Point
import sys
import string
import csv


def init_db():
    cursor.execute('''CREATE TABLE IF NOT EXISTS stations
             (id integer primary key,
            assnr integer,
            name text,
            x real,
            y real)''')
    conn.commit()


def save_station(entry):
    cursor.execute("""INSERT OR IGNORE
            INTO stations (id, assnr, name, x, y)
            VALUES (?, ?, ?, ?, ?)""",
        (entry["id"], entry["assnr"], entry["name"],
            entry["coords"][0], entry["coords"][1]))
    conn.commit()


def get_bounds():
    """
    Importiere Grenze von Köln
    als Shapely-Polygon
    """
    f = open("koeln_polygon.geojson")
    data = json.loads(f.read())
    f.close()
    coords = data["coordinates"][0]
    return Polygon(coords)


def find_stations(searchterm):
    """
    Sucht eine Station über die XML-Suche von auskunft.vrsinfo.de
    """
    url = "http://auskunft.vrsinfo.de/vrs/cgi/service/objects"
    headers = {
        "referer": "http://auskunft.vrsinfo.de/vrs/cgi/process/eingabeRoute"
    }
    search_string = '<?xml version="1.0" encoding="ISO-8859-1"?><Request>'
    search_string += '<ObjectInfo><ObjectSearch><String>%s</String><Classes>'
    search_string += '<Stop><GKZ>5</GKZ></Stop></Classes></ObjectSearch>'
    search_string += '<Options><Output><SRSName>urn:adv:crs:ETRS89_UTM32'
    search_string += '</SRSName></Output></Options></ObjectInfo></Request>'
    data = search_string % searchterm.decode("utf8").encode("latin-1")

    r = requests.post(url,
        allow_redirects=True,
        headers=headers,
        data=data)
    if r.status_code == 200:
        soup = BeautifulSoup(r.text)
        objectinfo = soup.find("objectinfo")
        for obj in objectinfo.find_all("object"):
            otype = obj.find("type").text
            if otype != "Stop":
                continue
            entry = {
                "id": int(obj.find("id").text),
                "assnr": int(obj.find("stop").find("assnr").text),
                "name": obj.find("value").text,
                "coords": obj.find("coords").text.split(","),
            }
            if len(entry["coords"]) != 2:
                continue
            try:
                entry["coords"] = transform(
                    source_proj, target_proj,
                    float(entry["coords"][0]),
                    float(entry["coords"][1]))
                yield entry
            except ValueError:
                print("ERROR:", entry)
    else:
        sys.stderr.write("FEHLER: %s\n" % r.status_code)


def find_stations2(searchterm):
    """
    Gibt Suchergebnis strukturierter aus. Gemeinde und Ortsteil sind
    als eigene Felder enthalten, assnr fehlt jedoch.
    """
    headers = {
        "referer": "http://www.vrsinfo.de/fahrplan/haltestellenkarte.html?tx_vrsstations_pi_map%5Bbb%5D%5Bnorth%5D=5661439&tx_vrsstations_pi_map%5Bbb%5D%5Beast%5D=2581842&tx_vrsstations_pi_map%5Bbb%5D%5Bsouth%5D=5633321&tx_vrsstations_pi_map%5Bbb%5D%5Bwest%5D=2554201"
    }
    url = "http://www.vrsinfo.de/index.php"
    payload = {
        'eID': 'tx_sbsgeoutil_getStops',
        'cmd': 'stops',
        'search_string': searchterm,
        'export_type': 'json',
        'xmin': '2511000',
        'xmax': '2639000',
        'ymin': '5566000',
        'ymax': '5694000'
    }
    url = url % searchterm
    r = requests.get(url,
        allow_redirects=True,
        headers=headers,
        params=payload)
    if r.status_code == 200:
        results = r.json
        if len(results) > 0:
            for result in results:
                if result["type"] != "stop":
                    continue
                entry = {
                    "id": int(result["id"]),
                    "name": result["name"],
                    "city": result["gemeinde"],
                    "suburb": result["ort"],
                    "coords": [
                        float(result["coord"]["x"]),
                        float(result["coord"]["y"])
                    ]
                }
                yield entry


def get_strassen_koeln():
    url = "http://www.offenedaten-koeln.de/node/569/download"
    r = requests.get(url)
    count = -1
    for line in r.text.split("\n"):
        row = line.split(";")
        count += 1
        if line.strip() == '':
            continue
        if count > 0:
            yield row[1]


def get_stationen_textfile():
    f = open("stationsnamen.txt")
    lines = f.read().split("\n")
    for line in lines:
        line = line.strip()
        if line != "":
            yield line


def get_stationen_qrcodes():
    f = open("kvb-qr-codes.csv")
    lines = f.read().split("\n")
    for line in lines:
        line = line.strip()
        if line != "":
            sid, name = line.split(",", 1)
            yield (sid, name)


def export(path):
    with open(path, "wb") as csvfile:
        writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(["id", "assnr", "name", "x", "y"])
        for row in conn.execute("select * from stations order by id"):
            writer.writerow([
                str(row[0]),
                str(row[1]),
                row[2].encode("utf8"),
                "%.7f" % row[3],
                "%.7f" % row[4]
            ])


if __name__ == '__main__':

    conn = sqlite3.connect('scrape_cache.db')
    cursor = conn.cursor()
    init_db()

    # Projektionen für Koordinaten-Umrechnung
    # VRS Quelle: ETRS89 UTM32
    source_proj = Proj(init='epsg:25832')
    target_proj = Proj(init='epsg:4326')  # WGS84

    # Grenze
    bounds = get_bounds()

    searchterms = set()

    ### Hier werden nach verschiedenen Methoden
    ### Suchbegriffe für die Suche nach Stationen
    ### zusammen getragen

    ## Stationsnamen aus Textdatei:
    #for station in get_stationen_textfile():
    #    searchterm = station + ", köln"
    #    searchterms.add(searchterm)

    # Stationsnamen aus QR-Codes-Stationsliste:
    for (sid, name) in get_stationen_qrcodes():
        searchterm = name + ", köln"
        searchterms.add(searchterm)

    ## Straßennamen
    #for street in get_strassen_koeln():
    #    searchterm = street.encode("utf8") + ", köln"
    #    searchterms.add(searchterm)

    ## Buchstabenkombinationen
    #letters = string.lowercase[0:26]
    #for l1 in letters:
    #    for l2 in letters:
    #        searchterm = l1 + l2 + ", köln"
    #        searchterms.add(searchterm)

    num = len(searchterms)
    count = 0

    for term in searchterms:
        count += 1
        print("%d von %d (%.2f%%): %s" % (count, num,
            (float(count) * 100.0 / float(num)), term))
        for result in find_stations(term):
            position = Point(result["coords"])
            if bounds.contains(position):
                print result
                save_station(result)
        time.sleep(2)

    export("stations.csv")
