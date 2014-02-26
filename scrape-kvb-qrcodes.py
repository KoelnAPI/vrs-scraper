# encoding: utf-8


"""
Liest KVB Haltestellennamen und deren QR-Code-ID
und speichert das Ergebnis in der CSV-Datei
kvb-qr-codes.csv
"""

import requests
from bs4 import BeautifulSoup
import time
import re
import csv


def scrape():
    headers = {
        "referer": "http://www.kvb-koeln.de/qr/haltestellen",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.117 Safari/537.3"
    }
    urlmask = "http://www.kvb-koeln.de/qr/haltestellen/%s/"
    tokens = [
        "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K",
        "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V",
        "W", "X", "Y", "Z", "Ae", "Ue"]
    for token in tokens:
        url = urlmask % token
        r = requests.get(url, headers=headers)
        #print r.text
        if r.status_code == 200:
            soup = BeautifulSoup(r.text)
            content = soup.find("center")
            for a in content.find_all("a"):
                #print a.get("href"), a.text
                match = re.match(r"\/qr\/([0-9]+)\/", str(a.get("href")))
                if match is not None:
                    yield (int(match.group(1)), a.text)
        time.sleep(1)


def export(stations, path):
    with open(path, "wb") as csvfile:
        writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(["id", "name"])
        for row in stations:
            writer.writerow([
                str(row[0]),
                row[1].encode("utf8")
            ])


if __name__ == "__main__":
    stations = list(scrape())
    export(stations, "kvb-qr-codes.csv")
