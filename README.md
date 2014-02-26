vrs-scraper
===========

Scrapes station information from VRS (vrsinfo.de)


### Setup

    virtualenv venv
    source venv/bin/activate
    pip install Shapely beautifulsoup4 pyproj requests

### Usage

Take your time and run the following commands:
	
	# optional (recreate kvb-qr-codes.csv)
    python scrape-kvb-qrcodes.py

    # create stations.csv
    python scrape.py

