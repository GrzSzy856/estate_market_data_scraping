# %%
import pandas as pd
import scrapy
import requests
import os
from bs4 import BeautifulSoup
import re
import json
from datetime import datetime, date

# %%
class OtodomScraper:
    """
    A class that allows the scraping of data from the "otodom" website, which provides information about housing offers in Poland.

    Attributes:
        key (str): The otodom url key that enables the data scraping.
        cities_pages (dict): Dictionary containing the number of available pages for given cities. (Set internally)
        cities_individual_urls (dict): Dictionary containing individual offers utl for given cities in a list. (Set internally)

    Methods:
        check_page_exists():
            Checks whether given page contains any offers. (Only for internal purposes)
        get_individual_urls():
            Scraps the url's of all the offer listings. (Only for internal purposes)
        get_all_urls():
            Generates all individual offers url's based on the main url's.
        scrap_data():
            Scraps data from the individual offers pages.
    
    """

    _base_url = 'https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie'
    _params = '?limit=72&viewType=listing&page='
    _cities = {
            'Katowice': '/slaskie/katowice/katowice/katowice', 
            'Kraków': '/malopolskie/krakow/krakow/krakow', 
            'Warszawa': '/mazowieckie/warszawa/warszawa/warszawa',
            'Wrocław': '/dolnoslaskie/wroclaw/wroclaw/wroclaw'
    }
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    
    def __init__(self, key):
        """
        Initializes the scraper with a given key.

        Args:
            key (str): The otodom url key that enables the data scraping.        
        """

        self.key = key
        self.cities_pages = {
            'Katowice': 0,
            'Kraków': 0,
            'Warszawa': 0,
            'Wrocław': 0
        }
        self.cities_individual_urls = {
            'Katowice': [],
            'Kraków': [],
            'Warszawa': [],
            'Wrocław': []
        }


    def check_page_exists(self, url):
        """Checks whether given page exists. (Only for internal purposes)"""

        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            json_content = BeautifulSoup(response.text, 'html.parser')
            if not json_content.find('h3', string='Nie znaleźliśmy żadnych ogłoszeń'):
                return True
        return False

    def get_individual_urls(self, city_name, main_url):
        """Scraps the url's of all the offer listings."""

        response = requests.get(main_url, headers=self.headers)
        json_content = BeautifulSoup(response.content, "html.parser")

        hrefs = []
        for section in json_content.find_all('section'):
            classes = section.get('class')
            href = section.find_all('a', {"class": "css-16vl3c1 e17g0c820"})
            if classes:
                classes = [classes for c in classes if 'eeungyz1' in c]
                if len(classes) > 0:
                    for h in href:
                        link = f"https://www.otodom.pl/_next/data/{self.key}{h.get('href')}.json"
                        hrefs.append(link)
        self.cities_individual_urls[city_name].extend(hrefs)
        

    def get_all_urls(self, print_page_numbers=False):
        """
        Generates all individual offers url's based on the main url's.
        
        Args:
            print_page_numbers (bool, optional): Indicates whether to print city names and page numbers, while scraping the data.

        Returns:
            cities_pages (dict): Dictionary with number of available pages for each city.
        """

        for city_name, city_url in OtodomScraper._cities.items():
            url = ''.join([OtodomScraper._base_url, city_url, OtodomScraper._params])
            page_number = 1 
            while True:
                full_url = url + str(page_number)
                if not self.check_page_exists(full_url):
                    print(f"Page: {full_url} doesn't exist")
                    break

                if print_page_numbers:
                    print(f"{city_name}: Page {page_number} exists.")

                self.get_individual_urls(city_name, full_url)
                page_number += 1
                self.cities_pages[city_name] += 1
        
        return self.cities_pages
    

    def scrap_data(self, print_page_numbers=False):
        """
        Scraps data from the individual offers pages.
        
        Args:
            print_page_numbers (bool, optional): Indicates whether to print city names and page numbers, while scraping the data.

        Returns:
            otoDomData (Data Frame): a table with data scraped from all the offer pages.
        """
        
        otoDomData = pd.DataFrame()
        for city_name, offer_urls in self.cities_individual_urls.items():
            for i, offer_url in enumerate(offer_urls):
                response = requests.get(offer_url, headers=self.headers)
                try:
                    json_content = json.loads(response.content)['pageProps']['ad']
                except:
                    print('No data has been found in the json file')
                    continue
                print(offer_url)
                try:
                    generalInformation = {
                    'id': json_content.get('id', None),
                    'source':'OtoDom',
                    'date': date.today().strftime('%Y-%m-%d'),
                    'city': city_name,
                    'market_type': json_content.get('market', None),
                    'create_date': json_content.get('createdAt', None),
                    'modify_date': json_content.get('modifiedAt', None),
                    'title': json_content.get('title', None),
                    'url': json_content.get('url', None),
                    'price': json_content.get('target', {}).get('Price', None),
                    'price_per_m': json_content.get('target', {}).get('Price_per_m', None),
                    'area': json_content.get('target', {}).get('Area', None),
                    'building_year': json_content.get('target', {}).get('Build_year', None),
                    'construction_status': json_content.get('target', {}).get('Construction_status', None)
                    }
                except: 
                    print('Cannot retrieve the data')
                    continue


                topInformation = {}
                topInformation.setdefault('rooms_num', None)
                topInformation.setdefault('car', None)
                topInformation.setdefault('rent', None)
                topInformation.setdefault('floor', None)
                topInformation.setdefault('outdoor', None)
                topInformation.setdefault('heating', None)

                for data in json_content['topInformation']:
                    label = data['label']
                    values = data['values']

                    if (label == 'rooms_num') & (len(values) > 0):
                        topInformation['rooms_num'] = values[0]

                    if (label == 'car') & (len(values) > 0):
                        topInformation['car'] = values[0]

                    if (label == 'rent') & (len(values) > 0):
                        topInformation['rent'] = values[0]

                    if (label == 'floor') & (len(values) > 0):
                        topInformation['floor'] = values[0]

                    if (label == 'outdoor') & (len(values) > 0):
                        topInformation['outdoor'] = values[0]

                    if (label == 'heating') & (len(values) > 0):
                        topInformation['heating'] = values[0]


                additionalInformation = {}
                additionalInformation.setdefault('building_material', None)
                additionalInformation.setdefault('windows_type', None)
                additionalInformation.setdefault('media_types', None)
                additionalInformation.setdefault('security_types', None)
                additionalInformation.setdefault('lift', None)

                for data in json_content['additionalInformation']:
                    label = data['label']
                    values = data['values']

                    if (label == 'building_material') & (len(values) > 0):
                        additionalInformation['building_material'] = values[0]

                    if (label == 'windows_type') & (len(values) > 0):
                        additionalInformation['windows_type'] = values[0]

                    if (label == 'media_types') & (len(values) > 0):
                        additionalInformation['media_types'] = values[0]

                    if (label == 'security_types') & (len(values) > 0):
                        additionalInformation['security_types'] = values[0]

                    if (label == 'lift') & (len(values) > 0):
                        additionalInformation['lift'] = values[0]


                allInformation = generalInformation | additionalInformation | topInformation
                allInformationDF = pd.DataFrame(allInformation, index=[i])
                otoDomData = pd.concat([otoDomData, allInformationDF])

                if print_page_numbers:
                    print(f"City: {city_name}, page number: {i} out of {len(offer_urls)}")
        
        return otoDomData



