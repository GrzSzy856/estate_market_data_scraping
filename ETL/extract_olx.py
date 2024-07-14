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
class OlxScraper:
    """
    A class that allows the scraping of data from the "olx" website, which provides information about housing offers in Poland.

    Attributes:
        cities_individual_urls (dict): Dictionary containing individual offers utl for given cities in a list. (Set internally)

    Methods:
        check_page_exists():
            Checks whether given page contains any offers. (Only for internal purposes)
        get_all_urls():
            Generates all individual offers url's based on the main url's.
        get_json():
            Scrapes json content from html code.
        scrap_data():
            Scraps data from the individual offers pages.
    
    """

    _base_url = 'https://www.olx.pl/nieruchomosci/mieszkania/sprzedaz/'
    _params = '/?page={f}&view=grid'

    def __init__(self):
        """Initializes the scraper."""

        self.cities_individual_urls = {
            'Katowice': [],
            'Krakow': [],
            'Warszawa': [],
            'Wroclaw': []
        }

    def check_page_exists(self, url):
        """Checks whether given page exists. (Only for internal purposes)"""

        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = requests.get(url, headers=headers, allow_redirects=True)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            if not soup.find('p', string='Sprawdź ogłoszenia w większej odległości:') and (response.url == url or re.search(r'page=(\d+)', url).group(1) == '1'):
                return True
        return False
        

    def get_all_urls(self, print_page_numbers=False):
        """
        Generates all individual offers url's.
        
        Args:
            print_page_numbers (bool, optional): Indicates whether to print city names and page numbers, while scraping the data.

        Returns:
            cities_pages (dict): Dictionary with url's of available pages for each city.
        """

        for city_name in self.cities_individual_urls:
            page_number = 1
            hrefs = []
            while True:
                full_url = ''.join([OlxScraper._base_url, city_name, OlxScraper._params]).format(f = page_number)
                if not self.check_page_exists(full_url):
                    print(f"Page: {full_url} doesn't exist")
                    break

                if print_page_numbers:
                    print(f"{city_name}: Page {page_number} exists.")

                response = requests.get(full_url, headers=headers, allow_redirects=True)
                soup = BeautifulSoup(response.content, "html.parser")
                a_elements = soup.find_all('a', {"class": "css-z3gu2d"})

                for a in a_elements:
                    href = a['href']
                    if 'otodom' not in href:
                        href = 'https://www.olx.pl' + href
                        hrefs.append(href)

                page_number += 1
            self.cities_individual_urls[city_name].extend(hrefs)

    
    def get_json(self, url):
        """
        Scraps json content from html code.

        Args:
            url (str): url to an olx offer page.
        Returns:
            json_content (json): Data related to an offer.
        
        """

        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.content, "html.parser")
        soup_str = str(soup)

        start_marker = '__PRERENDERED_STATE__= "'
        end_marker = 'window.__TAURUS__='

        start_index = soup_str.find(start_marker) + len(start_marker)

        end_index = soup_str.find('";', start_index)

        if end_index != -1:
            result = soup_str[start_index:end_index]
        else:
            result = soup_str[start_index:]

        clean_string = result.replace('\\"', '"').replace('\\\\"', '\\"')

        json_content = json.loads(clean_string)['ad']['ad']
        return(json_content)


    def scrap_data(self, print_page_numbers=False):
        """
        Scraps data from the json.
        
        Args:
            print_page_numbers (bool, optional): Indicates whether to print city names and page numbers, while scraping the data.

        Returns:
            olxData (Data Frame): a table with data scraped from all the offer pages.
        """
        
        olxData = pd.DataFrame()
        for city_name, offer_urls in self.cities_individual_urls.items():
            for i, offer_url in enumerate(offer_urls):
                try:
                    json_content = self.get_json(offer_url)
                except Exception as E:
                    print('No data has been found in the json file')
                    break
                
                generalInformation = {
                'id': json_content['id'],
                'source': 'OLX',
                'date': date.today().strftime('%Y-%m-%d'),
                'city_name': city_name,
                'market_type': 'Prywatny',
                'create_date': json_content['createdTime'],
                'modify_date': json_content['lastRefreshTime'],
                'title': json_content['title'],
                'url': json_content['url'],
                'price': json_content['price']['regularPrice']['value']

                }

                params = {}
                params.setdefault('price_per_m', None)
                params.setdefault('floor', None)
                params.setdefault('furniture', None)
                params.setdefault('market_type', None)
                params.setdefault('area', None)
                params.setdefault('rooms_num', None)

                for data in json_content['params']:
                    label = data['key']
                    values = data['normalizedValue']

                    if (label == 'price_per_m') & (len(values) > 0):
                        params['price_per_m'] = values

                    if (label == 'floor_select') & (len(values) > 0):
                        params['floor'] = values

                    if (label == 'furniture') & (len(values) > 0):
                        params['furniture'] = values

                    if (label == 'market') & (len(values) > 0):
                        params['market_type'] = values

                    if (label == 'm') & (len(values) > 0):
                        params['area'] = values

                    if (label == 'rooms') & (len(values) > 0):
                        params['rooms_num'] = values

                allInformation = generalInformation | params
                allInformationDF = pd.DataFrame(allInformation, index=[i])
                olxData = pd.concat([olxData, allInformationDF])

                if print_page_numbers:
                    print(f"City: {city_name}, page number: {i} out of {len(offer_urls)}")
        
        return olxData

# %%
x = OlxScraper()

# %%
x.get_all_urls(print_page_numbers=True)

# %%
dane = x.scrap_data(print_page_numbers=True)

# %%



