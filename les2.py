import requests
import bs4
import time
from urllib.parse import urljoin
import pymongo
import re
import datetime


class MagnitParse:

    def __init__(self, start_url, db):
        self.start_url = start_url
        self.db = db['data_mining_16_02_2021']

    @staticmethod
    def _get_response(url):
        while True:
            response = requests.get(url)
            if response.status_code == 200:
                return response
            time.sleep(0.5)

    def _get_soup(self, url):
        response = self._get_response(url)
        soup = bs4.BeautifulSoup(response.text, "lxml")
        return soup

    def run(self):
        soup = self._get_soup(self.start_url)
        catalog = soup.find('div', attrs={'class': 'сatalogue__main'})
        for product_a in catalog.find_all('a', recursive=False):
            product_data = self._parse(product_a)
            self.save(product_data)

    @staticmethod
    def to_datetime(day, str_month, year):
        month = {
            'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04',
            'мая': '05', 'июня': '06', 'июля': '07', 'августа': '08',
            'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12'
        }
        num_month = month[str_month]
        str_date = f'{year}-{num_month}-{day}'
        date = datetime.datetime.strptime(str_date, '%Y-%m-%d')
        return date

    def date_parser(self, period, till_or_from):
        now = datetime.datetime.now()
        year = int(now.year)
        date_strings = re.findall(r'\d+ \w+', period)
        day_from, str_month_from = re.split(' ', date_strings[0])
        date_from = self.to_datetime(day_from, str_month_from, year)
        if len(date_strings) > 1:
            day_till, str_month_till = re.split(' ', date_strings[1])
            date_till = self.to_datetime(day_till, str_month_till, year)
        elif till_or_from == 'from':
            return date_from
        else:
            return None
        if date_till < date_from:
            date_till = self.to_datetime(day_till, str_month_till, year+1)
        if date_from > now:
            date_from = self.to_datetime(day_till, str_month_till, year-1)
        if till_or_from == 'from':
            return date_from
        else:
            return date_till

    def template(self):
        temp = {
            'url': lambda a: urljoin(self.start_url, a.attrs.get('href')),
            'promo_name': lambda a: a.find('div', attrs={'class': 'card-sale__header'}).text,
            'title': lambda a: a.find('div', attrs={'class': 'card-sale__title'}).text,
            'old_price': lambda a:
                float(
                    f"{a.find('div', attrs={'class': 'label__price label__price_old'}).find('span', attrs={'class': 'label__price-integer'}).text}."
                    f"{a.find('div', attrs={'class': 'label__price label__price_old'}).find('span', attrs={'class': 'label__price-decimal'}).text}"),

            'new_price': lambda a:
                float(
                    f"{a.find('div', attrs={'class': 'label__price label__price_new'}).find('span', attrs={'class': 'label__price-integer'}).text}."
                    f"{a.find('div', attrs={'class': 'label__price label__price_new'}).find('span', attrs={'class': 'label__price-decimal'}).text}"),

            'image_url': lambda a: urljoin(self.start_url, a.find(attrs={'class': 'lazy'})['data-src']),
            'date_from': lambda a: self.date_parser(a.find('div', attrs={'class': 'card-sale__date'}).text, 'from'),
            'date_to': lambda a: self.date_parser(a.find('div', attrs={'class': 'card-sale__date'}).text, 'till')
        }
        return temp

    def _parse(self, product_a: bs4.Tag) -> dict:
        product_data = {}                          # ОБЪЯСНЕНИЕ:
        for key, func in self.template().items():  # items возвращает список пар словаря. пары раскладываются на 2
                                                   # переменных,key и funk. Funk - лямбда функция.
                                                   # словарь без .items() не раскладывается на переменные,
                                                   # возвращая только ключ
                                                   # dict = {'111': 'aaa',
                                                   #         '222': 'bbb'}
                                                   # for key, value in dict:
                                                   #     print (value) # такой код кидает ошибку.
            try:
                product_data[key] = func(product_a)
            except AttributeError:
                pass
        print(1)
        return product_data

    def save(self, data: dict):
        collection = self.db['magnit']
        collection.insert_one(data)


if __name__ == "__main__":
    db_client = pymongo.MongoClient()
    magnit_url = "https://magnit.ru/promo/?geo=moskva"
    parser = MagnitParse(magnit_url, db_client)
    parser.run()
