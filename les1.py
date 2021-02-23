import time
import json
from pathlib import Path
import requests


headers = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 "
                  "(Windows NT 10.0; Win64; x64; rv:85.0) "
                  "Gecko/20100101 Firefox/85.0",
}


class Parse5Ka:

    def __init__(self, start_url: str, products_path: Path, category, code):
        self.start_url = start_url
        self.products_path = products_path
        self.category = category
        self.code = code

    @staticmethod
    def _get_response(url):
        while True:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return response
            time.sleep(0.5)

    def run(self):
        file_path = self.products_path.joinpath(f"{self.category}.json")
        lst = []
        for product in self._parse(self.start_url):
            lst.append(product)
            print(product)
        dct = {
            "name": self.category,
            "code": self.code,
            "products": lst
        }
        self._save(dct, file_path)

    def _parse(self, url):
        while url:
            response = self._get_response(url)
            data = response.json()
            url = data["next"]
            for product in data["results"]:
                yield product

    @staticmethod
    def _save(data: dict, file_path):
        jdata = json.dumps(data, ensure_ascii=False)
        file_path.write_text(jdata, encoding="UTF-8")


def get_dir_path(dirname: str) -> Path:
    dir_path = Path(__file__).parent.joinpath('results').joinpath(dirname)
    if not dir_path.exists():
        dir_path.mkdir()
    return dir_path


if __name__ == "__main__":
    cat_url = "https://5ka.ru/api/v2/categories/"
    cats_response = requests.get(cat_url, headers=headers)
    cats = json.loads(cats_response.text)
    results_path = Path(__file__).parent.joinpath('results')
    if not results_path.exists():
        results_path.mkdir()  # Тут немного колхозинга, я не понял как заставить write_text создавать полный путь.
    for cat in cats:
        url1 = f'https://5ka.ru/api/v2/special_offers/?page=1&categories={cat["parent_group_code"]}'
        parser = Parse5Ka(url1, results_path, cat["parent_group_name"], cat["parent_group_code"])
        parser.run()
