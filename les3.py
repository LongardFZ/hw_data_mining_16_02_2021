import requests
from urllib.parse import urljoin
import bs4
import typing
from database.db import Database
import pandas as pd


class GbBlogParse:

    def __init__(self, start_url, database: Database):
        self.db = database
        self.start_url = start_url
        self.done_urls = set()
        self.tasks = []

    @staticmethod
    def _get_response(url):
        response = requests.get(url)
        return response

    def _get_soup(self, url):
        resp = self._get_response(url)
        soup = bs4.BeautifulSoup(resp.text, 'lxml')
        return soup

    def __create_task(self, url, callback, tag_list) -> None:
        for link in set(urljoin(url, href.attrs.get('href'))
                        for href in tag_list if href.attrs.get('href')):
            if link not in self.done_urls:
                task = self._get_task(link, callback)
                self.done_urls.add(link)
                self.tasks.append(task)

    def _parse_feed(self, url, soup) -> None:
        ul = soup.find('ul', attrs={'class': 'gb__pagination'})
        self.__create_task(url, self._parse_feed, ul.find_all('a'))
        self.__create_task(url, self._parse_post, soup.find_all('a', attrs={'class': 'post-item__title'}))
        pass

    def _parse_post(self, url, soup) -> dict:
        author_name_tag = soup.find("div", attrs={"itemprop": "author"})
        data = {
            "post_data": {
                "title": soup.find("h1", attrs={"class": "blogpost-title"}).text,
                "url": url,
                "id": soup.find("comments").attrs.get("commentable-id"),
                "image": soup.find('img').get('src'),
                "publication_date": pd.Timestamp(soup.find('time').get('datetime')).tz_convert("UTC")
            },
            "author": {
                "name": author_name_tag.text,
                "url": urljoin(url, author_name_tag.parent.attrs.get('href')),
            },
            "tags": [{"name": a_tag.text, "url": urljoin(url, a_tag.attrs.get('href'))}
                     for a_tag in soup.find_all("a", attrs={"class": "small"})],
            "comments_data": [{'comment_id': comment['id'],
                               'parent_id': comment['parent_id'],
                               'author': comment['user']['full_name'],
                               'body': comment['body']}
                              for comment in self._get_comments(soup.find("comments").attrs.get("commentable-id"))]

        }
        return data

    def _get_comments(self, post_id) -> list:
        api_path = f"/api/v2/comments?commentable_type=Post&commentable_id={post_id}&order=desc"
        response = self._get_response(urljoin(self.start_url, api_path))
        data = response.json()
        comments_list = []
        comments_list_final = []

        def comment_rec(comms: list):
            tmp = []
            for comm in comms:
                if comm['children']:
                    tmp = [comments['comment'] for comments in comm['children']]
                    comments_list.append(tmp)
                comment_rec(tmp)

        parents = [comments['comment'] for comments in data]
        comments_list.append(parents)
        comment_rec(parents)
        for com in comments_list:
            comments_list_final.extend(com)
        return comments_list_final

    def _get_task(self, url, callback: typing.Callable) -> typing.Callable:
        def task():
            soup = self._get_soup(url)
            return callback(url, soup)

        return task

    def run(self):
        self.tasks.append(self._get_task(self.start_url, self._parse_feed))
        self.done_urls.add(self.start_url)
        for task in self.tasks:
            result = task()
            if isinstance(result, dict):
                self.db.create_post(result)
            print(1)


if __name__ == '__main__':
    db = Database("sqlite:///gb_blog.db")
    parser = GbBlogParse('https://geekbrains.ru/posts', db)
    parser.run()
