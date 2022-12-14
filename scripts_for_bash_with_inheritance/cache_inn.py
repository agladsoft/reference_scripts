import os
import json
import xml.etree.ElementTree as ET
from requests_html import HTMLSession
from bs4 import BeautifulSoup
import validate_inn
import contextlib
import re
from pathlib import Path
import logging


class GetINNApi:
    def __init__(self, cache_file_name):
        self.cache_file_name = cache_file_name
        self.cache = self.load_cache()

    def get_inn_by_api(self, value, id, var_api_name=None):
        try:
            session = HTMLSession()
            api_inn = session.get(f'https://www.rusprofile.ru/search?query={value}')
            html_code = api_inn.html.html
            html = BeautifulSoup(html_code, 'html.parser')
            page_inn = html.find('span', attrs={'id': id})
            page_name = html.find('h1', attrs={'itemprop': 'name'})
            page_many_inn = html.findAll('span', attrs={'class': 'finded-text'})
            page_many_company = html.findAll("div", attrs={"class": "company-item"})
            for inn, inn_name in zip(page_many_inn, page_many_company):
                if not page_inn and not page_name and inn.text == value:
                    var_api_name = inn_name.find("span", {"class", "finded-text"}).parent.parent.parent.parent.find("a").text.strip()
                    var_api_name = re.sub(" +", " ", var_api_name)
                    break
            if page_name:
                var_api_name = page_name.text.strip()
            return value if value != 'empty' else None, var_api_name
        except Exception:
            return value if value != 'empty' else None, var_api_name

    @staticmethod
    def get_inn_from_html(myroot, index_page, results, list_inn, count_inn):
        value = myroot[0][index_page][0][results][1][3][0].text
        title = myroot[0][index_page][0][results][1][1].text
        inn_text = re.findall(r"\d+", value)
        inn_title = re.findall(r"\d+", title)
        for item_inn, item_title_inn in zip(inn_text, inn_title):
            with contextlib.suppress(Exception):
                inn = validate_inn.validate(item_inn) if validate_inn.is_valid(item_inn) else validate_inn.validate(item_title_inn)
                if inn in list_inn:
                    count_inn += 1
                list_inn[inn] = count_inn

    def get_inn_by_yandex(self, value):
        session = HTMLSession()
        r = session.get(f"https://xmlriver.com/search_yandex/xml?user=6390&key=e3b3ac2908b2a9e729f1671218c85e12cfe643b0&query={value} ??????")
        xml_code = r.html.html
        myroot = ET.fromstring(xml_code)
        index_page = 2 if myroot[0][1].tag == 'correct' else 1
        last_range = int(myroot[0][index_page][0][0].attrib['last'])
        list_inn = {}
        count_inn = 0
        for results in range(1, last_range):
            try:
                self.get_inn_from_html(myroot, index_page, results, list_inn, count_inn)
            except Exception as ex:
                continue
        return max(list_inn, key=list_inn.get) if list_inn else "empty"

    def load_cache(self):
        fle = Path(self.cache_file_name)
        if not os.path.exists(os.path.dirname(fle)):
            os.makedirs(os.path.dirname(fle))
        fle.touch(exist_ok=True)
        if os.stat(self.cache_file_name).st_size == 0:
            return {}
        with open(self.cache_file_name, 'r') as f:
            return json.load(f)

    def get_inn(self, inn):
        if cache_name := self.cache.get(inn, None):
            print(f"???????????? ???????? ?? ????????: ?????? - {inn}, ???????????????????????? - {cache_name}")
            return inn, cache_name
        for key in [inn]:
            api_inn, api_name = None, None
            if key != 'empty':
                if len(key) == 10:
                    api_inn, api_name = self.get_inn_by_api(key, 'clip_inn')
                elif len(key) == 12:
                    api_inn, api_name = self.get_inn_by_api(key, 'req_inn')
            if api_inn is not None and api_name is not None:
                print(self.cache_add_and_save(api_inn, api_name))
                break
            else:
                logging.error(f"Error: {key}")
        return api_inn, api_name

    def get_inn_from_value(self, value):
        if inn := self.cache.get(value, None):
            print(f"???????????? ???????? ?? ????????: ???????????? ???????????????????????? - {value}, ?????? - {inn}")
            return inn, value
        for key in [value]:
            api_inn = self.get_inn_by_yandex(key)
            print(self.cache_add_and_save(value, api_inn))
        return api_inn, value

    def cache_add_and_save(self, api_inn, api_name):
        self.cache[api_inn] = api_name
        with open(self.cache_file_name, 'w') as f:
            json.dump(self.cache, f, indent=4, ensure_ascii=False)
        return "???????????? ???????????????????????? ?? ??????", api_inn


if __name__ == "__main__":
    get_inn_api = GetINNApi("cache_inn/data_inn.json")
    # print(get_inn_api.get_inn("781310635186"))
    print(get_inn_api.get_inn("1658008723"))
