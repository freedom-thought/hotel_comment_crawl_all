#  -*- coding: utf8 -*-
# Author: KM Wang


import time
import json
import warnings

import configparser
import requests
import urllib3
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

warnings.filterwarnings("ignore")


def mei_hotel_comment_crawler(city_list, city_name_list, db_conn, url_home, phantomjs_path, store_path):
    engine = create_engine(db_conn)
    for city in city_list:
        print("开始" + city_name_list[city_list.index(city)] + "市酒店信息的爬取！")
        url = url_home + city + '/'

        Deca = dict(DesiredCapabilities.PHANTOMJS)
        Deca["phantomjs.page.settings.userAgent"] = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"
        # Deca["phantomjs.page.settings.referer"] = "https://hotels.ctrip.com/hotel/44737645.html?isFull=F"
        Deca["phantomjs.page.settings.loadImages"] = False
        Deca["phantomjs.page.settings.resourceTimeout"] = 20
        browser = webdriver.PhantomJS(
            executable_path=phantomjs_path,
            desired_capabilities=Deca)
        urllib3.disable_warnings()
        browser.get(url)
        browser.maximize_window()
        time.sleep(1)

        # 获取 cookies
        cookies = browser.get_cookies()
        cook = ''
        for cookie in cookies:
            cookie_name = cookie['name']
            cookie_value = cookie['value']
            cook = cookie_name + '=' + cookie_value + ';'

        header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.75 Safari/537.36',
            'Cookies': cook}

        data = browser.find_elements_by_css_selector('.poi-title')
        prices = browser.find_elements_by_css_selector('.poi-price em')
        addresses = browser.find_elements_by_css_selector('.poi-address')
        icons = browser.find_elements_by_css_selector('.location-icon')
        ranks = browser.find_elements_by_css_selector('.poi-grade')

        print(len(data))

        for item1 in data:
            time.sleep(1)
            icon = str(icons[data.index(item1)].text)
            hotel_name = str(item1.text).replace(icon, "")
            price = str(prices[data.index(item1)].text).replace('起', '').replace('¥', '')
            address = str(addresses[data.index(item1)].text).replace('查看地图', '')
            rank = ranks[data.index(item1)].text
            link = item1.get_attribute('href')[
                   item1.get_attribute('href').rfind(
                       'm/', 1) + 1:]
            hotel_info = pd.DataFrame(np.array([city_name_list[city_list.index(city)], hotel_name, price, address, rank, 'https://hotel.meituan.com/' + link]).reshape(1, 6),
                                      columns=["city", "hotel_name", "price", "address", "rank", "link"])
            hotel_info.to_sql(name="hotel_comment",
                                              con=engine,
                                              schema="test",
                                              if_exists="append",
                                              index=False,
                                              index_label=False)
            for i in range(1, 60):
                if browser.find_element_by_css_selector('#list-view').text != '很抱歉,暂时没有找到符合您条件的酒店':
                    try:
                        b = 'https://ihotel.meituan.com/group/v1/poi/comment' + link + '?sortType=default&noempty=1&withpic=0&filter=all&limit=10&offset=' + str(
                            (i * 10) - 10)
                        urllib3.disable_warnings()
                        web = requests.get(b, headers=header, allow_redirects=True)
                        web.encoding = 'utf-8'
                        web_data = json.loads(web.text)
                        if list(web_data) != ['error']:
                            for key, value in web_data.items():
                                for key1, value1 in value.items():
                                    if isinstance(value1, type([1, 2])):
                                        for item2 in value1:
                                            for key2, value2 in item2.items():
                                                if key2 == 'comment':
                                                    with open(store_path + city_name_list[
                                                        city_list.index(city)] + "市_" + str(
                                                        hotel_name) + '_user_comment.txt', mode='a',
                                                              encoding='utf-8') as f:
                                                        result1 = {"comment" + str(
                                                            (i - 1) * 10 + value1.index(item2) + 1): value2.replace(
                                                            '#',
                                                            "").replace(
                                                            " ",
                                                            "").replace(
                                                            "。",
                                                            ",")}
                                                        # result = json.dumps(result)
                                                        f.write(str(result1) + "\n")
                                                        f.close()
                                                    # print()
                                            # print()
                    except json.decoder.JSONDecodeError:
                        print("网页未加载成功，继续请求！")
                        pass

        print()

        for m in range(1, 90):
            if browser.find_element_by_css_selector('#list-view').text != '很抱歉,暂时没有找到符合您条件的酒店':
                browser.find_element_by_css_selector('.next a').click()
                time.sleep(1)
                data_next = browser.find_elements_by_css_selector('.poi-title')
                prices = browser.find_elements_by_css_selector('.poi-price em')
                addresses = browser.find_elements_by_css_selector('.poi-address')
                icons = browser.find_elements_by_css_selector('.location-icon')
                ranks = browser.find_elements_by_css_selector('.poi-grade')
                print(len(data_next))
                for item3 in data_next:
                    icon = str(icons[data_next.index(item3)].text)
                    hotel_name = str(item3.text).replace(icon, "")
                    price = str(prices[data_next.index(item3)].text).replace('起', '').replace('¥', '')
                    address = str(addresses[data_next.index(item3)].text).replace('查看地图', '')
                    rank = ranks[data_next.index(item3)].text
                    link = item3.get_attribute('href')[
                           item3.get_attribute('href').rfind(
                               'm/', 2) + 1:]
                    hotel_info = pd.DataFrame(np.array([city_name_list[city_list.index(city)], hotel_name, price, address, rank, 'https://hotel.meituan.com/' + link]).reshape(1, 6),
                                              columns=["city", "hotel_name", "price", "address", "rank", "link"])
                    hotel_info.to_sql(name="hotel_comment",
                                      con=engine,
                                      schema="test",
                                      if_exists="append",
                                      index=False,
                                      index_label=False)
                    for j in range(1, 60):
                        if browser.find_element_by_css_selector('#list-view').text != '很抱歉,暂时没有找到符合您条件的酒店':
                            try:
                                b = 'https://ihotel.meituan.com/group/v1/poi/comment' + link + '?sortType=default&noempty=1&withpic=0&filter=all&limit=10&offset=' \
                                    + str((j * 10) - 10)
                                urllib3.disable_warnings()
                                web = requests.get(b, headers=header, allow_redirects=True)
                                web.encoding = 'utf-8'
                                web_data = json.loads(web.text)
                                if list(web_data) != ['error']:
                                    for key, value in web_data.items():
                                        for key1, value1 in value.items():
                                            if isinstance(value1, type([1, 2])):
                                                for item4 in value1:
                                                    for key2, value2 in item4.items():
                                                        if key2 == 'comment':
                                                            with open(
                                                                    store_path +
                                                                    city_name_list[
                                                                        city_list.index(city)] + "_" + str(
                                                                        hotel_name) + '_user_comment.txt', mode='a',
                                                                    encoding='utf-8') as f:
                                                                result2 = {
                                                                    "comment" + str((j - 1) * 10 + value1.index(
                                                                        item4) + 1): value2.replace(
                                                                        '#',
                                                                        "").replace(
                                                                        " ",
                                                                        "").replace(
                                                                        "。",
                                                                        ",")}
                                                                # result = json.dumps(result)
                                                                f.write(str(result2) + "\n")
                                                                f.close()
                                                            # print()
                                                    # print()
                            except (json.decoder.JSONDecodeError, OSError, urllib3.exceptions.ProtocolError,
                                    requests.exceptions.ConnectionError):
                                print("网页未加载成功，继续请求！")
                                pass
                # print()

        browser.close()
        browser.quit()

        time.sleep(2)


if __name__ == '__main__':
    conf_file = "/kafka/model/location_com/config/Location_Conf_all.ini"
    config = configparser.ConfigParser()
    config.read(conf_file, encoding="utf-8")

    city_list_ = config.get('city_list', 'city_list')
    city_name_list_ = config.get('city_name_list', 'city_name_list')
    db_conn_ = config.get('db_conn', 'db_conn')
    url_home_ = config.get('url_home', 'url_home')
    phantomjs_path_ = config.get('phantomjs_path', 'phantomjs_path')
    store_path_ = config.get('store_path', 'store_path')
    mei_hotel_comment_crawler(city_list_, city_name_list_, db_conn_, url_home_, phantomjs_path_, store_path_)
