#!/usr/bin python3
# -*- coding: utf-8 -*-
# @Time    : 2019/3/7 17:40
# @Author  : nwm@chuangdata.com
# @File    : urban_rural_division_spider.py
import requests
import lxml
from lxml import etree
import csv
import time
import random
from retry import retry
from tqdm import tqdm


def fileload():
    f = open('./urban.csv', mode='w',
             encoding='utf-8', errors='ignore', newline='')
    return f


def run(writer):
    part_url = 'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/'
    url = part_url + 'index.html'
    tree = spider(url)

    td_list = tree.xpath('.//tr[@class="provincetr"]/td')
    for td in tqdm(td_list):
        href = td.xpath('./a/@href')[0]
        province = td.xpath('./a/text()')[0]
        url = part_url + href
        'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/33.html'
        city_tree = spider(url)

        citytr_list = city_tree.xpath('.//tr[@class="citytr"]')
        for citytr in citytr_list:
            county_href = citytr.xpath('./td[1]/a/@href')[0]
            code_lv2 = citytr.xpath('./td[1]/a/text()')[0]
            city_name = citytr.xpath('./td[2]/a/text()')[0]
            # print(city_name)
            county_url = part_url + county_href
            'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/33/3301.html'
            county_tree = spider(county_url)

            countytr_list = county_tree.xpath('.//tr[@class="countytr"]')
            for countytr in countytr_list:
                town_href = countytr.xpath('./td[1]/a/@href')
                if not town_href:
                    code_lv3 = countytr.xpath('./td[1]/text()')[0]
                    county_name = countytr.xpath('./td[2]/text()')[0]
                    code_lv4 = town_name = code_lv5 = village_name = '-'
                    writer.writerow([province, code_lv2, city_name, code_lv3, county_name,
                                     code_lv4, town_name, code_lv5, village_name])
                else:
                    town_href = town_href[0]
                    code_lv3 = countytr.xpath('./td[1]/a/text()')[0]
                    county_name = countytr.xpath('./td[2]/a/text()')[0]
                    # print(county_name)
                    town_url = part_url + str(county_href).split('/')[0] + '/' + town_href
                    'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/33/01/330102.html'
                    town_tree = spider(town_url)

                    towntr_list = town_tree.xpath('.//tr[@class="towntr"]')
                    for towntr in towntr_list:
                        village_href = towntr.xpath('./td[1]/a/@href')[0]
                        code_lv4 = towntr.xpath('./td[1]/a/text()')[0]
                        town_name = towntr.xpath('./td[2]/a/text()')[0]

                        village_url = part_url + str(county_href).split('/')[0] + '/' \
                                      + str(town_href).split('/')[0] + '/' + village_href
                        'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/33/01/02/330102001.html'
                        villages_tree = spider(village_url)

                        village_list = villages_tree.xpath('.//tr[@class="villagetr"]')
                        for village in village_list:
                            code_lv5 = village.xpath('./td[1]/text()')[0]
                            village_name = village.xpath('./td[3]/text()')[0]
                            writer.writerow([province, code_lv2, city_name, code_lv3, county_name,
                                             code_lv4, town_name, code_lv5, village_name])


@retry(delay=1, backoff=5, jitter=(0, 1))
def spider(url):
    time.sleep(random.random())

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Cookie": "AD_RS_COOKIE=20085415",
        "Host": "www.stats.gov.cn",
        "Pragma": "no-cache",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36",

    }

    res = requests.get(url, headers=headers)
    # res = requests.get(url, headers)
    res.encoding = 'gbk'
    # res.encoding = res.apparent_encoding
    tree = lxml.etree.HTML(res.text)
    return tree


def main():
    f = fileload()
    writer = csv.writer(f)
    run(writer)
    f.close()


if __name__ == '__main__':
    import logging

    logging.basicConfig()
    main()
