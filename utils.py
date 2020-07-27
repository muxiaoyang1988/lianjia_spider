# -*- coding: utf-8 -*-
import functools
import random
import time

import requests
from requests.adapters import HTTPAdapter

from proxy import ProxyPool
from settings import logging

User_Agent = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
    + 'Chrome/74.0.3729.169 Safari/537.36',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:31.0) Gecko/20100101 Firefox/31.0',
    'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6',
    'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.12 Safari/535.11',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:34.0) Gecko/20100101 Firefox/34.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu '
    + 'Chromium/44.0.2403.89 Chrome/44.0.2403.89 Safari/537.36',
    'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 '
    + 'Safari/534.50',
    'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1',
    'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) '
    + 'Chrome/17.0.963.56 Safari/535.11',
    'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11',
    'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11',
]


def get_header():
    return {'User-Agent': random.choice(User_Agent)}


def get_proxy():
    pool = ProxyPool.get_proxy_pool()
    while pool:
        proxy = random.choice(pool)
        if ProxyPool.is_valid_proxy(proxy):
            return proxy
        ProxyPool.expire(proxy)
        pool = ProxyPool.get_proxy_pool()
    logging.error("@get_proxy Error: no available proxy.")


def request_data(url, retry=0, auto_proxy=False, delay=0, **kwargs):
    """
    Get请求爬取源代码
    :param url: 目标网站
    :param retry: 是否重试
    :param auto_proxy: 是否使用代理ip
    :param delay: 延迟时间
    :param kwargs: requests.get参数
    :return: text
    """
    if delay:
        time.sleep(delay)

    if retry:
        sess = requests.Session()
        sess.mount('http://', HTTPAdapter(max_retries=retry))
        sess.mount('https://', HTTPAdapter(max_retries=retry))
        method = functools.partial(sess.request, method='get')
    else:
        method = requests.get

    if auto_proxy:
        kwargs.update({
            'proxies': {'http': 'http://{}'.format(get_proxy())}
        })

    try:
        res = method(
            url=url,
            headers=get_header(),
            **kwargs)
        if res.status_code == 200:
            logging.debug("Request Data - {0} - {1}".format(
                res.status_code, url))
            return res.text

        logging.info("Request Data - {0} - {1}".format(res.status_code, url))
    except requests.exceptions.RequestException as e:
        logging.error("Request ERROR: {0}, url: {1}".format(e, url))


if __name__ == '__main__':
    # request_data(
    #     url='http://mail.163.com',
    #     retry=3,
    # )

    request_data(
        url='http://mail.163.com',
        auto_proxy=True,
        timeout=5,
    )

    # request_data(
    #     url='http://mail.163.com',
    #     retry=0,
    #     proxies={'http': 'http://39.137.69.7:80'}
    # )
