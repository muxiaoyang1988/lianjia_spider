# -*- coding: utf-8 -*-
import datetime
import re
import time
from warnings import filterwarnings

import requests
from bs4 import BeautifulSoup
from sqlalchemy import Column, String, Integer, DateTime
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

from settings import *

filterwarnings("ignore")
engine = create_engine(
    f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/proxy?charset=utf8',
    encoding='utf-8')
DBSession = scoped_session(sessionmaker(bind=engine))
Base = declarative_base()


class Proxy(Base):
    """ 存储代理池 """
    __tablename__ = 'proxy_ip'
    __table_args__ = {"mysql_charset": "utf8"}

    ip = Column(String(50), primary_key=True, comment='代理地址')
    is_valid = Column(Integer, comment='是否有效')
    cate = Column(String(20), comment='代理类型')
    level = Column(String(20), comment='隐私等级')
    source = Column(String(20), comment='代理来源')
    update_time = Column(DateTime, index=True, default=datetime.datetime.now)


class ProxyPool:
    """ 自定义代理池，使用mysql存储 """

    def __init__(self):
        Base.metadata.create_all(engine)

    @classmethod
    def is_valid_proxy(cls, proxy, cate='http', url='http://icanhazip.com', timeout=10):
        """ 验证代理ip是否有效 """
        host = proxy.split(':')[0]

        try:
            res = requests.get(
                url=url,
                proxies={cate: f'{cate}://{proxy}'},
                timeout=timeout
            )
            logging.info("Requests - {0} - {1}".format(res.status_code, url))
            if res.status_code != 200:
                return False
        except requests.exceptions.RequestException as e:
            logging.error("Requests ERROR: {0}, url: {1}".format(e, url))
            return False

        ip = res.text
        if not ip or len(ip) < len(host):
            return False
        return ip[:len(host)] == host

    def download_proxies_from_zdaye(self, pages=1, offset=0):
        session = DBSession()
        for page in range(offset + 1, pages + 1):
            src_url = f"https://www.zdaye.com/dayProxy/{page}.html"
            logging.info('@download_proxies_from_zdaye: page {0}/{1} - url - {2}'.format(page, pages, src_url))
            content = requests.get(src_url, verify=False).text
            soup = BeautifulSoup(content, 'lxml')

            for item_tag in soup.find_all("h3", class_="thread_title"):
                url = "https://www.zdaye.com" + item_tag.a.get('href')
                title = item_tag.a.get_text()
                logging.info("@download_proxies: {0} - {1}".format(url, title))

                code = requests.get(src_url, verify=False).text
                details = BeautifulSoup(code, 'lxml')
                for data in details.find("div", class_="cont").contents:
                    if '@HTTP' in data:
                        ip = data.split('@HTTP')[0]
                        level = re.search(r'\[.+\]', data)
                        level = level.group() if level[1:-1] else None
                        if not self.is_valid_proxy(ip):
                            continue
                        try:
                            query = session.query(Proxy).filter(Proxy.ip == ip)
                            if not query.first():
                                session.add(Proxy(
                                    ip=ip, is_valid=1, cate='http',
                                    level=level, source='zdaye'))
                                session.commit()
                                logging.info(
                                    '@download_proxies: host: {0}, level: {1}, source: zdaye'.format(ip, level))
                        except Exception as e:
                            session.rollback()
                            logging.exception('@download_proxies: ERROR: {0}, ip: {1}'.format(e, ip))
                    time.sleep(5)
        session.close()

    def add_proxy(self, proxy_list):
        """ 手动添加 """
        session = DBSession()
        for params in proxy_list:
            ip = params['ip']
            if not self.is_valid_proxy(ip):
                logging.info('@add_proxy invalid ip: {0}'.format(ip))
                continue
            params['is_valid'] = 1
            try:
                query = session.query(Proxy).filter(Proxy.ip == ip)
                if not query.first():
                    session.add(Proxy(**params))
                    session.commit()
                    logging.info(
                        '@add_proxy success - ip: {0}'.format(ip))
            except Exception as e:
                session.rollback()
                logging.exception('@add_proxy ERROR: {0}, ip: {1}'.format(e, ip))
        session.close()
        logging.info('@add_proxy finish.')

    @classmethod
    def get_proxy_pool(cls):
        session = DBSession()
        query = session.query(Proxy.ip) \
            .filter(Proxy.is_valid == 1, Proxy.cate == 'HTTP') \
            .order_by(Proxy.update_time.desc()) \
            .limit(50)
        session.commit()
        session.close()
        proxy_pool = [x[0] for x in query]
        logging.debug("@get_proxy_pool - success - count: {}".format(len(proxy_pool)))
        return proxy_pool

    @classmethod
    def expire(cls, ip):
        session = DBSession()
        session.query(Proxy) \
            .filter(Proxy.ip == ip) \
            .update({'is_valid': 0})
        session.commit()
        session.close()
        logging.info("@expire ip: {}".format(ip))

    @classmethod
    def enable(cls, ip):
        session = DBSession()
        session.query(Proxy) \
            .filter(Proxy.ip == ip) \
            .update({'is_valid': 1})
        session.commit()
        session.close()
        logging.info("@enable ip: {}".format(ip))

    @classmethod
    def batch_check(cls, limit=100, is_valid=1):
        session = DBSession()
        query = session.query(Proxy.ip) \
            .filter(Proxy.is_valid == is_valid) \
            .limit(limit)
        for q in query:
            if cls.is_valid_proxy(q[0]):
                cls.enable(q[0])
            else:
                cls.expire(q[0])


if __name__ == '__main__':
    p_pool = ProxyPool()

    # is_valid = p_pool.is_valid_proxy(proxy='139.196.93.30:8081')
    # print(is_valid)

    # p_pool.download_proxies_from_zdaye(pages=10)

    # ips = [
    #     {"ip:port": "39.106.223.134:80"},
    # ]
    # proxies = [{'ip': x['ip:port'], 'cate': 'HTTP', 'source': 'manual'} for x in ips]
    # p_pool.add_proxy(proxy_list=proxies)

    # p_pool.batch_check(limit=100, is_valid=0)

    # p_pool.expire('101.37.118.54:8888')
    # p_pool.enable('101.37.118.54:8888')

    # res = p_pool.get_proxy_pool()
    # print(res)
