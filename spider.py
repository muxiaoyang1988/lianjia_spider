# -*- coding: utf-8 -*-
import json
import re
import time
import functools
from concurrent.futures import ThreadPoolExecutor, as_completed
from warnings import filterwarnings

from bs4 import BeautifulSoup

from model import SaleInfo, CommunityInfo, TransactionInfo, DBSession
from settings import logging
from utils import request_data

filterwarnings("ignore")


class LianJiaSpider:
    """
    链家二手房爬虫

    分为三个部分：
    1）在售房源：sale_info, 全量更新；
    2）小区信息：community_info，增量更新；
    3) 历史成交：transaction_info，增量更新；

    支持 - 城市选择：1个
    支持 - 通过指定区县爬取（粗粒度）；
    支持 - 通过搜索商圈或小区爬取（细粒度）；
    """

    def __init__(self, city, districts):
        self.base_url = f"http://{city}.lianjia.com/"
        self.city = city
        self.districts = districts

        self.bs4_parser = "lxml"
        self.max_workers = 3
        self.request_fn = functools.partial(
            request_data,
            retry=2,
            timeout=10,
            auto_proxy=False,
            delay=0.5
        )

    def set_request_params(self, max_workers, delay, retry=2, auto_proxy=False):
        """ 设置request参数 """
        self.max_workers = max_workers
        self.request_fn = functools.partial(
            request_data,
            retry=retry,
            timeout=10,
            auto_proxy=auto_proxy,
            delay=delay
        )

    def get_total_pages(self, url):
        """ 总页码数 """
        total_pages = 0
        content = self.request_fn(url)
        if not content:
            return total_pages

        soup = BeautifulSoup(content, self.bs4_parser)
        page_data = soup.find('div', class_='page-box house-lst-page-box').get('page-data')
        total_pages = json.loads(page_data).get('totalPage')

        return total_pages

    def parse_sale_content(self, item_tag):
        """ 在售房源列表 单条解析(含详情页) """
        info_dict = dict()

        # 导航页
        # 1. 标题
        title_tag = item_tag.find("div", class_='title')
        house_id = title_tag.a.get('data-housecode')
        link = title_tag.a.get('href')
        title = title_tag.a.text
        recommend_tag = title_tag.span.text if title_tag.span else None
        info_dict.update({
            'house_id': house_id,  # required
            'title': title,  # required
            'link': link,
            'recommend_tag': recommend_tag
        })

        # 2. 位置
        position_tag = item_tag.find("div", class_="positionInfo")
        community, biz_circle = position_tag.text.split('-')
        community_id = position_tag.find('a', {"data-el": "region"}).get('href').split('/')[-2]
        info_dict.update({
            'biz_circle': biz_circle.strip(),  # required
            'community': community.strip(),  # required
            'community_id': community_id,  # required
        })

        # 3. 房屋参数
        house_info = item_tag.find("div", class_="houseInfo").text.replace(' ', '').split('|')
        layout = house_info[0]
        area = float(house_info[1].strip('平米'))
        orient = house_info[2]
        decoration = house_info[3]
        floor_level = house_info[4].split('(')[0]
        search = re.search(r'\d+', house_info[4])
        total_floor = search.group() if search else None
        search = re.search(r'\d{4}', house_info[5])  # build_year
        build_year = search.group() if search else house_info[5]
        structure = house_info[6] if len(house_info) > 6 else None

        total_price = item_tag.find("div", class_="totalPrice").span.text
        unit_price = item_tag.find("div", class_="unitPrice").get('data-price')

        info_dict.update({
            'layout': layout,
            'area': area,
            'orient': orient,
            'decoration': decoration,
            'floor_level': floor_level,
            'total_floor': int(total_floor),
            'build_year': build_year,
            'structure': structure,
            'total_price': float(total_price),  # 总价(万)
            'unit_price': int(unit_price)  # 单价(元)
        })

        # 4. 特色标签
        tax_free_tag = item_tag.find("span", class_="taxfree")
        if not tax_free_tag:
            tax_free_tag = item_tag.find("span", class_="five")
        tax_free_tag = tax_free_tag.text if tax_free_tag else None
        subway_tag = item_tag.find("span", class_="subway")
        subway_tag = subway_tag.text if subway_tag else None
        info_dict.update({
            'tax_free_tag': tax_free_tag,
            'subway_tag': subway_tag,
        })

        # 详情页
        details = BeautifulSoup(self.request_fn(link), self.bs4_parser)

        # 1. 图片和位置
        image_info = details.find('ul', class_='smallpic')
        if image_info:
            top_image = image_info.find('li')
            top_image = top_image.get('data-src') if top_image else None
            layout_image = image_info.find('li', {'data-desc': '户型图'})
            layout_image = layout_image.get('data-src') if layout_image else None
            info_dict.update({
                'top_image': top_image,
                'layout_image': layout_image,
            })
        area_info = details.find('div', class_='areaName')
        if area_info:
            area_info = area_info.find('span', class_='info').text
            district_cn = area_info.split()[0]
            info_dict.update({'district': district_cn})
        follow = details.find('span', id='favCount', class_='count')
        follow = follow.text if follow else None
        info_dict.update({'follow': follow})

        # 2. 主要信息
        major_content = details.find('div', class_='introContent')
        base_info = major_content.find('div', class_='base')
        base_items = {x.contents[0].text: x.contents[1] for x in base_info.find_all('li')}
        transaction_info = major_content.find('div', class_='transaction')
        transaction_items = {x.contents[1].text: x.contents[3].text
                             for x in transaction_info.find_all('li')}
        base_items.update(transaction_items)
        base_key_map = {
            '上次交易': 'last_date',
            '交易权属': 'trans_auth',
            '产权所属': 'property_auth',
            '供暖方式': 'heating',
            '套内面积': 'inside_area',
            '建筑结构': 'material',
            '户型结构': 'duplex',
            '房屋年限': 'use_year',
            '房屋户型': 'layout',
            '房屋用途': 'usage',
            '房本备件': 'certificate',
            '抵押信息': 'mortgage',
            '挂牌时间': 'put_date',
            '梯户比例': 'ladder_ratio',
            '配备电梯': 'has_ladder'
        }
        base_result = {v: base_items.get(k) for k, v in base_key_map.items()}
        search = re.search(r'\d+\.?\d+', base_result['inside_area'])
        base_result['inside_area'] = float(search.group()) if search else 0  # 可能暂无数据
        if base_result['mortgage']:
            base_result['mortgage'] = base_result['mortgage'].strip('\n').strip()
        info_dict.update(base_result)

        return info_dict

    def parse_community_content(self, item_tag):
        """ 小区列表 单条解析(含详情页) """
        info_dict = dict()

        # 导航信息
        community_id = item_tag['data-id']
        link = item_tag.a['href']
        community_name = item_tag.find('div', class_='title').text.strip('\n')
        community_tag = item_tag.find('div', class_='tagList').text
        position_info = item_tag.find('div', class_='positionInfo')
        _district = position_info.find('a', class_='district').text
        biz_circle = position_info.find('a', class_='bizcircle').text
        info_dict.update({
            'id': community_id,
            'community': community_name,
            'tag': community_tag.strip('\n') if community_tag else None,
            'district': _district,
            'biz_circle': biz_circle,
            'link': link,
        })

        # 详情页
        details = BeautifulSoup(self.request_fn(link), self.bs4_parser)

        header_info = details.find('div', class_='xiaoquDetailHeader')
        if header_info:
            address = header_info.find('div', class_='detailDesc')
            follow = header_info.find('span', {'data-role': 'followNumber'})
            if address:
                info_dict.update({"address": address.text})
            if follow:
                info_dict.update({"follow": follow.text})

        describe = details.find('div', class_='xiaoquDescribe fr')
        if describe:
            # price
            price = describe.find('span', class_='xiaoquUnitPrice')
            price = price.contents[0] if price else None
            # base info
            base_info = describe.find('div', class_='xiaoquInfo')
            base_items = [x.contents[1].text for x in base_info.find_all('div', class_="xiaoquInfoItem")]
            search = re.search(r'\d+', base_items[0])
            year = search.group() if search else None
            structure = base_items[1]
            property_fee = base_items[2]
            property_company = base_items[3]
            developer = base_items[4]
            num_building = re.search(r'\d+', base_items[5]).group()
            num_household = re.search(r'\d+', base_items[6]).group()
            position = base_info.find('span', class_='actshowMap')
            if position:
                lng, lat = position.get('xiaoqu').split(',')
                lng, lat = float(lng.strip('[')), float(lat.strip(']'))
            else:
                lng, lat = None, None
            info_dict.update({
                'year': year,
                'structure': structure,
                'property_fee': property_fee,
                'property_company': property_company,
                'developer': developer,
                'num_building': int(num_building),
                'num_household': int(num_household),
                'lng': lng,
                'lat': lat,
            })
        return info_dict

    @classmethod
    def parse_transaction_content(cls, item_tag):
        """ 成交列表 单条解析 """

        info_dict = dict()

        title = item_tag.find('div', class_="title")
        title_info = title.text.split(' ')
        _community = title_info[0]
        layout = title_info[1] if len(title_info) > 1 else None
        area = title_info[2] if len(title_info) > 2 else '0'
        area = re.search(r'\d+', area)
        area = area.group() if area else None
        link = title.a.get('href')
        house_id = link.split('/')[-1].split('.')[0]
        info_dict.update({
            'house_id': house_id,
            # 'community_id': None,
            'community': _community,
            'layout': layout,
            'area': area,
            'link': link,
        })

        house_info = item_tag.find('div', class_="houseInfo").text.split('|')
        orient = house_info[0] if house_info else ''
        decoration = house_info[1] if len(house_info) > 1 else ''
        info_dict.update({
            'orient': orient.replace(' ', ''),
            'decoration': decoration.strip()
        })

        deal_date = item_tag.find('div', class_='dealDate').text.replace('.', '-')[:10]

        total_price = item_tag.find('div', class_='totalPrice').span
        if total_price:
            total_price = total_price.text
        if isinstance(total_price, str) and '-' in total_price:
            prices = [int(price) for price in total_price.split('-')]
            total_price = sum(prices) / 2

        unit_price = item_tag.find('div', class_='unitPrice').span
        if unit_price:
            unit_price = unit_price.text
        if isinstance(unit_price, str) and '-' in unit_price:
            prices = [int(price) for price in unit_price.split('-')]
            unit_price = sum(prices) / 2

        info_dict.update({
            'id': house_id + '_' + deal_date[:7],
            'deal_date': deal_date,
            'deal_price': total_price,
            'unit_price': unit_price
        })

        floor_info, building_info = item_tag.find('div', class_='positionInfo').text.split(' ')
        floor_level = floor_info.split('(')[0]
        search = re.search(r'\d+', floor_info)
        total_floor = search.group() if search else None
        if '年建' in building_info:
            building_info = building_info.split('年建')
            build_year = building_info[0]
            structure = building_info[1] if len(building_info) > 1 else None
        else:
            build_year = None
            structure = building_info
        info_dict.update({
            'floor_level': floor_level,
            'total_floor': total_floor,
            'build_year': build_year,
            'structure': structure
        })

        house_tag = item_tag.find('span', class_='dealHouseTxt')
        if house_tag:
            info_dict.update({
                'tax_free_tag': int('房屋满五年' in house_tag.text),
                'subway_tag': int('近地铁' in house_tag.text)
            })

        deal_info = item_tag.find('span', class_='dealCycleTxt')
        if deal_info:
            deal_info = deal_info.find_all('span')
            put_price = re.search(r'\d+', deal_info[0].text).group()
            info_dict.update({'put_price': put_price})
            if len(deal_info) > 1:
                deal_period = re.search(r'\d+', deal_info[1].text).group()
                info_dict.update({'deal_period': deal_period})

        return info_dict

    def crawl_sale_by_district(self, args):
        """ 根据区县爬取一页在售房源 """
        district, page = args
        url_page = self.base_url + f"ershoufang/{district}/pg{page}/"
        content = self.request_fn(url_page)
        soup = BeautifulSoup(content, self.bs4_parser)
        logging.debug('@crawl_sale_by_district: {0} - page - {1}: {2}'.format(district, page, url_page))

        session = DBSession()
        for ul_tag in soup.find_all("ul", class_="sellListContent"):
            for item_tag in ul_tag.find_all("li"):
                try:
                    info_dict = self.parse_sale_content(item_tag)
                    logging.debug('@crawl_sale_by_district: {0} - page - {1}: {2}'.format(district, page, info_dict))
                    sale_info = SaleInfo(**info_dict)
                    if sale_info.house_id and sale_info.community_id and sale_info.district:
                        session.add(sale_info)
                except Exception as e:
                    session.rollback()
                    logging.exception('@crawl_sale_by_district: {0} - page - {1}: {2}'.format(district, page, e))
                    time.sleep(3)

        session.commit()
        session.close()
        logging.info('@crawl_sale_by_page: {0} - page - {1} complete.'.format(district, page))

    def crawl_community_by_district(self, args):
        """ 根据区县爬取一页小区信息 """
        district, page = args
        url_page = self.base_url + f"xiaoqu/{district}/pg{page}/"
        content = self.request_fn(url_page)
        soup = BeautifulSoup(content, self.bs4_parser)
        logging.debug('@crawl_community_by_district: {0} - page - {1}: {2}'.format(district, page, url_page))

        session = DBSession()
        for ul_tag in soup.find_all("ul", class_="listContent"):
            for item_tag in ul_tag.find_all("li"):
                try:
                    info_dict = self.parse_community_content(item_tag)
                    query = session.query(CommunityInfo).filter(CommunityInfo.id == info_dict['id'])
                    if query.first():
                        query.update(info_dict)
                    else:
                        session.add(CommunityInfo(**info_dict))
                    session.commit()
                    logging.debug('@crawl_community_by_district: {0} - page - {1}: {2}'.format(district, page, info_dict))
                except Exception as e:
                    session.rollback()
                    logging.exception('@crawl_community_by_district: {0} - page - {1}: {2}'.format(district, page, e))
                    time.sleep(3)

        session.close()
        logging.info('@crawl_community_by_district: {0} - page - {1} complete.'.format(district, page))

    def crawl_district_pool(self, module, max_pages=100):
        """ 依据地区批量爬取 """

        crawl_mapper = {
            'sale_info': {
                'url': 'ershoufang',
                'func': self.crawl_sale_by_district
            },
            'community_info':  {
                'url': 'xiaoqu',
                'func': self.crawl_community_by_district
            },
        }
        url_prefix = crawl_mapper[module]['url']
        crawl_function = crawl_mapper[module]['func']

        for district in self.districts:
            url = self.base_url + f"{url_prefix}/{district}/"
            total_pages = self.get_total_pages(url)
            total_pages = min(total_pages, max_pages)
            logging.info("@crawl_{0}: total {1} pages found for {2}".format(
                module, total_pages, district))

            if not total_pages:
                logging.exception("@crawl_{0}: no pages found for {1}".format(
                    module, district))
                continue

            executor = ThreadPoolExecutor(max_workers=self.max_workers)
            args = [(district, page + 1) for page in range(total_pages)]
            all_task = [executor.submit(crawl_function, arg) for arg in args]
            for future in as_completed(all_task):
                future.result()

            logging.info("@crawl_{0}: {1} - all {2} pages complete.".format(
                module, district, total_pages))
            time.sleep(1)

    def crawl_sale_by_search(self, args):
        """ 根据商圈或社区爬取一页在售房源 """
        search_key, page = args
        url_page = self.base_url + f"ershoufang/pg{page}rs{search_key}/"
        content = self.request_fn(url_page)
        soup = BeautifulSoup(content, self.bs4_parser)
        logging.debug('@crawl_sale_by_search: {0} - page - {1}: {2}'.format(search_key, page, url_page))

        session = DBSession()
        for ul_tag in soup.find_all("ul", class_="sellListContent"):
            for item_tag in ul_tag.find_all("li"):
                try:
                    info_dict = self.parse_sale_content(item_tag)
                    logging.debug('@crawl_sale_by_search: {0} - page - {1}: {2}'.format(search_key, page, info_dict))
                    sale_info = SaleInfo(**info_dict)
                    if not sale_info.house_id or not sale_info.community_id or not sale_info.district:
                        continue
                    session.add(sale_info)
                except Exception as e:
                    session.rollback()
                    logging.exception('@crawl_sale_by_search: {0} - page - {1}: {2}'.format(search_key, page, e))
                    time.sleep(3)
        session.commit()
        session.close()
        logging.info('@crawl_sale_by_search: {0} - page - {1} complete.'.format(search_key, page))

    def crawl_transaction_by_search(self, args):
        """ 依据商圈或小区 爬取一页历史成交房源 """
        search_key, page = args
        url_page = self.base_url + f"chengjiao/pg{page}rs{search_key}/"
        content = self.request_fn(url_page)
        soup = BeautifulSoup(content, self.bs4_parser)
        logging.debug('@crawl_transaction_by_search: {0} - page - {1}: {2}'.format(search_key, page, url_page))

        session = DBSession()
        for ul_tag in soup.find_all("ul", class_="listContent"):
            for item_tag in ul_tag.find_all("li"):
                try:
                    info_dict = self.parse_transaction_content(item_tag)
                    query = session.query(TransactionInfo).filter(TransactionInfo.id == info_dict['id'])
                    if query.first():
                        query.update(info_dict)
                    else:
                        session.add(TransactionInfo(**info_dict))
                    session.commit()
                    logging.debug('@crawl_transaction_by_search: {0} - page - {1}: {2}'.format(
                        search_key, page, info_dict))
                except Exception as e:
                    logging.exception('@crawl_transaction_by_search: {0} - page - {1}: {2}'.format(
                        search_key, page, e))
                    time.sleep(3)

        logging.info('@crawl_transaction_by_search: {0} - page - {1} complete.'.format(search_key, page))

    def crawl_search_pool(self, module, collection, max_pages=100, coll_start=1):
        """ 依据商圈或小区批量爬取 """

        total_cnt = len(collection)
        logging.info("@crawl_{0}: total {1} found".format(module, total_cnt))

        crawl_mapper = {
            'sale_info': {
                'url': 'ershoufang',
                'func': self.crawl_sale_by_search
            },
            'transaction_info':  {
                'url': 'chengjiao',
                'func': self.crawl_transaction_by_search
            },
        }
        url_prefix = crawl_mapper[module]['url']
        crawl_function = crawl_mapper[module]['func']

        for i, search_key in enumerate(collection):

            # 指定开始，方便中断后继续爬取
            if i + 1 < coll_start:
                continue

            url = self.base_url + f"{url_prefix}/rs{search_key}/"
            total_pages = self.get_total_pages(url)
            total_pages = min(total_pages, max_pages)
            logging.info("@crawl_{0}: {1}/{2} - {3} - total {4} pages found.".format(
                module, i + 1, total_cnt, search_key, total_pages))
            if not total_pages:
                continue

            executor = ThreadPoolExecutor(max_workers=self.max_workers)
            args = [(search_key, page + 1) for page in range(total_pages)]
            all_task = [executor.submit(crawl_function, arg) for arg in args]
            for future in as_completed(all_task):
                future.result()
            logging.info("@crawl_{0}: {1}/{2} - {3} - all {4} pages complete.".format(
                module, i + 1, total_cnt, search_key, total_pages))
            time.sleep(1)

    @classmethod
    def query_biz_circle(cls, districts):
        """ 查商圈 """
        session = DBSession()
        query = session.query(CommunityInfo.biz_circle) \
            .filter(CommunityInfo.district.in_(districts)) \
            .all()
        session.commit()
        session.close()
        result = list(set([x[0] for x in query]))
        result.sort()
        return result

    @classmethod
    def query_community(cls, districts=None, biz_circle=None):
        """ 查小区 """
        session = DBSession()
        if districts:
            query = session.query(CommunityInfo.community) \
                .filter(CommunityInfo.district.in_(districts)) \
                .all()
        elif biz_circle:
            query = session.query(CommunityInfo.community) \
                .filter(CommunityInfo.biz_circle.in_(biz_circle)) \
                .all()
        else:
            query = [[]]
            logging.exception("@query_community: query condition un-defined.")
        session.commit()
        session.close()
        result = list(set([x[0] for x in query]))
        result.sort()
        return result
