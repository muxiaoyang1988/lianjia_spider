# -*- coding: utf-8 -*-
import warnings
from unittest import TestCase
from spider import LianJiaSpider


class TestSpider(TestCase):

    def setUp(self):
        self.spider = LianJiaSpider(city="bj", districts=['haidian'])
        warnings.simplefilter("ignore", ResourceWarning)

    def test_get_total_pages(self):
        url = "http://bj.lianjia.com/ershoufang/bj/chaoyang/"
        total_pages = self.spider.get_total_pages(url=url)
        print(total_pages)
        self.assertEqual(type(total_pages), int)

    def test_crawl_sale_by_district(self):
        self.spider.crawl_sale_by_district(args=['daxing', 1])

    def test_crawl_sale_by_district_pool(self):
        self.spider.crawl_district_pool(module='sale_info', max_pages=3)

    def test_crawl_community_by_district(self):
        self.spider.crawl_community_by_district(args=['daxing', 1])

    def test_crawl_community_by_district_pool(self):
        self.spider.crawl_district_pool(module='community_info', max_pages=3)

    def test_crawl_sale_by_search(self):
        self.spider.crawl_sale_by_search(args=['新龙城', 1])

    def test_crawl_sale_by_search_pool(self):
        collection = self.spider.query_community(biz_circle=['中关村', '五道口'])
        self.spider.crawl_search_pool(module='sale_info', collection=collection, max_pages=3)

    def test_crawl_transaction_by_search(self):
        self.spider.crawl_transaction_by_search(args=['新龙城', 1])

    def test_crawl_transaction_by_search_pool(self):
        collection = self.spider.query_community(biz_circle=['中关村', '五道口'])
        self.spider.crawl_search_pool(module='transaction_info', collection=collection, max_pages=3)

    def test_query_biz_circle(self):
        res = self.spider.query_biz_circle(districts=['大兴', '海淀'])
        print(res)

    def test_query_community(self):
        res = self.spider.query_community(districts=['大兴', '海淀'])
        print(res)
        res = self.spider.query_community(biz_circle=['中关村', '五道口'])
        print(res)
