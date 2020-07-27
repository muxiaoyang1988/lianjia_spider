from settings import logging
from model import init_db, drop_db
from spider import LianJiaSpider


CITY = 'bj'  # only one
DISTRICTS = ['changping', 'haidian', 'chaoyang', 'dongcheng', 'xicheng', 'fengtai', 'shijingshan']  # pinyin
DISTRICTS_CN = ['昌平', '海淀', '朝阳', '东城', '西城', '丰台', '石景山']


def run_spider():
    """
    运行爬虫
    :return:
    """
    # drop_db()
    init_db()

    logging.info("Spider start ... city: {0}, districts: {1}".format(CITY, ','.join(DISTRICTS)))
    spider = LianJiaSpider(
        city=CITY,
        districts=DISTRICTS,
    )

    # 爬取所有小区信息（首次必须）
    spider.crawl_district_pool(module='community_info')

    # 爬取在售房源: 3种方式
    # 1. 按照地区爬取
    # spider.crawl_district_pool(module='sale_info')
    # 2. 按照商圈爬取（推荐）
    biz_circles = spider.query_biz_circle(districts=DISTRICTS_CN)
    spider.set_request_params(max_workers=3, delay=0.5)  # 限速
    spider.crawl_search_pool(module='sale_info', collection=biz_circles, coll_start=1)
    # 3. 按照社区爬取
    # communities = spider.query_community(biz_circle=biz_circles)
    # spider.crawl_search_pool(module='sale_info', collection=communities)

    # 爬取历史成交
    spider.set_request_params(max_workers=1, delay=3)  # 限速
    spider.crawl_search_pool(module='transaction_info', collection=biz_circles, coll_start=1)
    # spider.crawl_search_pool(module='transaction_info', collection=communities)

    logging.info("Spider finished ...")


if __name__ == '__main__':
    run_spider()
