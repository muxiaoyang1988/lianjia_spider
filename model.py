# -*- coding: utf-8 -*-
import datetime
from sqlalchemy import Column, String, Integer, Numeric, Float, DateTime, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from settings import *

Base = declarative_base()


class SaleInfo(Base):
    """ 在售房源表 """
    __tablename__ = 'sale_info'
    __table_args__ = {"mysql_charset": "utf8"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    house_id = Column(String(20), nullable=False, index=True, comment='链家房源ID')
    title = Column(String(50), nullable=False, comment='房源标题')

    district = Column(String(20), comment='区县')
    biz_circle = Column(String(20), nullable=False, comment='商圈')
    community = Column(String(20), nullable=False, comment='小区')
    community_id = Column(String(20), nullable=False, index=True, comment='小区ID')

    put_date = Column(String(10), comment='挂牌时间')
    total_price = Column(Integer, nullable=False, comment='总价(万)')
    unit_price = Column(Integer, nullable=False, comment='单价(元)')
    area = Column(Float, nullable=False, comment='建筑面积')
    inside_area = Column(Float, comment='套内面积')
    tax_free_tag = Column(String(10), comment='满五免税标签')
    subway_tag = Column(String(10), comment='临近地铁标签')
    recommend_tag = Column(String(10), comment='推荐标签')

    layout = Column(String(20), comment='户型')
    orient = Column(String(10), comment='朝向')
    decoration = Column(String(10), comment='装修')
    floor_level = Column(String(10), comment='楼层高度')
    total_floor = Column(Integer, comment='总楼层')
    build_year = Column(String(4), comment='修建年份')
    structure = Column(String(10), comment='建筑类型')
    has_ladder = Column(String(10), comment='配备电梯')
    ladder_ratio = Column(String(10), comment='梯户比')
    heating = Column(String(10), comment='供暖方式')
    trans_auth = Column(String(10), comment='交易权属')
    property_auth = Column(String(10), comment='产权权属')
    duplex = Column(String(10), comment='户型结构')
    material = Column(String(10), comment='建筑结构')
    usage = Column(String(10), comment='房屋用途')
    use_year = Column(String(10), comment='房屋年限')
    last_date = Column(String(10), comment='上次交易时间')
    mortgage = Column(String(50), comment='抵押情况')
    certificate = Column(String(50), comment='房本情况')
    follow = Column(Integer, comment='关注人数')

    link = Column(String(200), comment='详情页链接')
    top_image = Column(String(200), comment='首页实景图')
    layout_image = Column(String(200), comment='户型图')

    create_time = Column(DateTime, default=datetime.datetime.now, comment='创建时间')


class CommunityInfo(Base):
    """ 社区表 """
    __tablename__ = 'community_info'
    __table_args__ = {"mysql_charset": "utf8"}

    id = Column(String(20), nullable=False, primary_key=True, comment='链家社区ID')
    community = Column(String(20), nullable=False, index=True, comment='小区名')
    district = Column(String(20), nullable=False, comment='区县')
    biz_circle = Column(String(20), nullable=False, comment='商圈')
    address = Column(String(100), comment='街道地址')

    tag = Column(String(20), comment='首页标签')
    price = Column(Numeric, comment='小区均价')
    year = Column(String(4), comment='修建年份')
    structure = Column(String(20), comment='建筑类型')
    property_fee = Column(String(20), comment='物业费')
    property_company = Column(String(50), comment='物业公司')
    developer = Column(String(50), comment='开发商')
    num_building = Column(Integer, comment='总楼栋数')
    num_household = Column(Integer, comment='总户数')
    lng = Column(Float, comment='经度')
    lat = Column(Float, comment='纬度')
    follow = Column(Integer, comment='关注人数')
    link = Column(String(100), comment='详情页链接')

    create_time = Column(DateTime, default=datetime.datetime.now, comment='创建时间')


class TransactionInfo(Base):
    """ 历史成交纪录表 """
    __tablename__ = 'transaction_info'
    __table_args__ = {"mysql_charset": "utf8"}

    id = Column(String(30), primary_key=True)
    house_id = Column(String(20), nullable=False, index=True, comment='链家房源ID')
    # community_id = Column(String(20), index=True, comment='链家社区ID')
    community = Column(String(20), index=True, comment='小区')

    deal_date = Column(String(10), comment='成交时间')
    deal_price = Column(Numeric, comment='成交价(万)')
    put_price = Column(Numeric, comment='挂牌价(万)')
    unit_price = Column(Integer, comment='单价(元)')
    layout = Column(String(20), comment='户型')
    area = Column(Numeric, comment='建筑面积')
    orient = Column(String(10), comment='朝向')
    decoration = Column(String(10), comment='装修')
    floor_level = Column(String(10), comment='楼层高度')
    total_floor = Column(Integer, comment='总楼层')
    build_year = Column(String(4), comment='修建年份')
    structure = Column(String(10), comment='建筑类型')
    tax_free_tag = Column(String(10), comment='满五免税标签')
    subway_tag = Column(String(10), comment='临近地铁标签')
    deal_period = Column(Integer, comment='成交周期(天)')
    link = Column(String(100), comment='详情页链接')
    create_time = Column(DateTime, default=datetime.datetime.now, comment='创建时间')


engine = create_engine(
    f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8',
    encoding='utf-8')
DBSession = scoped_session(sessionmaker(bind=engine))


def init_db():
    Base.metadata.create_all(engine)


def drop_db():
    Base.metadata.drop_all(engine)


if __name__ == '__main__':
    drop_db()
    init_db()
