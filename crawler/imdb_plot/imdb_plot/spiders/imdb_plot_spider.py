#coding=utf-8
import scrapy
from scrapy.spiders import CrawlSpider
from scrapy.selector import Selector
from scrapy.http import Request
from imdb_plot.items import ImdbPlotItem
import urllib
from sqlalchemy import Column, ForeignKey, Integer, String, Float, BIGINT, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class IMDB_Movie_Info(Base):
    __tablename__ = 'imdb_movie_info'

    id = Column(Integer, primary_key=True)
    title = Column(String(80))
    year = Column(String(80))
    certificate = Column(String(10))
    run_time = Column(String(20))
    genre = Column(String(80))
    summary = Column(Text)
    rating = Column(Float)
    rating_count = Column(Integer)
    gross = Column(BIGINT)
    actor = Column(String(80))
    serial = Column(String(20))

class imdb_plot(CrawlSpider):

    DIALCT = "mysql"
    DRIVER = "pymysql"
    USERNAME = "root"
    PASSWORD = ""
    HOST = "127.0.0.1"
    PORT = "3306"
    DATABASE = "imdb"
    DB_URI = "{}+{}://{}:{}@{}:{}/{}?charset=utf8" \
        .format(DIALCT, DRIVER, USERNAME, PASSWORD, HOST, PORT, DATABASE)
    engine = create_engine(DB_URI)
    Base.metadata.create_all(engine)
    sess = sessionmaker(bind=engine)
    session = sess()

    name = 'imdb_plot'
    start_urls = []

    movie_info = {}
    for i in range(1950, 1950+1):
        movie_serials = session.query(IMDB_Movie_Info.serial, IMDB_Movie_Info.id).filter_by(year=str(i))
        for serial in movie_serials:
            movie_info[serial.serial] = serial.id
            start_urls.append("https://www.imdb.com/title/" + str(serial.serial) + "/plotsummary")
    url = 'https://www.imdb.com'


    def parse(self, response):
        item = ImdbPlotItem()
        selector = Selector(response)
        summaries = selector.xpath('//ul[contains(@id, "plot-summaries-content")]/li')
        url = response.request.url.split("/")
        print("url is", url)
        for su in summaries:
            summary = su.xpath('p/text()').extract()
            summary = "".join(summary)



            item['summary'] = summary
            item['movie_id'] = imdb_plot.movie_info[url[4]]



            if "It looks like we don't have any Plot Summaries for this title yet." not in summary:
                yield item
