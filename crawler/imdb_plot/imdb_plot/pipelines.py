# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from sqlalchemy import Column, ForeignKey, Integer, String, Float, BIGINT, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
# from imdb_plot.spiders.imdb_plot_spider import IMDB_Movie_Info
import timeit

Base = declarative_base()


start = timeit.default_timer()

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


class IMDB_Movie_Summary(Base):
    __tablename__ = 'imdb_movie_summary'

    id = Column(Integer, primary_key=True)
    summary = Column(Text)
    imdb_movie_info = relationship(IMDB_Movie_Info)
    movie_id = Column(Integer, ForeignKey('imdb_movie_info.id'))

class ImdbPlotPipeline(object):
    def __init__(self):
        DIALCT = "mysql"
        DRIVER = "pymysql"
        USERNAME = "root"
        PASSWORD = "root"
        HOST = "127.0.0.1"
        PORT = "3306"
        DATABASE = "imdb_test"
        DB_URI = "{}+{}://{}:{}@{}:{}/{}?charset=utf8" \
            .format(DIALCT, DRIVER, USERNAME, PASSWORD, HOST, PORT, DATABASE)
        self.engine = create_engine(DB_URI)
        Base.metadata.create_all(self.engine)
        self.sess = sessionmaker(bind=self.engine)
        self.session = self.sess()



    def process_item(self, item, spider):
        self.session.add(IMDB_Movie_Summary(**item))
        self.session.commit()

        return item


    def close_spider(self, spider):
        self.session.close()
        stop = timeit.default_timer()
        print('Time: ', stop - start)
