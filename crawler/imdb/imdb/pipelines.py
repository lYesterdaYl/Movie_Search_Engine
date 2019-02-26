# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from sqlalchemy import Column, ForeignKey, Integer, String, Float, BIGINT, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

import timeit

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

class ImdbPipeline(object):
    def __init__(self):
        DIALCT = "mysql"
        DRIVER = "pymysql"
        USERNAME = "root"
        PASSWORD = "root"
        HOST = "127.0.0.1"
        PORT = "3306"
        DATABASE = "imdb"
        DB_URI = "{}+{}://{}:{}@{}:{}/{}?charset=utf8" \
            .format(DIALCT, DRIVER, USERNAME, PASSWORD, HOST, PORT, DATABASE)
        self.engine = create_engine(DB_URI)
        Base.metadata.create_all(self.engine)
        self.sess = sessionmaker(bind=self.engine)
        self.session = self.sess()



    def process_item(self, item, spider):
        movie = self.session.query(IMDB_Movie_Info.id).filter_by(serial=item['serial']).first()
        if movie is None:
            self.session.add(IMDB_Movie_Info(**item))
            self.session.commit()

        return item


    def close_spider(self, spider):
        self.session.close()
        stop = timeit.default_timer()
        print('Time: ', stop - start)