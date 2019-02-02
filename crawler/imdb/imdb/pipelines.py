# -*- coding: utf-8 -*-
from sqlalchemy import Column, ForeignKey, Integer, String, Float, BIGINT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


class IMDB_Movie_Info(Base):
    __tablename__ = 'imdb_movie_info'

    id = Column(Integer, primary_key=True)
    title = Column(String(80), nullable=False)
    certificate = Column(String(10))
    run_time = Column(String(10))
    genre = Column(String(80), nullable=False)
    summary = Column(String(255), nullable=False)
    rating = Column(Float)
    rating_count = Column(Integer)
    gross = Column(BIGINT)
    actor = Column(String(255), nullable=False)
    serial = Column(String(20))


class ImdbPipeline(object):
    def __init__(self):
        DIALCT = "mysql"
        DRIVER = "pymysql"
        USERNAME = "root"
        PASSWORD = ""
        HOST = "127.0.0.1"
        PORT = "3306"
        DATABASE = "item_catalog"
        DB_URI = "{}+{}://{}:{}@{}:{}/{}?charset=utf8" \
            .format(DIALCT, DRIVER, USERNAME, PASSWORD, HOST, PORT, DATABASE)
        self.engine = create_engine(DB_URI)
        Base.metadata.create_all(self.engine)
        self.sess = sessionmaker(bind=self.engine)
        self.session = self.sess()

    def process_item(self, item, spider):
        self.session.add(IMDB_Movie_Info(**item))
        self.session.commit()

        return item

    def close_spider(self, spider):
        self.session.close()
