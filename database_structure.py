
from sqlalchemy import Column, ForeignKey, Integer, String, Float, BIGINT, Text
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine



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


class IMDB_Movie_Summary(Base):
    __tablename__ = 'imdb_movie_summary'

    id = Column(Integer, primary_key=True)
    summary = Column(Text)
    imdb_movie_info = relationship(IMDB_Movie_Info)
    movie_id = Column(Integer, ForeignKey('imdb_movie_info.id'))

class IMDB_Index_Data(Base):
    __tablename__ = 'imdb_movie_index'

    id = Column(Integer, primary_key=True)
    word = Column(String(50))
    document_id = Column(LONGTEXT)

class IMDB_Index_Data_2(Base):
    __tablename__ = 'imdb_movie_index_2'

    id = Column(Integer, primary_key=True)
    word = Column(String(50))
    document_id = Column(LONGTEXT)

DIALCT = "mysql"
DRIVER = "pymysql"
USERNAME = "root"
PASSWORD = ""
HOST = "127.0.0.1"
PORT = "3306"
DATABASE = "imdb_test"
DB_URI = "{}+{}://{}:{}@{}:{}/{}?charset=utf8" \
    .format(DIALCT, DRIVER, USERNAME, PASSWORD, HOST, PORT, DATABASE)
engine = create_engine(DB_URI)
Base.metadata.create_all(engine)

