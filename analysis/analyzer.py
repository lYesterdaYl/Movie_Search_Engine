from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database_structure import IMDB_Movie_Summary, IMDB_Movie_Info
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer


Base = declarative_base()


class Analyzer:

    def __init__(self):
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
        self.session = sess()
        self.year = [1950, 2019]

    def get_summary_data(self):
        summary_data = self.session.query(IMDB_Movie_Summary.summary).join(IMDB_Movie_Info, IMDB_Movie_Summary.movie_id==IMDB_Movie_Info.id).filter(IMDB_Movie_Info.year>self.year[0], IMDB_Movie_Info.year<self.year[1])

        return summary_data

    def stop_word(self, str):
        result = {}
        stopWords = set(stopwords.words('english'))
        for word in word_tokenize(str):
            if word not in stopWords:
                result.append(word)
        return  "".join(result)

    def stemming(self, str):
        result = []
        ps = PorterStemmer()
        for word in word_tokenize(str):
            result.append(ps.stem(word))
        return result

    def analyze(self):
        pass