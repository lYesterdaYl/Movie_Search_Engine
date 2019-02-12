from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database_structure import IMDB_Movie_Summary, IMDB_Movie_Info, IMDB_Index_Data
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer
import json
from collections import Counter
from operator import itemgetter


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
        summary_data = self.session\
            .query(IMDB_Movie_Summary.summary, IMDB_Movie_Info.id)\
            .join(IMDB_Movie_Info, IMDB_Movie_Summary.movie_id == IMDB_Movie_Info.id)\
            .filter(IMDB_Movie_Info.year >= self.year[0], IMDB_Movie_Info.year <= self.year[1])

        return summary_data

    def stop_word(self, str):
        result = []
        stopWords = set(stopwords.words('english'))
        for word in word_tokenize(str):
            if word not in stopWords:
                result.append(word)
        return " ".join(result)

    def stemming(self, str):
        result = []
        ps = PorterStemmer()
        for word in word_tokenize(str):
            result.append(ps.stem(word))
        return " ".join(result)

    def process_with_tf(self, str):
        result = {}
        for word in word_tokenize(str):
            if word not in result:
                result[word] = 1
            elif word in result:
                result[word] += 1
        return result

    def update_database(self, data: {}):
        for word, tf in data.items():

            word_data = self.session.query(IMDB_Index_Data).filter_by(word=word).first()
            if word_data is None:
                tf = dict(sorted(tf.items(), key=itemgetter(1), reverse=True))
                self.session.add(IMDB_Index_Data(word=word, document_id=json.dumps(tf)))
            else:
                print("Word data = ", str(word_data))
                print(word)
                print(tf)
                document_id = json.loads(word_data.document_id)
                print(document_id)
                word_data.document_id = dict(Counter(json.loads(word_data.document_id)) + Counter(tf))
                word_data.document_id = json.dumps(dict(sorted(word_data.document_id.items(), key=itemgetter(1), reverse=True)))
                self.session.add(word_data)
        self.session.commit()


    def analyze(self):
        result = {}
        datas = self.get_summary_data()
        for data in datas:
            sw = self.stop_word(data.summary)
            st = self.stemming(sw)
            finished_dict = self.process_with_tf(st)
            for key, value in finished_dict.items():
                if key not in result:
                    result[key] = {}
                if data.id not in result[key]:
                    result[key][data.id] = value
        print(result)
        self.update_database(result)

if __name__ == '__main__':
    analyzer = Analyzer()
    analyzer.year = [1950, 1950]
    analyzer.analyze()

