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
import timeit

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

    def get_movie_data(self):
        summary_data = self.session\
            .query(IMDB_Movie_Summary.summary, IMDB_Movie_Info.id, IMDB_Movie_Info.title
                   , IMDB_Movie_Info.actor, IMDB_Movie_Info.genre, IMDB_Movie_Info.year)\
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
        start = timeit.default_timer()
        total = len(data)
        n = 1
        for word, tf in data.items():
            stop = timeit.default_timer()
            print("Update-Database-Progress: ", round(n/total*100, 2), "%", "   Used Time: ", round(stop - start, 2), "   Estimate Remain Time: ", round(100/round(n/total*100, 3)*round(stop-start, 3) - round(stop - start, 3), 2), " seconds")
            word_data = self.session.query(IMDB_Index_Data).filter_by(word=word).first()
            if word_data is None:
                tf = dict(sorted(tf.items(), key=itemgetter(1), reverse=True))
                self.session.add(IMDB_Index_Data(word=word, document_id=json.dumps(tf)))
            else:
                word_data.document_id = dict(Counter(json.loads(word_data.document_id)) + Counter(tf))
                word_data.document_id = json.dumps(dict(sorted(word_data.document_id.items(), key=itemgetter(1), reverse=True)))
                self.session.add(word_data)
            n += 1
        self.session.commit()


    def analyze(self):
        result = {}
        datas = self.get_movie_data()
        start = timeit.default_timer()
        total = datas.count()

        n = 1
        for data in datas:
            stop = timeit.default_timer()
            print("Pre-Processing-Progress: ", round(n / total * 100, 2), "%", "   Used Time: ", round(stop - start, 2),
                  "   Estimate Remain Time: ",
                  round(100 / round(n / total * 100, 3) * round(stop - start, 3) - round(stop - start, 3), 2),
                  " seconds")
            sw = self.stop_word(data.summary)
            st = self.stemming(sw)
            summary_dict = self.process_with_tf(st)

            sw = self.stop_word(data.title)
            st = self.stemming(sw)
            title_dict = self.process_with_tf(st)

            sw = self.stop_word(data.genre)
            st = self.stemming(sw)
            genre_dict = self.process_with_tf(st)

            sw = self.stop_word(data.year)
            st = self.stemming(sw)
            year_dict = self.process_with_tf(st)

            sw = self.stop_word(data.actor)
            st = self.stemming(sw)
            actor_dict = self.process_with_tf(st)

            movie_info_dict = dict(Counter(title_dict) + Counter(genre_dict) + Counter(year_dict) + Counter(actor_dict))
            for item in movie_info_dict:
                movie_info_dict[item] += 100


            finished_dict = dict(Counter(summary_dict) + Counter(movie_info_dict))
            for key, value in finished_dict.items():

                if key not in result:
                    result[key] = {}
                if data.id not in result[key]:
                    result[key][data.id] = value
            n += 1
        self.update_database(result)

if __name__ == '__main__':
    analyzer = Analyzer()
    analyzer.year = [1951, 1951]
    analyzer.analyze()

