from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database_structure import IMDB_Movie_Summary, IMDB_Movie_Info, IMDB_Index_Data, IMDB_Index_Data_2
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
        DATABASE = "imdb_test"
        DB_URI = "{}+{}://{}:{}@{}:{}/{}?charset=utf8" \
            .format(DIALCT, DRIVER, USERNAME, PASSWORD, HOST, PORT, DATABASE)
        engine = create_engine(DB_URI)
        Base.metadata.create_all(engine)
        sess = sessionmaker(bind=engine)
        self.session = sess()
        self.year = [1950, 2019]
        self.ignore_set = {',', '.', ':', '?', "'", "(", ")", "-", '_', '/', '|'}
        self.timer = {}

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
            if word not in self.ignore_set:
                if word not in result:
                    result[word] = 1
                elif word in result:
                    result[word] += 1
        return result

    def update_database(self, data: {}):
        index = {}
        words = self.session.query(IMDB_Index_Data)
        start = timeit.default_timer()
        total = words.count()
        n = 1

        for word in words:
            index[word.word] = json.loads(word.document_id)

            stop = timeit.default_timer()
            print("Initialize-Index-Progress: ", round(n / total * 100, 2), "%", "   Used Time: ",
                  round(stop - start, 2), "   Estimate Remain Time: ",
                  round(100 / (n / total * 100) * (stop - start) - (stop - start), 2),
                  " seconds")
            self.timer['Initialize-Index: '] = round(stop - start, 2)

            n += 1

        start = timeit.default_timer()
        total = len(data)
        n = 1
        for word, tf in data.items():
            if word not in index:
                tf = dict(sorted(tf.items(), key=itemgetter(1), reverse=True))
                index[word] = tf
            else:
                for key, value in tf.items():
                    if key not in index[word]:
                        index[word][key] = value
                    else:
                        index[word][key] += value

            stop = timeit.default_timer()
            print("Update-Index-Progress: ", round(n / total * 100, 2), "%", "   Used Time: ",
                  round(stop - start, 2), "   Estimate Remain Time: ",
                  round(100 / (n / total * 100) * (stop - start) - (stop - start), 2),
                  " seconds")
            self.timer['Update-Index: '] = round(stop - start, 2)

            n += 1

        start = timeit.default_timer()
        total = len(index)
        n = 1
        for word, document_id in index.items():
            # word_data = self.session.query(IMDB_Index_Data).filter_by(word=word).first()
            # word_data.document_id = json.dumps(document_id)
            # self.session.add(word_data)

            word_data = IMDB_Index_Data_2(word=word, document_id=json.dumps(document_id))
            self.session.add(word_data)

            stop = timeit.default_timer()
            print("Update-Database-Progress: ", round(n / total * 100, 2), "%", "   Used Time: ",
                  round(stop - start, 2), "   Estimate Remain Time: ",
                  round(100 / (n / total * 100) * (stop - start) - (stop - start), 2),
                  " seconds")
            n += 1
            self.timer['Update-Database: '] = round(stop - start, 2)

        self.session.commit()

        stop = timeit.default_timer()
        self.timer['Update-Database-Total: '] = round(stop - start, 2)


    def analyze(self):
        result = {}
        datas = self.get_movie_data()
        start = timeit.default_timer()
        total = datas.count()

        n = 1
        for data in datas:
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

            stop = timeit.default_timer()
            print("Pre-Processing-Progress: ", round(n / total * 100, 2), "%", "   Used Time: ", round(stop - start, 2),
                  "   Estimate Remain Time: ",
                  round(100 / (n / total * 100) * (stop - start) - (stop - start), 2),
                  " seconds")
            n += 1

            self.timer['Pre-Processing: '] = round(stop - start, 2)
        self.update_database(result)

    def print_timer(self):
        for key, value in self.timer.items():
            print(key, value, " seconds")
if __name__ == '__main__':
    analyzer = Analyzer()
    analyzer.year = [1981, 2018]
    analyzer.analyze()
    analyzer.print_timer()

