from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database_structure import IMDB_Movie_Summary, IMDB_Movie_Info, IMDB_Index_Data, IMDB_Index_Data_2, IMDB_Summary_Index_Data, IMDB_Info_Index_Data
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer
import json
from collections import Counter
from operator import itemgetter
import timeit
import setting
Base = declarative_base()

class Analyzer:

    def __init__(self):
        engine = create_engine(setting.DB_URI)
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
            # .filter(IMDB_Movie_Info.year >= self.year[0], IMDB_Movie_Info.year <= self.year[1])

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

    def symbol_processing(self, str):
        result = []
        for word in str.split(" "):
            result.append(word)
            for symbol in self.ignore_set:
                if symbol in word:
                    sub_word = word.split(symbol)
                    for sub_str in sub_word:
                        result.append(sub_str)
                    result.append("".join(sub_word))
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

    def update_database(self, data: {}, data_type):
        index = {}

        # words = self.session.query(IMDB_Index_Data)
        # start = timeit.default_timer()
        # total = words.count()
        # n = 1
        #
        # for word in words:
        #     index[word.word] = json.loads(word.document_id)
        #
        #     stop = timeit.default_timer()
        #     print("Initialize-Index-Progress: ", round(n / total * 100, 2), "%", "   Used Time: ",
        #           round(stop - start, 2), "   Estimate Remain Time: ",
        #           round(100 / (n / total * 100) * (stop - start) - (stop - start), 2),
        #           " seconds")
        #     self.timer['Initialize-Index: '] = round(stop - start, 2)
        #
        #     n += 1

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
            if data_type == 'summary':
                word_data = IMDB_Summary_Index_Data(word=word, document_id=json.dumps(document_id))
                t = 'Update-Summary-Database'
            elif data_type == 'info':
                word_data = IMDB_Info_Index_Data(word=word, document_id=json.dumps(document_id))
                t = 'Update-Info-Database'

            self.session.add(word_data)

            stop = timeit.default_timer()
            print("Update-Database-Progress: ", round(n / total * 100, 2), "%", "   Used Time: ",
                  round(stop - start, 2), "   Estimate Remain Time: ",
                  round(100 / (n / total * 100) * (stop - start) - (stop - start), 2),
                  " seconds")
            n += 1
            self.timer[t + ': '] = round(stop - start, 2)

        self.session.commit()

        stop = timeit.default_timer()
        self.timer[t +'-Total: '] = round(stop - start, 2)

    def convert_genre(self, genre):
        if genre == "Sci-Fi":
            return "Science Fiction"
        elif genre == "Game-Show":
            return "Game Show"
        elif genre == "Talk-Show":
            return "Talk Show"
        elif genre == "Reality-TV":
            return "Reality TV"
        else:
            return genre

    def analyze(self):
        summary = {}
        info = {}
        datas = self.get_movie_data()
        start = timeit.default_timer()
        total = datas.count()

        n = 1
        for data in datas:
            sw = self.stop_word(data.summary)
            st = self.stemming(sw)
            sp = self.symbol_processing(st)
            summary_dict = self.process_with_tf(sp)

            sw = self.stop_word(data.title)
            st = self.stemming(sw)
            sp = self.symbol_processing(st)
            title_dict = self.process_with_tf(sp)

            genre = self.convert_genre(data.genre)
            sw = self.stop_word(genre)
            st = self.stemming(sw)
            sp = self.symbol_processing(st)
            genre_dict = self.process_with_tf(sp)

            sw = self.stop_word(data.year)
            st = self.stemming(sw)
            sp = self.symbol_processing(st)
            year_dict = self.process_with_tf(sp)

            sw = self.stop_word(data.actor)
            st = self.stemming(sw)
            sp = self.symbol_processing(st)
            actor_dict = self.process_with_tf(sp)

            movie_info_dict = dict(Counter(title_dict) + Counter(genre_dict) + Counter(year_dict) + Counter(actor_dict))

            # finished_dict = dict(Counter(summary_dict) + Counter(movie_info_dict))
            for key, value in summary_dict.items():
                if key not in summary:
                    summary[key] = {}
                if data.id not in summary[key]:
                    summary[key][data.id] = value

            for key, value in movie_info_dict.items():
                if key not in info:
                    info[key] = {}
                if data.id not in info[key]:
                    info[key][data.id] = value
            stop = timeit.default_timer()
            print("Pre-Processing-Progress: ", round(n / total * 100, 2), "%", "   Used Time: ", round(stop - start, 2),
                  "   Estimate Remain Time: ",
                  round(100 / (n / total * 100) * (stop - start) - (stop - start), 2),
                  " seconds")
            n += 1

            self.timer['Pre-Processing: '] = round(stop - start, 2)
        self.update_database(summary, 'summary')
        self.update_database(info, 'info')

    def print_timer(self):
        for key, value in self.timer.items():
            print(key, value, " seconds")
if __name__ == '__main__':
    analyzer = Analyzer()
    analyzer.year = [1951, 2018]
    analyzer.analyze()
    analyzer.print_timer()

