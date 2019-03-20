from flask import Flask, render_template, request, redirect, \
    url_for, flash, jsonify, make_response
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from database_structure import IMDB_Index_Data, IMDB_Movie_Info, Base, IMDB_Info_Index_Data, IMDB_Summary_Index_Data
import json
from analysis.analyzer_3 import Analyzer
from collections import Counter
from operator import itemgetter
from collections import OrderedDict
import setting
from spellchecker import SpellChecker
import movie_data_formator

import timeit

app = Flask(__name__)

APPLICATION_NAME = "Movie Search App"
app.config['SQLALCHEMY_DATABASE_URI'] = setting.DB_URI
app.config['SQLALCHEMY_POOL_SIZE'] = 100
app.config['SQLALCHEMY_POOL_RECYCLE'] = 3600
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
session = db.session
# engine = create_engine(setting.DB_URI, pool_size=100,pool_recycle=3600)
#
# Base.metadata.bind = engine
#
# DBSession = sessionmaker(bind=engine)
# session = scoped_session(DBSession)()

mi= session.query(IMDB_Movie_Info)
mii = session.query(IMDB_Info_Index_Data)
msi = session.query(IMDB_Summary_Index_Data)

mi = movie_data_formator.movie_info_to_dict(mi)
mii = movie_data_formator.movie_info_index_to_dict(mii)
msi = movie_data_formator.movie_summary_index_to_dict(msi)

@app.route('/', methods=['GET', 'POST'])
@app.route('/index', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        analyzer = Analyzer()
        search_query = request.form['query']
        stop_word = analyzer.stop_word(search_query)
        stem = analyzer.stemming(stop_word)

        result = Counter({})
        for word in set(stem.split(" ")):
            word = session.query(IMDB_Index_Data).filter_by(word=word).first()
            print(str(word))
            if word is not None:
                result += result + Counter(json.loads(word.document_id))

        result = OrderedDict(sorted(result.items(), key=lambda d: d[1], reverse=True))
        s = ""

        n = 0
        r = {}
        for movie_id, tf in result.items():
            if len(r) < 50:
                # movie_info = session.query(IMDB_Movie_Info.title, IMDB_Movie_Info.year, IMDB_Movie_Info.serial).filter_by(id=movie_id).first()
                movie_info = session.query(IMDB_Movie_Info).filter_by(id=movie_id).first()

                if movie_info.run_time != "":
                    print(movie_info.run_time)
                    if float(movie_info.run_time.split(" ")[0]) > 80:
                    # if 1==1:
                        r[n] = {}
                        r[n]['title'] = movie_info.title
                        r[n]['year'] = movie_info.year
                        r[n]['certificate'] = movie_info.certificate
                        r[n]['run_time'] = movie_info.run_time
                        r[n]['genre'] = movie_info.genre
                        r[n]['rating'] = movie_info.rating
                        r[n]['rating_count'] = movie_info.rating_count
                        r[n]['gross'] = movie_info.gross
                        r[n]['actor'] = movie_info.actor
                        r[n]['serial'] = movie_info.serial
                        r[n]['tf'] = tf
                        n += 1

            else:
                break

        # return s
        return render_template('result.html', result=r)
    else:
        # return "OK"
        return render_template('index.html')

@app.route('/search_0', methods=['GET', 'POST'])
def search_without_rating_count():
    if request.method == 'POST':
        analyzer = Analyzer()
        search_query = request.form['query']
        stop_word = analyzer.stop_word(search_query)
        stem = analyzer.stemming(stop_word)

        info_result = Counter({})
        summary_result = Counter({})
        for word in set(stem.split(" ")):
            info_word = session.query(IMDB_Info_Index_Data).filter_by(word=word).first()
            summary_word = session.query(IMDB_Summary_Index_Data).filter_by(word=word).first()
            if info_word is not None:
                info_result += info_result + Counter(json.loads(info_word.document_id))
            if summary_word is not None:
                summary_result += summary_result + Counter(json.loads(summary_word.document_id))

        result = info_result + summary_result
        result = OrderedDict(sorted(result.items(), key=lambda d: d[1], reverse=True))
        s = ""

        n = 0
        r = {}
        for movie_id, tf in result.items():
            if len(r) < 50:
                # movie_info = session.query(IMDB_Movie_Info.title, IMDB_Movie_Info.year, IMDB_Movie_Info.serial).filter_by(id=movie_id).first()
                movie_info = session.query(IMDB_Movie_Info).filter_by(id=movie_id).first()
                if movie_info.run_time != "":
                    if float(movie_info.run_time.split(" ")[0]) > 80:
                        r[n] = {}
                        r[n]['title'] = movie_info.title
                        r[n]['year'] = movie_info.year
                        r[n]['certificate'] = movie_info.certificate
                        r[n]['run_time'] = movie_info.run_time
                        r[n]['genre'] = movie_info.genre
                        r[n]['rating'] = movie_info.rating
                        r[n]['rating_count'] = movie_info.rating_count
                        r[n]['gross'] = movie_info.gross
                        r[n]['actor'] = movie_info.actor
                        r[n]['serial'] = movie_info.serial
                        r[n]['tf'] = tf
                        n += 1

            else:
                break

        return render_template('result.html', result=r)
    else:
        return render_template('search.html')

@app.route('/search', methods=['GET', 'POST'])
def search():
    if request.method == 'POST':
        start = timeit.default_timer()
        analyzer = Analyzer()
        search_query = request.form['query']
        stop_word = analyzer.stop_word(search_query)
        stem = analyzer.stemming(stop_word)

        info_result = Counter({})
        summary_result = Counter({})
        for word in set(stem.split(" ")):
            info_word = session.query(IMDB_Info_Index_Data).filter_by(word=word).first()
            summary_word = session.query(IMDB_Summary_Index_Data).filter_by(word=word).first()
            if info_word is not None:
                info_result += info_result + Counter(json.loads(info_word.document_id))
            if summary_word is not None:
                summary_result += summary_result + Counter(json.loads(summary_word.document_id))

        result = info_result + summary_result
        result = OrderedDict(sorted(result.items(), key=lambda d: d[1], reverse=True))

        n = 0
        r = {}
        for movie_id, tf in result.items():
            if len(r) < 500:
                # movie_info = session.query(IMDB_Movie_Info.title, IMDB_Movie_Info.year, IMDB_Movie_Info.serial).filter_by(id=movie_id).first()
                movie_info = session.query(IMDB_Movie_Info).filter_by(id=movie_id).first()


                if movie_info.run_time != "":
                    run_time = movie_info.run_time.split(" ")
                    run_time = run_time[0].split(",")
                    run_time = "".join(run_time)
                    if float(run_time) > 80:

                        r[n] = {}
                        r[n]['title'] = movie_info.title
                        r[n]['year'] = movie_info.year
                        r[n]['certificate'] = movie_info.certificate
                        r[n]['run_time'] = movie_info.run_time
                        r[n]['genre'] = movie_info.genre
                        r[n]['rating'] = movie_info.rating
                        r[n]['rating_count'] = movie_info.rating_count
                        r[n]['gross'] = movie_info.gross
                        r[n]['actor'] = movie_info.actor
                        r[n]['serial'] = movie_info.serial
                        r[n]['tf'] = tf
                        r[n]['rank'] = n
                        n += 1
            else:
                break
        rr = OrderedDict(sorted(r.items(), key=lambda d: d[1]['rating_count'], reverse=True))
        for i, key in enumerate(rr):
            r[key]['rank'] += i

        r = OrderedDict(sorted(r.items(), key=lambda d: d[1]['rank'], reverse=False))

        spell = SpellChecker()
        query = search_query.split(" ")
        correct = []
        for word in query:
            correct.append(spell.correction(word))
        correct = " ".join(correct)
        if correct == search_query:
            correct = ""
        stop = timeit.default_timer()
        return render_template('result.html', result=r, correct=correct, query=search_query, query_time=round(stop-start,2))
    else:
        return render_template('search.html')

@app.route('/search_v2', methods=['GET', 'POST'])
def search_v2():
    start = timeit.default_timer()
    if request.method == 'POST':
        analyzer = Analyzer()
        search_query = request.form['query']
        stop_word = analyzer.stop_word(search_query)
        stem = analyzer.stemming(stop_word)

        info_result = Counter({})
        summary_result = Counter({})
        for word in set(stem.split(" ")):
            if word in mii:
                info_word = mii[word]
                info_result += info_result + Counter(info_word['document_id'])

            if word in msi:
                summary_word = msi[word]
                summary_result += summary_result + Counter(summary_word['document_id'])

        result = info_result + summary_result
        result = OrderedDict(sorted(result.items(), key=lambda d: d[1], reverse=True))

        n = 0
        r = {}
        for movie_id, tf in result.items():
            if len(r) < 500:
                # movie_info = session.query(IMDB_Movie_Info.title, IMDB_Movie_Info.year, IMDB_Movie_Info.serial).filter_by(id=movie_id).first()
                movie_info = session.query(IMDB_Movie_Info).filter_by(id=movie_id).first()
                movie_info = mi[int(movie_id)]


                if movie_info['run_time'] != "":
                    run_time = movie_info['run_time'].split(" ")
                    run_time = run_time[0].split(",")
                    run_time = "".join(run_time)
                    if float(run_time) > 80:

                        r[n] = {}
                        r[n]['title'] = movie_info['title']
                        r[n]['year'] = movie_info['year']
                        r[n]['certificate'] = movie_info['certificate']
                        r[n]['run_time'] = movie_info['run_time']
                        r[n]['genre'] = movie_info['genre']
                        r[n]['rating'] = movie_info['rating']
                        r[n]['rating_count'] = movie_info['rating_count']
                        r[n]['gross'] = movie_info['gross']
                        r[n]['actor'] = movie_info['actor']
                        r[n]['serial'] = movie_info['serial']
                        r[n]['tf'] = tf
                        r[n]['rank'] = n
                        n += 1
            else:
                break
        rr = OrderedDict(sorted(r.items(), key=lambda d: d[1]['rating_count'], reverse=True))
        for i, key in enumerate(rr):
            r[key]['rank'] += i

        r = OrderedDict(sorted(r.items(), key=lambda d: d[1]['rank'], reverse=False))

        spell = SpellChecker()
        query = search_query.split(" ")
        correct = []
        for word in query:
            correct.append(spell.correction(word))
        correct = " ".join(correct)
        if correct == search_query:
            correct = ""
        stop = timeit.default_timer()
        return render_template('result_v2.html', result=r, correct=correct, query=search_query, query_time=round(stop-start,2))
    else:
        return render_template('search_v2.html')



if __name__ == '__main__':
    app.secret_key = "secret_key"
    app.debug = True
    app.run(host='0.0.0.0', port=5000)


