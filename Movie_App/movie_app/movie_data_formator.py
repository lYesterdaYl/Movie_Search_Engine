import json

def movie_info_to_dict(movie_info):
    result = {}
    for movie in movie_info:
        result[movie.id] = {}
        result[movie.id]['title'] = movie.title
        result[movie.id]['year'] = movie.year
        result[movie.id]['certificate'] = movie.certificate
        result[movie.id]['run_time'] = movie.run_time
        result[movie.id]['genre'] = movie.genre
        result[movie.id]['rating'] = movie.rating
        result[movie.id]['rating_count'] = movie.rating_count
        result[movie.id]['gross'] = movie.gross
        result[movie.id]['actor'] = movie.actor
        result[movie.id]['serial'] = movie.serial

    return result

def movie_info_index_to_dict(movie_info_index):
    result = {}
    for movie in movie_info_index:
        result[movie.word] = {}
        result[movie.word]['document_id'] = json.loads(movie.document_id)
    return result

def movie_summary_index_to_dict(movie_summary_index):
    result = {}
    for movie in movie_summary_index:
        result[movie.word] = {}
        result[movie.word]['document_id'] = json.loads(movie.document_id)
    return result