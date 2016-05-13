""" Query OMDB API for each movie """

from __future__ import print_function
import csv
import json
import logging
import os
import requests
from time import sleep, time

from tqdm import tqdm

CACHE_FOLDER = '../cache/'
DELAY = 3
IMDB_IDS = 'imdb_ids.json'
MOVIE_IDS = 'movie_ids.json'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__ if __name__ != '__main__' else [])
logging.getLogger("requests").setLevel(logging.WARNING)


"""
Example Raw request.json()

{
  "Title": "Argo",
  "Year": "2012",
  "Rated": "R",
  "Released": "12 Oct 2012",
  "Runtime": "120 min",
  "Genre": "Biography, Drama, History",
  "Director": "Ben Affleck",
  "Writer": "Chris Terrio (screenplay), Tony Mendez (based on a selection from \"The Master of Disguise\" by), Joshuah Bearman (based on the Wired Magazine article \"The Great Escape\" by)",
  "Actors": "Ben Affleck, Bryan Cranston, Alan Arkin, John Goodman",
  "Plot": "Acting under the cover of a Hollywood producer scouting a location for a science fiction film, a CIA agent launches a dangerous operation to rescue six Americans in Tehran during the U.S. hostage crisis in Iran in 1980.",
  "Language": "English, Persian",
  "Country": "USA",
  "Awards": "Won 3 Oscars. Another 88 wins & 146 nominations.",
  "Poster": "http://ia.media-imdb.com/images/M/MV5BMTc3MjI0MjM0NF5BMl5BanBnXkFtZTcwMTYxMTQ1OA@@._V1_SX300.jpg",
  "Metascore": "86",
  "imdbRating": "7.8",
  "imdbVotes": "440,087",
  "imdbID": "tt1024648",
  "Type": "movie",
  "tomatoMeter": "96",
  "tomatoImage": "certified",
  "tomatoRating": "8.4",
  "tomatoReviews": "303",
  "tomatoFresh": "290",
  "tomatoRotten": "13",
  "tomatoConsensus": "Tense, exciting, and often darkly comic, Argo recreates a historical event with vivid attention to detail and finely wrought characters.",
  "tomatoUserMeter": "90",
  "tomatoUserRating": "4.2",
  "tomatoUserReviews": "205312",
  "tomatoURL": "http://www.rottentomatoes.com/m/argo_2012/",
  "DVD": "19 Feb 2013",
  "BoxOffice": "$136.0M",
  "Production": "Warner Bros. Pictures",
  "Website": "http://argothemovie.warnerbros.com",
  "Response": "True"
}
"""

FIELDS = ["Title", "Year", "Rated", "Released", "Runtime", "Genre", "Director",
          "Writer", "Actors", "Plot", "Language", "Country", "Awards", "Poster",
          "Metascore", "imdbRating", "imdbVotes", "imdbID", "Type",
          "tomatoMeter", "tomatoImage", "tomatoRating", "tomatoReviews",
          "tomatoFresh", "tomatoRotten", "tomatoConsensus", "tomatoUserMeter",
          "tomatoUserRating", "tomatoUserReviews", "tomatoURL", "DVD",
          "BoxOffice", "Production", "Website", "Response", "id"]


def fetch_info(imdb_id, response='json', plot='full', tomatoes=True):
    """
    E.g. http://www.omdbapi.com/?i=tt1024648&plot=short&r=json&tomatoes=true
    """
    url = 'http://www.omdbapi.com/?i=' + imdb_id
    url += '&plot=' + plot
    url += '&r=' + response
    url += '&tomatoes=' + str(tomatoes)
    rq = requests.get(url)
    return rq.json()


def cache_json():
    with open(IMDB_IDS, 'rb') as fi:
        imdb_ids = json.load(fi)
    with open(MOVIE_IDS, 'rb') as fi:
        movie_ids = json.load(fi)
    movie_ids[472] = u'Precious'
    movie_ids[499] = u'Les Mis\xe9rables'
    title_to_id = {v: k for k, v in movie_ids.items()}
    logger.info('Fetching %d titles..' % len(imdb_ids))
    d = {}
    for k, v in tqdm(imdb_ids.items(), total=len(imdb_ids)):
        sleep(DELAY)
        data = fetch_info(v)
        data['id'] = str(title_to_id[k])
        d[k] = data
    if not os.path.exists(CACHE_FOLDER):
        os.makedirs(CACHE_FOLDER)
    out_path = CACHE_FOLDER + 'imdb.json'
    with open(out_path, 'wb') as fo:
        json.dump(d, fo)
    logger.info('Saved: %s' % out_path)
    return d


def main():
    t0 = time()

    json_file = CACHE_FOLDER + 'imdb.json'
    if os.path.exists(json_file):
        logger.info('Using JSON Cache file: %s' % json_file)
        with open(json_file, 'rb') as fi:
            d = json.load(fi)
    else:
        logger.info('JSON cache file not found: %s' % json_file)
        d = cache_json()

    out_file = CACHE_FOLDER + 'imdb.csv'
    with open(out_file, 'wb') as fo:
        w = csv.DictWriter(fo, FIELDS)
        w.writeheader()
        for m in sorted(d):
            w.writerow(dict((k, v.encode('utf-8')) for k, v in d[m].items()))
    logger.info('Saved: %s' % out_file)

    tf = (time() - t0) / 60
    logger.info('--- %0.3f minutes ---' % tf)


if __name__ == '__main__':
    main()
