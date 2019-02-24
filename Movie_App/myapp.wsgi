#!/usr/bin/python3.6
import sys
sys.path.insert(0,"/var/www/Movie_App/")
sys.path.insert(0,"/var/www/Movie_App/movie_app/")
from movie_app import app as application

application.secret_key = "secret_key"