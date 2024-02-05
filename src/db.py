from pony.orm import Database

db = Database()
db.bind(provider='postgres', user="postgres", password="postgres", host='127.0.0.1', database="db_lobby", port="5432")
