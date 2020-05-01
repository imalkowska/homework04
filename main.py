
import sqlite3

from fastapi import FastAPI

app = FastAPI()

# conn = sqlite3.connect('chinook.db')
# conn.close()

# with sqlite3.connect('chinook.db') as connection:
#     cursor = connection.cursor()
#     tracks = cursor.execute("SELECT name FROM tracks").fetchall()
#     print(len(tracks))
#     print(tracks[:2])



@app.on_event("startup")
async def startup():
    app.db_connection = sqlite3.connect('chinook.db')


@app.on_event("shutdown")
async def shutdown():
    app.db_connection.close()



# Zadanie 1

@app.get("/tracks")
async def root(page:int = 0, per_page:int = 10):
    ogr = page * per_page
    app.db_connection.row_factory = sqlite3.Row
    tracks = app.db_connection.execute(f"SELECT * FROM tracks order by trackid LIMIT {per_page} OFFSET {ogr}").fetchall()
    return tracks



# @app.get("/")
# async def root():
#     cursor = app.db_connection.cursor()
#     ....
#     return ...
