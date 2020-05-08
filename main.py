
import sqlite3

from fastapi import FastAPI, Response
from pydantic import BaseModel
from fastapi.encoders import jsonable_encoder


class Album(BaseModel):
    title: str
    artist_id: int


class Customer(BaseModel):
    Company: str = None
    Address: str = None
    City: str = None
    State: str = None
    Country: str = None
    PostalCode: str = None
    Fax: str = None



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
async def get_tracks(page:int = 0, per_page:int = 10):
    ogr = page * per_page
    app.db_connection.row_factory = sqlite3.Row
    tracks = app.db_connection.execute(f"SELECT * FROM tracks order by trackid LIMIT {per_page} OFFSET {ogr}").fetchall()
    return tracks



# Zadanie 2 

@app.get("/tracks/composers/")
async def get_composers(response: Response, composer_name:str):
    app.db_connection.row_factory = lambda cursor, x: x[0]
    composers = app.db_connection.execute(f"select Name from tracks t where Composer = '{composer_name}' order by 1").fetchall()
    if composers!=[]:
        return composers
    else:
        response.status_code = 404
        return {"detail":{"error": "Brak kompozytora"}}



# Zadanie 3

@app.post("/albums")
async def album_add(response: Response, album: Album):
    app.db_connection.row_factory =  lambda cursor, x: x[0]
    cursor0 = app.db_connection.execute(f"select name from artists where ArtistId = {album.artist_id}").fetchone()
    if cursor0 is None:
        response.status_code = 404
        return {"detail":{"error": "Brak artysty"}}   
    else:
        cursor = app.db_connection.execute(f"INSERT INTO albums (title, artistid) VALUES ('{album.title}', {album.artist_id})", )
        app.db_connection.commit()
        response.status_code = 201
        return {
            "AlbumId": cursor.lastrowid,
            "Title": album.title,
            "ArtistId": album.artist_id
        }



@app.get("/albums/{album_id}")
async def album_get(response: Response, album_id:int):
    app.db_connection.row_factory =  sqlite3.Row
    cursor1 = app.db_connection.execute(f"select * from albums where albumid = {album_id}").fetchone()
    if cursor1 is None:
        response.status_code = 404
        return {"detail":{"error": "Brak albumu"}}   
    else:
        response.status_code = 200
        return cursor1


# Zadanie 4

@app.put("/customers/{customer_id}")
async def customer_put(response: Response, customer_id: int, customer: Customer):
    app.db_connection.row_factory =  sqlite3.Row
    cursor1 = app.db_connection.execute(f"select company, address, city, state, country, postalcode, fax from customers where customerid = {customer_id}").fetchone()
    if cursor1 is None:
        response.status_code = 404
        return {"detail":{"error": "Brak klienta"}}   
    else:
        response.status_code = 200
        customer_model = Customer(**cursor1)
        update_data = customer.dict(exclude_unset = True)
        updated_customer = customer_model.copy(update = update_data)
        cursor2 = app.db_connection.execute(
        	"""UPDATE customers SET Company = ?, Address = ?, City = ?, State = ?, Country = ?, PostalCode = ?, Fax = ? WHERE CustomerId = ?""", 
            (updated_customer.Company, updated_customer.Address, updated_customer.City, updated_customer.State, updated_customer.Country, updated_customer.PostalCode, updated_customer.Fax, customer_id))
        app.db_connection.commit()

        app.db_connection.row_factory = sqlite3.Row
        cursor3 = app.db_connection.execute(
        """SELECT *
         FROM customers WHERE CustomerId = ?""",
        (customer_id, )).fetchone()
        return cursor3