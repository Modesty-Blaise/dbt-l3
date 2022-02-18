from bottle import get, post, run, request, response
import sqlite3
from urllib.parse import unquote

db = sqlite3.connect("movies.sqlite")

@get('/ping')
def pingpong():
    response.status = 200
    return {"pong"}

@post('/reset')
def reset_database():
    c = db.cursor()
    try:
        c.execute("DELETE FROM theatres")
        db.commit()

        c.execute("DELETE FROM movies")
        db.commit()
    
        c.execute("DELETE FROM performances")
        db.commit()

        c.execute("DELETE FROM customers")
        db.commit()

        c.execute("DELETE FROM tickets")
        db.commit()

        c.execute(
            """
            INSERT 
            INTO theatres (name, capacity)
            VALUES ('Kino', 10), ('Regal', 16), ('Skandia', 100);
            """
        )
        db.commit()

        response.status = 201
        return {"data": "reset"}
    
    except sqlite3.IntegrityError:
        response.status = 400
        return ""


@post('/users') 
def create_user():
    user = request.json
    c = db.cursor()
    try: 
        c.execute(
            """
            INSERT
            INTO       customers(user_name, full_name, password)
            VALUES     (?, ?, ?)
            """,
            [user['username'], user['fullName'], hash(user['pwd'])]
            )
        db.commit()
        response.status = 201
        return f"/users/{user['username']}"

    except sqlite3.IntegrityError:
        response.status = 400
        return ""

#test:
#curl -X POST http://localhost:7007/users -H 'Content-Type: application/json' -d '{"username": "alice","fullName": "Alice Lidell","pwd": "aliceswaytoosimplepassword"}'


@post('/movies')
def add_movie():
    movie = request.json
    c = db.cursor()
    try: 
        c.execute(
            """
            INSERT 
            INTO    movies(imdb, title, production_year)
            VALUES  (?, ?, ?)
            """, [movie['imdbKey'], movie['title'], movie['year']]
            )
        db.commit()
        response.status = 201
        return f"/movies/{movie['imdbKey']}"

    except sqlite3.IntegrityError:
        response.status = 400
        return "" 

#curl -X POST http://localhost:7007/movies -H 'Content-Type: application/json' -d '{"imdbKey": "tt4975722","title": "Moonlight","year": 2016}'


@post('/performances')
def add_performance():
    performance = request.json
    c = db.cursor()
    try:
        c.execute(
            """
            INSERT 
            INTO    performances(imdb, name, start_date, start_time)
            VALUES  (?, ?, ?, ?)
            """, 
            [performance['imdbKey'], 
            performance['theater'], 
            performance['date'], 
            performance['time']]
        )
        db.commit()
        c.execute("""
                SELECT  p_id
                FROM    performances
                WHERE   rowid = last_insert_rowid()
                """
        )
        found = c.fetchone()

        p_id,=found
        print(p_id)
        response.status = 201
        return f"/performances/{p_id}"
        
    except sqlite3.IntegrityError:
        response.status = 400
        return "No such movie or theater" 

#curl -X POST http://localhost:7007/performances -H 'Content-Type: application/json'    -d '{"imdbKey": "tt5580390","theater": "Kino","date": "2021-02-22","time": "19:00"}'

#@get('/movies')
#def get_movies():
#    c = db.cursor()
#    c.execute(
#        """
#        SELECT  imdbKey, title, year
#        FROM    movies
#        """
#    )
#    found = [{"imdbKey": imdbKey, "title": title, "year": year} 
#             for imdbKey, title, year in c]
#    response.status = 200
#    return {"data": found}

@get('/movies')
def get_movies():
    query = """
        SELECT  imdb, title, production_year
        FROM    movies
        WHERE   1 = 1
        """
    params = []
    if request.query.title:
        query += " AND title = ?"
        params.append(unquote(request.query.title))
    if request.query.year:
        query += " AND production_year = ?"
        params.append(unquote(request.query.year))
    c = db.cursor()
    c.execute(query, params)
    found = [{"imdbKey": imdb, "title": title, "year": production_year} 
             for imdb, title, production_year in c]
    response.status = 200
    return {"data": found}


@get('/movies/<imdbKey>')
def get_movies(imdbKey):
    c = db.cursor()
    c.execute(
        """
        SELECT imdb, title, production_year
        FROM movies
        WHERE imdb = ?
        """,
        [imdbKey]
    )
    found = [{"imdbKey": imdbKey, "titel": title, "year": year}
         for imdbKey, title, year in c]
    response.status = 200
    return {"data": found}


@get('/performances')
def get_performances():
    c = db.cursor()
    c.execute(
        """
        WITH sold_tickets(p_id, number_sold) AS (
            SELECT p_id, count() AS number_sold
            FROM tickets
            GROUP BY p_id
        )
        SELECT p_id, start_date, start_time, title, production_year, name, capacity, coalesce(number_sold, 0) AS tickets_left
        FROM performances
        JOIN movies
        USING (imdb)
        JOIN theatres
        USING (name) 
        LEFT OUTER JOIN sold_tickets
        USING (p_id)
        """
    )

    found = [{"performanceId": p_id, "date": start_date, "startTime": start_time, "title": title, "year": production_year, "theater": name, "remainingSeats": (capacity-tickets_left)}
         for p_id, start_date, start_time, title, production_year, name, capacity, tickets_left in c]
   
    response.status = 200
    return {"data": found}

@post('/tickets')
def buy_ticket():
    order = request.json
    c = db.cursor()

    c.execute(

        """
        SELECT username
        FROM users
        WHERE username = ? AND pwd = ?
        """,
        [order['username'], hash(order['pwd'])]
    )

    found = c.fetchone()

    if not found: 
        response.status = 401
        return "Wrong user credentials"
    
    c.execute(
        """
        WITH sold_tickets(p_id, number_sold) AS (
            SELECT p_id, count() AS number_sold
            FROM tickets
            GROUP BY p_id
        )
        SELECT (capacity - coalesce(number_sold,0)) AS tickets_left
        FROM performances
        JOIN theatres
        USING(name)
        LEFT OUTER JOIN tickets_sold
        USING(p_id)
        WHERE p_id = ? AND tickets_left > 0
        """,
        [order['p_id']]
    ) 

    found = c.fetchone()

    if not found: 
         response.status = 400
         return "No tickets left"

    c.execute("""
        INSERT
        INTO tickets(p_id, username)
        VALUES (?, ?)
        """, 
        [order['p_id'], order['username']]
    )

    c.execute("""
            SELECT  t_id
            FROM    tickets
            WHERE   rowid = last_insert_rowid()
            """
        )
    found = c.fetchone()

    if found:
        db.commit()
        response.status = 201
        return f"/tickets/{found}"

    else:
        response.status = 400
        return "Error"

@get("/users/<username>/tickets")
def get_tickets(username):
    
    c = db.cursor()

    c.execute(
        """
        WITH user_tickets AS (
            SELECT p_id, username, count(t_id) AS bought_tickets
            FROM performances
            LEFT OUTER JOIN tickets
            USING (p_id)
            WHERE username = ?
            GROUP BY p_id
        )
        
        SELECT date, time, theater, title, year, bought_tickets
        FROM performances
        JOIN movies
        USING (imdb)
        JOIN ticket_count
        USING(p_id)
        WHERE username = ?
        """,
        [username, username] 
    )

    found=[{"date": date, "startTime": time, "theater": theater, "title": title, "year": year, "nbrOfTickets": bought_tickets}
        for date, time, theater, title, year, bought_tickets in c]

    #print(found)
    
    response.status = 200
    return {"data": found}


def hash(msg):
    import hashlib
    return hashlib.sha256(msg.encode('utf-8')).hexdigest()


run(host='localhost', port=7007)