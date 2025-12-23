from fastapi import FastAPI, HTTPException
import psycopg2
import pandas as pd

# create FastAPI object
app = FastAPI()

def getConnection():
    # create connection
    conn = psycopg2.connect(
        dbname="neondb", user="neondb_owner", password="npg_sLfVg8iW4EwO",
        host="ep-steep-water-a102fmjl-pooler.ap-southeast-1.aws.neon.tech",
    )

    return conn

@app.get('/') # Create endpoint
async def getWelcome():
    return {
        "msg": "sample-fastapi-pg"
    }

@app.get('/profiles') # Create endpoint for data profile retrieval
async def getProfiles():
    conn = getConnection() # Connect to db
    df = pd.read_sql("SELECT * FROM profiles", conn)
    return df.to_dict(orient="records") # Give response

@app.get('/profiles/{id}') # Create endpoint fot data profile retrieval with filter
async def getProfileById(id: int):
    conn = getConnection()
    df = pd.read_sql(f"SELECT * FROM profiles WHERE id = {id}", conn)

    # Error handling
    if len(df) == 0:
        raise HTTPException(status_code=404, detail="Data not found!")

    return df.to_dict(orient="records")

#@app.post(...)
#async def createProfile():
#    pass

#@app.patch(...)
#async def updateProfile():
#    pass

#@app.delete(...)
#async def deleteProfile():
#    pass