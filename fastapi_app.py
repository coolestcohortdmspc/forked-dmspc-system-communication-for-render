from fastapi import FastAPI

# create instance 
app = FastAPI()

@app.get("/ping")
def ping():
    return {"message": "pong"}