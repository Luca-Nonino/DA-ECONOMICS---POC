# scripts/utils/auth.py
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import sqlite3

security = HTTPBasic()

def get_current_user(credentials: HTTPBasicCredentials = Depends(security)):
    conn = sqlite3.connect('data/database/database.sqlite')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE username=? AND password=?", (credentials.username, credentials.password))
    user = cursor.fetchone()
    conn.close()
    if user:
        return user
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
