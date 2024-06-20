import os
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import sqlite3

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
security = HTTPBasic()

def get_current_user(credentials: HTTPBasicCredentials = Depends(security)):
    db_path = os.path.join(BASE_DIR, 'data/database/database.sqlite')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users_table WHERE username=? AND password=?", (credentials.username, credentials.password))
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
