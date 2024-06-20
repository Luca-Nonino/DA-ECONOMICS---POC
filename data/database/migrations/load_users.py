# data/database/migrations/load_users.py
import sys
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add the directory containing users_table.py to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from users_table import User, Base

def load_users():
    engine = create_engine('sqlite:///data/database/database.sqlite')
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    users = [
        User(username='NataliaTonin', password='NT@2024'),
        User(username='AlexLima', password='AL@2024'),
        User(username='AnersonAlvarenga', password='AA@2024'),
        User(username='ThalesCaramella', password='TC@2024'),
        User(username='VitorOliveira', password='VO@2024'),
        User(username='LucaNonino', password='LN@2024'),
        User(username='PedroJuliato', password='PJ@2024'),
        User(username='Admin', password='Admin@443251'),
        User(username='Tester', password='Tester')
    ]

    session.bulk_save_objects(users)
    session.commit()
    session.close()

if __name__ == "__main__":
    load_users()
