# data/database/migrations/populate_country.py
import sys
import os
from sqlalchemy import create_engine, Column, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError

# Ensure the root of the project is in the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from data.database.documents_table import DocumentsTable
from data.database.base import Base

def add_country_column(engine):
    with engine.connect() as conn:
        try:
            conn.execute('ALTER TABLE documents_table ADD COLUMN country VARCHAR')
        except OperationalError as e:
            if 'duplicate column name: country' in str(e):
                print("Column 'country' already exists. Skipping add.")
            else:
                raise

def populate_country(engine):
    Session = sessionmaker(bind=engine)
    session = Session()

    # Update all entries to set the country to "US"
    session.query(DocumentsTable).update({DocumentsTable.country: "US"})

    session.commit()
    session.close()

if __name__ == "__main__":
    # Define the explicit path for the database file
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../database.sqlite'))
    engine = create_engine(f'sqlite:///{db_path}')
    add_country_column(engine)  # Ensure the column is added
    populate_country(engine)  # Populate the column with "US"
