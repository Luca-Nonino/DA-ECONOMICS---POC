# data/database/migrations/update.py

import os
import sys

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from data.database.models import Base, DocumentsTable

# Define the path to the database
db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../database.sqlite'))
print(f"Database path: {db_path}")

# Create the SQLAlchemy engine
engine = create_engine(f'sqlite:///{db_path}', echo=True)

# Create a session factory
Session = sessionmaker(bind=engine)

def update_documents_table():
    session = Session()

    # Data to be inserted/updated
    data = [
        {"document_id": 22, "pipe_name": "Updated PDF Link", "pipe_id": 3},
        {"document_id": 23, "pipe_name": "Updated Custom Logic", "pipe_id": 4}
    ]

    for record in data:
        document = session.query(DocumentsTable).filter_by(document_id=record['document_id']).first()
        if document:
            # Update existing record
            document.pipe_name = record['pipe_name']
            document.pipe_id = record['pipe_id']
            print(f"Updated document_id {record['document_id']}")
        else:
            # Insert new record
            document = DocumentsTable(**record)
            session.add(document)
            print(f"Inserted document_id {record['document_id']}")
    
    session.commit()
    session.close()
    engine.dispose()

if __name__ == "__main__":
    update_documents_table()
