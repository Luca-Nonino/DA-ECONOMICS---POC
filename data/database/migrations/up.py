# data/database/migrations/update_documents_table.py

import os
import sys

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Define the path to the database
db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../database.sqlite'))

# Create the SQLAlchemy engine
engine = create_engine(f'sqlite:///{db_path}')

# Create a base class for declarative models
Base = declarative_base()

# Create a session factory
Session = sessionmaker(bind=engine)

class DocumentsTable(Base):
    __tablename__ = 'documents_table'

    document_id = Column(Integer, primary_key=True)
    pipe_name = Column(String)
    pipe_id = Column(Integer)

def update_documents_table():
    session = Session()

    # Data to be inserted/updated
    data = [
        {"document_id": 22, "pipe_name": "PDF Download Link", "pipe_id": 3},
        {"document_id": 23, "pipe_name": "Custom Logic", "pipe_id": 4}
    ]

    for item in data:
        # Check if the document already exists
        existing_document = session.query(DocumentsTable).filter_by(document_id=item['document_id']).first()

        if existing_document:
            # Update existing document
            for key, value in item.items():
                setattr(existing_document, key, value)
        else:
            # Insert new document
            new_document = DocumentsTable(**item)
            session.add(new_document)

    # Commit the changes
    session.commit()
    session.close()

    print("Documents table updated successfully.")

if __name__ == "__main__":
    # Ensure the table exists
    Base.metadata.create_all(engine)
    
    # Update the documents table
    update_documents_table()
