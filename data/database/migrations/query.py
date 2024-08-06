import os
import sys

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from data.database.models import Base, DocumentsTable

# Define the path to the database
db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../database.sqlite'))
print(f"Database path: {db_path}")

# Create the SQLAlchemy engine
engine = create_engine(f'sqlite:///{db_path}', echo=True)

# Create a session factory
Session = sessionmaker(bind=engine)

def list_all_documents():
    session = Session()

    try:
        # Create the select statement
        stmt = select(
            DocumentsTable.document_id,
            DocumentsTable.document_name,
            DocumentsTable.source_name,
            DocumentsTable.path,
            DocumentsTable.country
        )

        # Execute the query
        result = session.execute(stmt)

        # Fetch all results
        documents = result.fetchall()

        # Print the results
        print("All documents:")
        for doc in documents:
            print(f"ID: {doc.document_id}, Name: {doc.document_name}, Source: {doc.source_name}, Path: {doc.path}, Country: {doc.country}")

        return documents

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    list_all_documents()
