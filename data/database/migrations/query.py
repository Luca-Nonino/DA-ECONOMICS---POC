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

def query_htm_documents():
    session = Session()

    try:
        # Create the select statement
        stmt = select(DocumentsTable.document_id, DocumentsTable.path).where(
            DocumentsTable.path.like('%htm')
        )

        # Execute the query
        result = session.execute(stmt)

        # Fetch all results
        htm_documents = result.fetchall()

        # Print the results
        print("Documents with paths ending in 'htm':")
        for doc in htm_documents:
            print(f"ID: {doc.document_id}, Path: {doc.path}")

        return htm_documents

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    query_htm_documents()
