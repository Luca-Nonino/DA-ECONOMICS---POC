import os
import sys

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from sqlalchemy import create_engine, insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from data.database.models import Base, DocumentsTable

# Define the path to the database
db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../database.sqlite'))
print(f"Database path: {db_path}")

# Create the SQLAlchemy engine
engine = create_engine(f'sqlite:///{db_path}', echo=True)

# Create a session factory
Session = sessionmaker(bind=engine)

def insert_ism_documents():
    session = Session()

    # Define the ISM documents to insert
    documents = [
        {
            "document_id": 44,
            "path": "https://www.ismworld.org/supply-management-news-and-reports/reports/ism-report-on-business/pmi/",
            "reference_month": "CURRENT",
            "pipe_name": "On Page Content",
            "pipe_id": 1,
            "freq": "M",
            "document_name": "ISM REPORT ON BUSINESS PMI",
            "source_name": "Institute for Supply Management",
            "escope": "N",
            "country": "US",  # Assuming US as the country
            "current_release_date": None,  # Optional, adjust if specific dates are available
            "hash": None,  # Optional, adjust if specific data is available
            "next_release_date": None,  # Optional, adjust if specific dates are available
            "next_release_time": None  # Optional, adjust if specific times are available
        },
        {
            "document_id": 45,
            "path": "https://www.ismworld.org/supply-management-news-and-reports/reports/ism-report-on-business/services/",
            "reference_month": "CURRENT",
            "pipe_name": "On Page Content",
            "pipe_id": 1,
            "freq": "M",
            "document_name": "ISM REPORT ON BUSINESS SERVICES",
            "source_name": "Institute for Supply Management",
            "escope": "N",
            "country": "US",  # Assuming US as the country
            "current_release_date": None,  # Optional, adjust if specific dates are available
            "hash": None,  # Optional, adjust if specific data is available
            "next_release_date": None,  # Optional, adjust if specific dates are available
            "next_release_time": None  # Optional, adjust if specific times are available
        }
    ]

    for doc in documents:
        try:
            stmt = insert(DocumentsTable).values(**doc)
            session.execute(stmt)
            session.commit()
            print(f"Successfully inserted document with id {doc['document_id']}")
        except IntegrityError:
            print(f"Document with id {doc['document_id']} already exists")
            session.rollback()
        except Exception as e:
            print(f"An error occurred while inserting document {doc['document_id']}: {e}")
            session.rollback()

    session.close()

if __name__ == "__main__":
    insert_ism_documents()
