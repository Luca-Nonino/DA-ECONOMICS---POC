import os
import sys

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from sqlalchemy import create_engine, update
from sqlalchemy.orm import sessionmaker
from data.database.models import Base, DocumentsTable

# Define the path to the database
db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../database.sqlite'))
print(f"Database path: {db_path}")

# Create the SQLAlchemy engine
engine = create_engine(f'sqlite:///{db_path}', echo=True)

# Create a session factory
Session = sessionmaker(bind=engine)

def update_document_source_name():
    session = Session()

    # Update the source_name of document_id 6 to 'U.S DoL'
    try:
        stmt = update(DocumentsTable).where(DocumentsTable.document_id == 6).values(source_name='U.S DoL')
        session.execute(stmt)
        session.commit()
        print("Successfully updated source_name of document_id 6 to 'U.S DoL'")
    except Exception as e:
        print(f"An error occurred while updating document_id 6: {e}")
        session.rollback()

    session.close()

if __name__ == "__main__":
    update_document_source_name()
