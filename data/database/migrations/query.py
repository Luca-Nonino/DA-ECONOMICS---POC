import os
import sys

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from sqlalchemy import create_engine, select, Column, Integer, String, Text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base

# Define the path to the database
db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../database.sqlite'))
print(f"Database path: {db_path}")

# Create the SQLAlchemy engine
engine = create_engine(f'sqlite:///{db_path}', echo=True)

# Create a base class for declarative models
Base = declarative_base()

# Create a session factory
Session = sessionmaker(bind=engine)

class DocumentsTable(Base):
    __tablename__ = 'documents_table'

    document_id = Column(Integer, primary_key=True)
    pipe_name = Column(String)
    pipe_id = Column(Integer)
    document_name = Column(String)
    source_name = Column(String)

def read_documents_table():
    session = Session()

    # Create a query to select document_id, document_name, source_name, pipe_name, and pipe_id
    stmt = select(
        DocumentsTable.document_id,
        DocumentsTable.document_name,
        DocumentsTable.source_name,
        DocumentsTable.pipe_name,
        DocumentsTable.pipe_id
    )

    # Execute the query
    result = session.execute(stmt)

    # Print the results
    print("document_id | document_name | source_name | pipe_name | pipe_id")
    print("------------|---------------|-------------|-----------|--------")
    for row in result:
        print(f"{row.document_id:11d} | {row.document_name:13s} | {row.source_name:11s} | {row.pipe_name:9s} | {row.pipe_id}")

    session.close()
    engine.dispose()

if __name__ == "__main__":
    read_documents_table()
