import os
import sys
from datetime import datetime

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from data.database.models import Base, DocumentsTable, SummaryTable

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
            DocumentsTable.country,
            DocumentsTable.current_release_date
        )

        # Execute the query
        result = session.execute(stmt)

        # Fetch all results
        documents = result.fetchall()

        # Print the results
        print("All documents:")
        for doc in documents:
            print(f"ID: {doc.document_id}, ReleaseDate: {doc.current_release_date}, Name: {doc.document_name}, Source: {doc.source_name}, Path: {doc.path}, Country: {doc.country}")

        return documents

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        session.close()

def list_all_summaries():
    session = Session()

    try:
        # Create the select statement for the SummaryTable
        stmt = select(
            SummaryTable.summary_id,
            SummaryTable.document_id,
            SummaryTable.release_date,
        )

        # Execute the query
        result = session.execute(stmt)

        # Fetch all results
        summaries = result.fetchall()

        # Print the results
        print("All summaries:")
        for summary in summaries:
            summary_id = summary.summary_id
            document_id = summary.document_id
            release_date = summary.release_date
            
            # Convert release_date to string representation
            if isinstance(release_date, datetime.date):
                formatted_release_date = release_date.strftime('%Y-%m-%d')
            elif isinstance(release_date, str):
                try:
                    parsed_date = datetime.fromisoformat(release_date)
                    formatted_release_date = parsed_date.strftime('%Y-%m-%d')
                except ValueError:
                    formatted_release_date = "Invalid date"
            else:
                formatted_release_date = "Unknown"

            print(f"Summary ID: {summary_id}, Document ID: {document_id}, "
                  f"Release Date: {formatted_release_date}")

        return summaries

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        session.close()


if __name__ == "__main__":
    list_all_documents()
    list_all_summaries()
