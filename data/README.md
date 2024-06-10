# Database Directory

This directory contains the database files and migration scripts.

## Structure

- `database.sqlite`: The SQLite database file.
- `migrations/`: Migration scripts for setting up the database tables.

## Setup

1. Initialize the database:
   ```sh
   python data/database/migrations/initiate_sqlite.py
   ```

2. Load initial data:
   ```sh
   python data/database/migrations/load_users.py
   ```

## Tables

- `documents_table`: Contains document metadata.
- `summary_table`: Contains summaries of the documents.
- `keytakeaways_table`: Contains key takeaways of the documents.
