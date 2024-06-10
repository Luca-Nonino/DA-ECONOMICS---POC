# data/database/keytakeaways_table.py
from sqlalchemy import Column, Integer, Text, Date, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from data.database.documents_table import Base, DocumentsTable

class KeyTakeawaysTable(Base):
    __tablename__ = 'key_takeaways_table'

    takeaway_id = Column(Integer, primary_key=True)
    document_id = Column(Integer, ForeignKey('documents_table.document_id'))
    release_date = Column(Date)
    title = Column(Text)
    content = Column(Text)

    document = relationship("DocumentsTable", back_populates="key_takeaways")
