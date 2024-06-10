# data/database/summary_table.py
from sqlalchemy import Column, Integer, Text, Date, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from data.database.documents_table import Base, DocumentsTable

class SummaryTable(Base):
    __tablename__ = 'summary_table'

    summary_id = Column(Integer, primary_key=True)
    document_id = Column(Integer, ForeignKey('documents_table.document_id'))
    release_date = Column(Date)
    style = Column(Text)
    content = Column(Text)
    en_summary = Column(Text)
    pt_summary = Column(Text)

    document = relationship("DocumentsTable", back_populates="summaries")
