# data/database/documents_table.py
from sqlalchemy import Column, Integer, String, Date, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class DocumentsTable(Base):
    __tablename__ = 'documents_table'

    document_id = Column(Integer, primary_key=True)
    path = Column(Text)
    reference_month = Column(String)
    pipe_name = Column(String)
    pipe_id = Column(Integer)
    freq = Column(String)
    document_name = Column(String)
    source_name = Column(String)
    escope = Column(String)
    current_release_date = Column(Integer, nullable=True)
    hash = Column(String, nullable=True)
    next_release_date = Column(Integer, nullable=True)
    next_release_time = Column(Integer, nullable=True)

    prompts = relationship("PromptsTable", back_populates="document")
    summaries = relationship("SummaryTable", order_by="SummaryTable.summary_id", back_populates="document")
    key_takeaways = relationship("KeyTakeawaysTable", order_by="KeyTakeawaysTable.takeaway_id", back_populates="document")
    analyses = relationship("AnalysisTable", order_by="AnalysisTable.analysis_id", back_populates="document")
