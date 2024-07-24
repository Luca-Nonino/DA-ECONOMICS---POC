# data/database/models.py
from sqlalchemy import Column, Integer, String, Text, Date, ForeignKey, create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

Base = declarative_base()

# User model
class User(Base):
    __tablename__ = 'users_table'
    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True, nullable=False)
    password = Column(String, nullable=False)

# DocumentsTable model
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
    country = Column(String, nullable=True)  # New column added

    prompts = relationship("PromptsTable", back_populates="document")
    summaries = relationship("SummaryTable", order_by="SummaryTable.summary_id", back_populates="document")
    key_takeaways = relationship("KeyTakeawaysTable", order_by="KeyTakeawaysTable.takeaway_id", back_populates="document")
    analyses = relationship("AnalysisTable", order_by="AnalysisTable.analysis_id", back_populates="document")

# PromptsTable model
class PromptsTable(Base):
    __tablename__ = 'prompts_table'

    prompt_id = Column(Integer, primary_key=True)
    document_id = Column(Integer, ForeignKey('documents_table.document_id'))
    persona_expertise = Column(Text)
    persona_tone = Column(Text)
    format_input = Column(Text)
    format_output_overview_title = Column(Text)
    format_output_overview_description = Column(Text)
    format_output_overview_enclosure = Column(Text)
    format_output_overview_title_enclosure = Column(Text)
    format_output_key_takeaways_title = Column(Text)
    format_output_key_takeaways_description = Column(Text)
    format_output_key_takeaways_enclosure = Column(Text)
    format_output_key_takeaways_title_enclosure = Column(Text)
    format_output_macro_environment_impacts_title = Column(Text)
    format_output_macro_environment_impacts_description = Column(Text)
    format_output_macro_environment_impacts_enclosure = Column(Text)
    tasks_1 = Column(Text)
    tasks_2 = Column(Text, nullable=True)
    tasks_3 = Column(Text, nullable=True)
    tasks_4 = Column(Text, nullable=True)
    tasks_5 = Column(Text, nullable=True)
    audience = Column(Text)
    objective = Column(Text)
    constraints_language_usage = Column(Text)
    constraints_language_style = Column(Text)
    constraints_search_tool_use = Column(Text)

    document = relationship("DocumentsTable", back_populates="prompts")

# SummaryTable model
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

# KeyTakeawaysTable model
class KeyTakeawaysTable(Base):
    __tablename__ = 'key_takeaways_table'

    takeaway_id = Column(Integer, primary_key=True)
    document_id = Column(Integer, ForeignKey('documents_table.document_id'))
    release_date = Column(Date)
    title = Column(Text)
    content = Column(Text)

    document = relationship("DocumentsTable", back_populates="key_takeaways")

# AnalysisTable model
class AnalysisTable(Base):
    __tablename__ = 'analysis_table'

    analysis_id = Column(Integer, primary_key=True)
    document_id = Column(Integer, ForeignKey('documents_table.document_id'))
    release_date = Column(Date)
    topic = Column(Text)
    content = Column(Text)

    document = relationship("DocumentsTable", back_populates="analyses")

def create_all_tables(engine_url='sqlite:///data/database/database.sqlite'):
    engine = create_engine(engine_url)
    Base.metadata.create_all(engine)

# Example usage
if __name__ == "__main__":
    create_all_tables()
