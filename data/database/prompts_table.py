# data/database/prompts_table.py
from sqlalchemy import Column, Integer, Text, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from data.database.documents_table import Base, DocumentsTable

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
