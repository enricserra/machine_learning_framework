from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, Boolean, Sequence, ForeignKey
from sqlalchemy.orm import sessionmaker


class FileAttributes(Base):
    __tablename__ = "files_attrs_table"

    attr_id = Column(Integer, Sequence('file_attr_id'), primary_key=True)
    file_path = Column(String)
    def __repr__(self):
        return "<SomeTable(attr_id='%s', file_path='%s')>" % (
            self.attr_id, self.file_path
        )


class AttributesSummary(Base):
    __tablename__ = "attributes_summary"

    plot_title = Column(String)
    attr_id = Column(Integer, primary_key=True)
    plot_minimum = Column(Float)
    plot_maximum = Column(Float)
    plot_values = Column(String)
    is_numeric = Column(Boolean)