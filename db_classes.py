from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Files(Base):
    __tablename__ = 'files'

    source_path = Column(String, unique=True, primary_key=True)
    middle_path = Column(String)
    destination_path = Column(String)
    size = Column(Integer)
    checksum = Column(String)
    last_mod_time = Column(Integer)
    progress = Column(String)
    job = Column(String, ForeignKey('jobs.name'))




class Jobs(Base):
    __tablename__ = 'jobs'

    name = Column(String, primary_key=True)
    source_directory = Column(String)
    middle_directory = Column(String)
    destination_directory = Column(String)
    reserved_space = Column(Integer)
    sync_deletions = Column(String)
    dump_cargo = Column(String)





