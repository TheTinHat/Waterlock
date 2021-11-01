from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.sqltypes import REAL

Base = declarative_base()

class Files(Base):
    __tablename__ = 'files'

    source_path = Column(String, unique=True, primary_key=True)
    middle_path = Column(String)
    destination_path = Column(String)
    size = Column(Integer)
    checksum = Column(String)
    last_modtime_ms = Column(Integer) #Must use milisecond time as integer!
    progress = Column(String)
    job = Column(String, ForeignKey('jobs.name'))

class FileVersions(Base):
    __tablename__ = 'fileversions'
    version_path = Column(String, unique=True, primary_key=True)
    destination_path = Column(String, ForeignKey('files.destination_path'))
    size = Column(Integer)
    checksum = Column(String)
    last_modtime_ms = Column(Integer) #Must use milisecond time as integer!
    job = Column(String, ForeignKey('jobs.name'))
    status = Column(String)


class Jobs(Base):
    __tablename__ = 'jobs'

    name = Column(String, primary_key=True)
    source_directory = Column(String)
    middle_directory = Column(String)
    destination_directory = Column(String)
    reserved_space = Column(Integer)
    sync_deletions = Column(String)
    dump_cargo = Column(String)
    hostname = Column(String)
    days_to_prune = Column(Integer)




