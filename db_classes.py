from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.sqltypes import REAL


Base = declarative_base()
class Files(Base):
    __tablename__ = 'files'
    rel_path = Column(String, primary_key=True)
    job = Column(String, ForeignKey('jobs.name'), primary_key=True)
    size = Column(Integer)
    checksum = Column(String)
    modtime = Column(REAL)
    progress = Column(Integer)


class Versions(Base):
    __tablename__ = 'fileversions'
    version_path = Column(String, unique=True, primary_key=True)
    rel_path = Column(String, ForeignKey('files.rel_path'))
    job = Column(String, ForeignKey('jobs.name'))
    size = Column(Integer)
    modtime = Column(REAL)



class Jobs(Base):
    __tablename__ = 'jobs'
    name = Column(String, primary_key=True)
    src_dir = Column(String)
    mid_dir = Column(String)
    dst_dir = Column(String)
    reserved = Column(Integer)
    sync_deletions = Column(String)
    hostname = Column(String)
    prune_age = Column(Integer)