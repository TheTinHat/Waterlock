import logging
from pathlib import Path
from socket import gethostname
from sqlalchemy import create_engine, exists
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from tools import *
from db_classes import DstFiles, Files, Jobs
from file import File


logging.basicConfig(filename='waterlock.log', \
                    filemode='w', \
                    level=logging.INFO, \
                    format='%(name)s - %(levelname)s - %(asctime)s - %(message)s')


class Waterlock():
    def __init__(self, engine_path:str='sqlite:///config.db'):
        self.engine = create_engine(engine_path)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        self.Base = declarative_base()
        Jobs.metadata.create_all(self.engine)


    def initialize(self,
                job_name: str = '',
                src_dir: str = '',
                dst_dir: str = '',
                mid_dir: str = 'cargo',
                reserved: int = 1,
                sync_deletions: bool = False,
                prune_age: int = 90
                ):

        src_dir = make_posix(src_dir)
        dst_dir = make_posix(dst_dir)
        mid_dir = make_posix(mid_dir)

        assert Path(src_dir).is_absolute, 'Source directory path must be absolute'
        assert Path(dst_dir).is_absolute, 'Destination directory path must be absolute'

        sync_deletions = 0 if sync_deletions is False else 1

        if self.session.query(exists().where(Jobs.name==job_name)).scalar() is False:
            config = Jobs( \
                    name = job_name,
                    src_dir = str(src_dir),
                    mid_dir = str(mid_dir),
                    dst_dir = str(dst_dir),
                    reserved = reserved * 2 ** 30,
                    sync_deletions = sync_deletions,
                    hostname = gethostname(),
                    prune_age = prune_age)

            self.session.add(config)
            self.session.commit()

        logging.info('Initialized %s', job_name)
        return True


    @property
    def job_count(self):
        return self.session.query(Jobs).count()

    def edit_job(self, job, **kwargs):
        for key, value in kwargs.items():
            self.session.query(Jobs).where(Jobs.name == job)\
                .update({str(key) : value})
        logging.info('Edited %s - %s', job, kwargs)
        self.session.commit()


    def edit_all_jobs(self, **kwargs):
        for key, value in kwargs.items():
            self.session.query(Jobs).\
                update({str(key) : value})
        logging.info('Edited all jobs - %s', kwargs)
        self.session.commit()


    def make_file(self, rel_path, job):
        file = Files(Session=self.Session,
                rel_path=rel_path,
                src_dir=job.src_dir,
                mid_dir=job.mid_dir,
                dst_dir=job.dst_dir,
                job=job.name,
                reserved=job.reserved)
        return file


    def get_job(self, name:str):
        return self.session.query(Jobs).where(Jobs.name == name).one()


    def get_file_list(self, job:str, exclude_moved=False, only_moved=False):
        if exclude_moved == False:
            return self.session.query(Files).where(Files.job == job).all()
        elif exclude_moved == True:
            return self.session.query(Files).where(\
                    Files.job == job, Files.progress < 2).all()
        elif only_moved == True:
            return self.session.query(Files).where(\
                    Files.job == job, Files.progress == 2).all()


    def scan_src(self, job:str):
        job = self.get_job(job)
        file_list = Path(job.src_dir).glob('**/*')
        file_list = [file for file in file_list if file.is_file()]
        src_files = []
        for file in file_list:
            modtime = file.stat().st_mtime
            rel_path = file.relative_to(job.src_dir)
            file = self.make_file(rel_path, job)
            if modtime > file.modtime:
                file.set_progress(0)
                file.update_attrs()
            src_files.append(file)
        return src_files


    def scan_deleted(self, job:str):
        job = self.get_job(job)
        file_list = self.get_file_list(job.name)
        for file in file_list:
            file = self.make_file(file.rel_path, job)
            if file.src_path.exists() is False:
                file.mark_for_removal()
        return True


    def prune(self, job:str):
        job = self.get_job(job)
        file_list = self.get_file_list(job.name)
        for file in file_list:
            file = self.make_file(file.rel_path, job)
            file.prune_versions(job.prune_age)
        return True


    def start_job(self, name:str, same_system=False):
        job = self.get_job(name)

        if job.hostname != gethostname() or same_system == True:
            Path(job.dst_dir).mkdir(parents=True, exist_ok=True)

        if job.hostname == gethostname():
            self.scan_src(job.name)
            self.scan_deleted(job.name)
            Path(job.mid_dir).mkdir(parents=True, exist_ok=True)

        file_list = self.get_file_list(job.name, exclude_moved=True)
        for file in file_list:
            file = self.make_file(file.rel_path, job)
            file.verify_mid()

            if job.sync_deletions == True:
                file.sync_deletions(delete_now=False)

            if file.free_space == True:
                file.next_lock()
        self.prune(job.name)


    def scan_destination(self, job):
        job = self.get_job(job)
        dst_dir_job = job.dst_dir.joinpath(job.name)
        file_list = Path(dst_dir_job).glob('**/*')
        file_list = [file for file in file_list if file.is_file()]
        for file in file_list:
            rel_path = file.relative_to(dst_dir_job)
            dst_file = DstFiles(
                    rel_path = rel_path,
                    job = job.name,
                    checksum = hash_this(file))
            self.session.add(dst_file)
            self.session.commit()
        return True


    def import_destination(self, job:str):
        job = self.get_job(job)
        src_files = self.scan_src(job.name)
        for file in src_files:
            file.merge_destination()
        return True


    def verify_destination(self, job:str):
        job = self.get_job(job)
        file_list = self.get_file_list(job.name, only_moved=True)
        for file in file_list:
            hash_match = file.verify_dst()
            if hash_match == False:
                logging.info('File hash does not match destination: %s', file.rel_path)
        return True


    def restore(self):
        pass
