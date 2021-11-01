from tools import sanitize, hash, ms_modtime
from shutil import copy2, disk_usage, move
from sqlalchemy import Column, Integer, String, create_engine, exists, or_
from sqlalchemy.ext.declarative import declarative_base 
from sqlalchemy.orm import sessionmaker, Session, Query
from db_classes import Files, Jobs, FileVersions
import os
from socket import gethostname
from time import time



class Waterlock():
    def __init__(self, engine_path='sqlite:///config.db'):
        self.engine = create_engine(engine_path)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        self.Base = declarative_base()
        Jobs.metadata.create_all(self.engine)
    
    @property
    def job_count(self):
        return self.session.query(Jobs).count()

    def Initialize(self,
                    job_name = '',
                    source_directory = '',
                    destination_directory = '',
                    middle_directory = 'cargo',
                    reserved_space = 1,
                    sync_deletions = False,
                    dump_cargo = False,
                    days_to_prune = 90
                    ):
        session = self.session
        assert os.path.isabs(source_directory), "Error, source directory path is not absolute"
        assert os.path.isabs(destination_directory), "Error, destination directory path is not absolute"
        source_directory = sanitize(source_directory)
        destination_directory = sanitize(destination_directory)
        middle_directory = sanitize(middle_directory)
        destination_directory = '/'.join([destination_directory, str(job_name)])
        middle_directory = '/'.join([middle_directory, str(job_name)])
        os.makedirs(middle_directory, exist_ok=True)
        
        if session.query(exists().where(Jobs.name==job_name)).scalar() is False:
            config = Jobs( \
                name = job_name,
                source_directory = source_directory,              \
                middle_directory = middle_directory,              \
                destination_directory = destination_directory,    \
                reserved_space = reserved_space * 2**30,          \
                sync_deletions = sync_deletions,                  \
                dump_cargo = dump_cargo,
                hostname = gethostname(),
                days_to_prune = days_to_prune)
            
            session.add(config)
            session.commit()

        return True


    def edit_job(self, job, **kwargs):
        for key, value in kwargs.items():
            self.session.query(Jobs).where(Jobs.name == job).\
                update({str(key) : value})
        self.session.commit()        

    def edit_all_jobs(self, **kwargs):
        for key, value in kwargs.items():
            self.session.query(Jobs).\
                update({str(key) : value})
        self.session.commit()   

    def free_space(self, job):
        src, mid, dst = self.session.query(Jobs.source_directory, \
            Jobs.middle_directory, \
            Jobs.destination_directory)\
            .where(Jobs.name == job).one()

        source_free = middle_free = destination_free = False

        if os.path.exists(src):
            source_free = disk_usage(src)[2]

        if os.path.exists(mid):
            middle_free = disk_usage(mid)[2]

        if os.path.exists(dst):
            destination_free = disk_usage(dst)[2]
        
        return source_free, middle_free, destination_free
                

    def add_new_files(self, job):
        src, mid, dst = self.session.query(\
            Jobs.source_directory, \
            Jobs.middle_directory, \
            Jobs.destination_directory)\
            .where(Jobs.name == job).one()

        for folder, _, file_list in os.walk(src):
            folder = sanitize(folder)
            for file in file_list:
                source_path = sanitize(file)
                source_path = '/'.join([folder, source_path])
                middle_path = source_path.replace(src, mid)
                dest_path = source_path.replace(src, dst)
                if self.session.query(exists().where(\
                        Files.source_path == source_path)).scalar() is False:

                    new_file = Files( \
                        source_path = source_path,
                        middle_path = middle_path,
                        destination_path = dest_path,
                        progress = "source",
                        size = os.path.getsize(source_path),
                        last_modtime_ms = ms_modtime(source_path),
                        job = job)
                    self.session.add(new_file)
                    self.session.commit()
        return True
    
    def refresh_source(self, job):
        self.add_new_files(job)
        file_list = self.session.query(Files)\
            .where(Files.job == job).all()
        for file in file_list:
            if os.path.exists(file.source_path):
                if ms_modtime(file.source_path) != file.last_modtime_ms or \
                        os.path.getsize(file.source_path) != file.size:
                    print(f'Updating {file.source_path}')
                    self.create_db_version(file)
                    self.update_modtime(file)
                    self.update_size(file)
                    self.update_checksum(file)
            if file.checksum == None:
                self.update_checksum(file)


    def update_modtime(self, file):
        mod_time = ms_modtime(file.source_path)
        self.session.query(Files).where(Files.source_path == file.source_path).\
            update({'last_modtime_ms' : mod_time,
                    'progress' : 'source'})
        self.session.commit()
        return mod_time


    def update_size(self, file):
        new_size = os.path.getsize(file.source_path)
        self.session.query(Files).where(Files.source_path == file.source_path).\
            update({'size' : new_size})
        self.session.commit()
        return new_size


    def update_checksum(self, file):
        checksum = hash(file.source_path)
        self.session.query(Files).where(Files.source_path == file.source_path).\
            update({'checksum' : checksum})
        self.session.commit()
        return checksum


    # I NEED TO CREATE DIRECTORY ON DESTINATION SYSTEM ONLY IF HOSTNAME != HOSTNAME ON SOURCE

    def start(self, job, same_system=False):
        source_free, _, destination_free = self.free_space(job)
        hostname, destination_directory = self.session.query(\
            Jobs.hostname, Jobs.destination_directory)\
            .where(Jobs.name == job).one()

        if gethostname() != hostname or same_system == True:
            os.makedirs(destination_directory, exist_ok=True)
        
        if source_free:
            self.refresh_source(job)

        file_list = self.session.query(Files)\
            .where(Files.job == job, Files.progress != "destination").all()
        
        free_space = True
        for file in file_list:
            if free_space == True:
                free_space = self.copy_cargo(file)


    def create_db_version(self, file):
        directory = sanitize(os.path.dirname(file.destination_path))
        file_name = file.destination_path.split('/')[-1]
        file_name = '_'.join([file_name, str(file.last_modtime_ms)])
        file_name = '/'.join([directory, '.archive', file_name])
        version = FileVersions( \
                    version_path = file_name, \
                    destination_path = file.destination_path,
                    size = file.size,
                    checksum = file.checksum,
                    last_modtime_ms = file.last_modtime_ms,
                    job = file.job,
                    status = 'pending'
                )
        self.session.add(version)
        self.session.query(FileVersions).where(\
            FileVersions.status == 'pending',\
            FileVersions.destination_path == file.destination_path,\
            FileVersions.checksum != file.checksum
            ).delete()
        self.session.commit()
        return True


    def archive_version(self, file):
        old_modtime = ms_modtime(file.destination_path)
        version_path = self.session.query(FileVersions.version_path).where(\
            FileVersions.destination_path == file.destination_path,
            FileVersions.last_modtime_ms == old_modtime, \
            FileVersions.status == 'pending'
            ).one()[0]
        print(f'Versioning: {version_path}')
        os.makedirs(os.path.dirname(version_path), exist_ok=True)
        move(file.destination_path, version_path)
        self.session.query(FileVersions).where(FileVersions.version_path == version_path).\
            update({'status' : 'success'})
        self.session.commit()
        return True


    def prune_versions(self, job, days):
        miliseconds = days * 86400 * 1000
        now = time() * 1000
        prune_threshold = now - miliseconds
        old_versions = self.session.query(FileVersions).where(\
            FileVersions.last_modtime_ms < prune_threshold,\
            FileVersions.job == job,\
            FileVersions.status == 'success').all()
        
        for version in old_versions:
            print(f'Pruning {version.version_path}')
            os.remove(version.version_path)
            self.session.query(FileVersions).where(\
                FileVersions.checksum == version.checksum).delete()
            self.session.commit()
        return True


    def copy_cargo(self, file):
        _, mid_free, dst_free = self.free_space(file.job)
        reserved, days_to_prune = self.session.query(Jobs.reserved_space, \
            Jobs.days_to_prune).where(Jobs.name == file.job).one()
        space_needed = reserved + file.size

        if file.progress == 'source' and mid_free and mid_free > space_needed:
            print(f'Copying {file.source_path}')
            os.makedirs(os.path.dirname(file.middle_path), exist_ok=True)
            copy2(file.source_path, file.middle_path)
            result_checksum = hash(file.middle_path)
            if result_checksum == file.checksum:
                self.session.query(Files).where(Files.source_path == file.source_path).\
                    update({'progress' : 'middle'})
            else:
                raise Exception(f'Checksums do not match for {file.source_path}')
    
        elif file.progress == 'source' and mid_free and mid_free < space_needed:
            print("Out of space! Skipping...")
            return False            

        elif file.progress == 'middle' and dst_free and dst_free > space_needed:
            print(f'Copying {file.middle_path}')
            if os.path.exists(file.destination_path):
                self.archive_version(file)
                self.prune_versions(file.job, days_to_prune)
            os.makedirs(os.path.dirname(file.destination_path), exist_ok=True)
            copy2(file.middle_path, file.destination_path)
            result_checksum = hash(file.destination_path)
            if result_checksum == file.checksum:
                self.session.query(Files).where(Files.source_path == file.source_path).\
                    update({'progress' : 'destination'})
            else:
                raise Exception(f'Checksums do not match for {file.source_path}')
        
        elif file.progress == 'middle' and dst_free and dst_free < space_needed:
            print("Out of space! Skipping...")
            return False
        self.session.commit()
        return True


    def change_destination(self, job, new_path):
        pass


'''Features
    -File encryption
    - Remove on destination
    - Clear Cargo
    - Full Hash Verification
    - Restore
        - to point in time
    - File Versioning
        - Create separate table for old files, create function to cycle files there with name_timestamp
        - For files smaller than X
        - Add pruning of older_than files
    - SQLite database backup
    - Exclusion Filters
    - Compression
    - Command Line Arguments
    - Reset incomplete file transfers




'''

## BELOW IS HOW QUERIES WORK
'''q = session.query(Jobs).where(Jobs.source_directory==source_directory).all()
for x in q:
    print(x.source_directory)'''



'''    def free_space(self, file):
        reserved = self.session.query(Jobs.reserved_space).where(Jobs.name == file.job).one()[0]

        if file.progress == 'source':
            if disk_usage(file.middle_path)[2] > reserved:
                return True
        elif file.progress == 'middle':
            if disk_usage(file.destination_path)[2] > reserved:
                return True
        else:
            return False'''