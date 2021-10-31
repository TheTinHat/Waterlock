from tools import sanitize, hash
from shutil import copy2, disk_usage
from sqlalchemy import Column, Integer, String, create_engine, exists, or_
from sqlalchemy.ext.declarative import declarative_base 
from sqlalchemy.orm import sessionmaker, Session, Query
from db_classes import Files, Jobs
import os



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
                    dump_cargo = False
                    ):
        session = self.session
        assert os.path.isabs(source_directory), "Error, source directory path is not absolute"
        assert os.path.isabs(destination_directory), "Error, destination directory path is not absolute"
        source_directory = sanitize(source_directory)
        destination_directory = sanitize(destination_directory)
        middle_directory = sanitize(middle_directory)
        destination_directory = '/'.join([destination_directory, str(job_name)])
        middle_directory = '/'.join([middle_directory, str(job_name)])
        
        if session.query(exists().where(Jobs.name==job_name)).scalar() is False:
            config = Jobs( \
                name = job_name,
                source_directory = source_directory,              \
                middle_directory = middle_directory,              \
                destination_directory = destination_directory,    \
                reserved_space = reserved_space * 2**30,          \
                sync_deletions = sync_deletions,                  \
                dump_cargo = dump_cargo)
            
            session.add(config)
            session.commit()

        return True


    def free_space(self, job):
        src, mid, dst = self.session.query(Jobs.source_directory, \
            Jobs.middle_directory, \
            Jobs.destination_directory)\
            .where(Jobs.name == job).one()

        source_free = middle_free = destination_free = False

        if os.path.exists(src):
            source_free = disk_usage(src)

        if os.path.exists(mid):
            middle_free = disk_usage(mid)

        if os.path.exists(dst):
            destination_free = disk_usage(dst)
        
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
                        last_mod_time = os.path.getmtime(source_path),
                        job = job)
                    self.session.add(new_file)
                    self.session.commit()
        return True
    

    def update_modtime(self, file):
        mod_time = os.path.getmtime(file.source_path)
        self.session.query(Files).where(Files.source_path == file.source_path).\
            update({'last_mod_time' : mod_time,
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


    def start(self, job):
        session = self.session
        self.add_new_files(job)
        file_list = session.query(Files)\
            .where(Files.job == job).all()
        for file in file_list:
            if os.path.getmtime(file.source_path) != file.last_mod_time or \
                    os.path.getsize(file.source_path) != file.size:
                self.update_modtime(file)
                self.update_size(file)
                self.update_checksum(file)
            if file.checksum == None:
                self.update_checksum(file)

    # IN PROGRESS
    '''def copy_cargo(self, file):
        _, mid_free, dst_free = self.free_space(file.job)
        reserved = self.session.query(Jobs.reserved_space).where(Jobs.name == file.job).one()[0]
        space_needed = reserved + file.size

        if file.progress == 'source' and mid_free > space_needed:
                print("Good to go to mid")

        elif file.progress == 'middle' and dst_free > space_needed:
                print("Good to go to dest")
            


            copy2(file.source_path, file.middle_path)
            new_checksum = hash(file.middle_path)
            if new_checksum == file.checksum:
                self.session.query(Files).where(Files.source_path == file.source_path).\
                    update({'progress' : 'middle'})
            else:
                raise Exception(f'Checksums do not match for {file.source_path}')

        elif file.progress == 'middle':
            copy2(file.middle_path, file.destination_path)'''


    def change_destination(self, job, new_path):
        pass



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