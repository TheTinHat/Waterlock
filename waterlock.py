import os
import logging
from shutil import copy2, disk_usage, move
from hashlib import blake2b
from time import time
from socket import gethostname
from sqlalchemy import create_engine, exists
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from db_classes import Files, Jobs, FileVersions


class Waterlock():
    def __init__(self, engine_path='sqlite:///config.db'):
        self.engine = create_engine(engine_path)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        self.Base = declarative_base()
        Jobs.metadata.create_all(self.engine)
        logging.basicConfig(filename='waterlock.log', \
                            filemode='w', \
                            level=logging.INFO, \
                            format='%(name)s - %(levelname)s - %(asctime)s - %(message)s')


    @property
    def job_count(self):
        return self.session.query(Jobs).count()


    def initialize(self,
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

        assert os.path.isabs(source_directory), \
            "Error, source directory path is not absolute"
        assert os.path.isabs(destination_directory), \
            "Error, destination directory path is not absolute"

        source_directory = self._sanitize(source_directory)
        destination_directory = self._sanitize(destination_directory)
        middle_directory = self._sanitize(middle_directory)
        destination_directory = '/'.join([destination_directory, str(job_name)])
        middle_directory = '/'.join([middle_directory, str(job_name)])
        os.makedirs(middle_directory, exist_ok=True)

        if session.query(exists().where(Jobs.name==job_name)).scalar() is False:
            config = Jobs( \
                        name = job_name,
                        source_directory = source_directory, \
                        middle_directory = middle_directory, \
                        destination_directory = destination_directory, \
                        reserved_space = reserved_space * 2**30, \
                        sync_deletions = sync_deletions, \
                        dump_cargo = dump_cargo,
                        hostname = gethostname(),
                        days_to_prune = days_to_prune)

            session.add(config)
            session.commit()

        logging.info('Initialized %s', job_name)
        return True


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


    def _sanitize(self, file):
        file = file.replace("\\","/").split('/')
        file = [x for x in file if x != '']
        file = '/'.join(file)
        return file


    def _hash(self, file):
        blake = blake2b()
        with open(file, 'rb') as f:
            chunk = f.read(32768)
            while len(chunk) > 0:
                blake.update(chunk)
                chunk = f.read(32768)
        return str(blake.hexdigest())


    def _add_new_files(self, job):
        logging.info('Scanning for new files for %s', job)
        src, mid, dst = self.session.query(\
            Jobs.source_directory, \
            Jobs.middle_directory, \
            Jobs.destination_directory)\
            .where(Jobs.name == job).one()

        for folder, _, file_list in os.walk(src):
            folder = self._sanitize(folder)
            for file in file_list:
                source = self._sanitize(file)
                source = '/'.join([folder, source])
                middle = source.replace(src, mid)
                dest_path = source.replace(src, dst)
                if self.session.query(exists().where(\
                        Files.source == source)).scalar() is False:

                    new_file = Files( \
                        source = source,
                        middle = middle,
                        destination = dest_path,
                        progress = 0,
                        size = os.path.getsize(source),
                        modtime = os.path.getmtime(source),
                        job = job)
                    self.session.add(new_file)
                    self.session.commit()
        return True


    def _refresh_source(self, job):
        logging.info('Scanning for file changes for %s', job)
        self._add_new_files(job)
        file_list = self.session.query(Files)\
            .where(Files.job == job).all()
        for file in file_list:
            if os.path.exists(file.source):
                if os.path.getmtime(file.source) > file.modtime:
                    logging.info('Updating %s', file.source)
                    self._create_db_version(file)
                    self._update_modtime(file)
                    self._update_size(file)
                    self._update_checksum(file)
            if file.checksum is None:
                self._update_checksum(file)


    def _update_modtime(self, file):
        mod_time = os.path.getmtime(file.source)
        self.session.query(Files).where(Files.source == file.source).\
            update({'modtime' : mod_time,
                    'progress' : 0})
        self.session.commit()
        return mod_time


    def _update_size(self, file):
        new_size = os.path.getsize(file.source)
        self.session.query(Files).where(Files.source == file.source).\
            update({'size' : new_size})
        self.session.commit()
        return new_size


    def _update_checksum(self, file):
        checksum = self._hash(file.source)
        self.session.query(Files).where(Files.source == file.source).\
            update({'checksum' : checksum})
        self.session.commit()
        return checksum


    def start(self, job, same_system=False):
        source_free, _, _ = self.free_space(job)
        hostname, destination_directory, reserved, days_to_prune = self.session.query(\
                Jobs.hostname, Jobs.destination_directory,\
                Jobs.reserved_space, Jobs.days_to_prune)\
                .where(Jobs.name == job).one()

        if gethostname() != hostname or same_system is True:
            os.makedirs(destination_directory, exist_ok=True)

        if source_free:
            self._refresh_source(job)

        file_list = self.session.query(Files)\
            .where(Files.job == job, Files.progress != 2).all()

        for file in file_list:
            _, mid_free, dst_free = self.free_space(job)
            space_needed = reserved + file.size

            if same_system is True:
                os.makedirs(os.path.dirname(file.destination), exist_ok=True)

            if file.progress == 0 and mid_free and mid_free > space_needed:
                self._copy_cargo(file.source, file.middle, file.progress, file.checksum)

            elif file.progress == 1 and dst_free and dst_free > space_needed:
                if os.path.exists(file.destination):
                    self._archive_version(file)
                    self._prune_versions(file.job, days_to_prune)

                self._copy_cargo(file.middle, file.destination, file.progress, file.checksum)

            elif mid_free < space_needed or dst_free < space_needed:
                logging.error("Not enough space on copy target! Ending...")
                return False
        return True


    def _copy_cargo(self, src, dst, progress, checksum):
        logging.info('Copying %s', src)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        copy2(src, dst)
        result_checksum = self._hash(dst)

        if result_checksum == checksum:
            self.session.query(Files).where(Files.source == src).\
                update({'progress' : progress + 1})

        else:
            raise Exception(f'Checksums do not match for {src}')


    def _create_db_version(self, file):
        directory = self._sanitize(os.path.dirname(file.destination))
        file_name = file.destination.split('/')[-1]
        file_name = '_'.join([file_name, str(file.modtime)])
        file_name = '/'.join([directory, '.archive', file_name])
        version = FileVersions(version = file_name, \
                            destination = file.destination,\
                            size = file.size,\
                            checksum = file.checksum,\
                            modtime = file.modtime,\
                            job = file.job,\
                            status = 'pending')
        self.session.add(version)
        self.session.query(FileVersions).where(\
                            FileVersions.status == 'pending',\
                            FileVersions.destination == file.destination,\
                            FileVersions.checksum != file.checksum).delete()
        self.session.commit()
        return True


    def _archive_version(self, file):
        old_modtime = os.path.getmtime(file.destination)
        version = self.session.query(FileVersions.version).where( \
                            FileVersions.destination == file.destination,\
                            FileVersions.modtime == old_modtime,\
                            FileVersions.status == 'pending'\
                            ).one()[0]
        logging.info('Versioning: %s', version)
        os.makedirs(os.path.dirname(version), exist_ok=True)
        move(file.destination, version)
        self.session.query(FileVersions).where(FileVersions.version == version).\
            update({'status' : 'success'})
        self.session.commit()
        return True


    def _prune_versions(self, job, days):
        seconds = days * 86400
        now = time()
        prune_threshold = now - seconds
        old_versions = self.session.query(FileVersions).where(\
                            FileVersions.modtime < prune_threshold,\
                            FileVersions.job == job,\
                            FileVersions.status == 'success').all()

        for version in old_versions:
            logging.info('Pruning %s', version.version)
            os.remove(version.version)
            self.session.query(FileVersions).where(\
                            FileVersions.checksum == version.checksum).delete()
            self.session.commit()
        return True


    def import_destination(self, job):
        dst = self.session.query(Jobs.destination_directory)\
                .where(Jobs.name == job).one()[0]
        logging.info('Beginning analysis of destination: %s', dst)

        for folder, _, file_list in os.walk(dst):
            folder = self._sanitize(folder)
            for file in file_list:
                destination = self._sanitize(file)
                destination = '/'.join([folder, destination])
                logging.info('Analyzing %s', destination)
                checksum = self._hash(destination)
                dest_time = os.path.getmtime(destination)
                if self.session.query(exists().where(\
                            Files.checksum == checksum)).scalar() is True:
                    logging.info('Recognized file: %s', destination)
                    self.session.query(Files).where(Files.destination == destination)\
                            .update({'progress' : 2})
                elif self.session.query(exists().where(\
                            Files.destination == destination,
                            Files.checksum != checksum,
                            Files.modtime > dest_time)).scalar() is True:
                    logging.info('Recognized older version of file: %s', destination)
                    self.session.query(Files).where(Files.destination == destination)\
                            .update({'progress' : 0, 'modtime' : dest_time})
                if self.session.query(exists().where(\
                            Files.destination == destination)).scalar() is False:
                    logging.warning('Unrecognized file on destination: %s', destination)
        return True


    def change_destination(self, job, new_path):
        pass


    def clear_cargo(self):
        pass


    def verify_destination(self):
        pass


    def restore(self, job):
        #provide list of restore points for user to select from
        pass


    def reset(self):
        pass

'''Features
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
    - Reset incomplete file transfers'''