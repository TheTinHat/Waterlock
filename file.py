from pathlib import Path
import logging
from shutil import copy2, disk_usage, move
from sqlalchemy import exists, and_
from time import time
from functools import cached_property

from tools import *
from db_classes import Files, Versions, DstFiles


class File():
    def __init__(self, Session: object, rel_path: Path, \
                src_dir: Path, mid_dir: Path, dst_dir: Path, \
                job: str, reserved: int=1):
        self.session = Session()
        self.rel_path = make_posix(rel_path)
        self.src_dir = Path(src_dir)
        self.mid_dir = Path(mid_dir)
        self.dst_dir = Path(dst_dir)
        self.job = job
        self.reserved = reserved

        if self.session.query(exists().where(and_(\
                    Files.rel_path == self.rel_path, Files.job == self.job)))\
                    .scalar() is False:
            self.insert_db()


    @cached_property
    def src_path(self):
        return self.src_dir.joinpath(self.rel_path)


    @cached_property
    def mid_path(self):
        return self.mid_dir.joinpath(self.job, self.rel_path)


    @cached_property
    def dst_path(self):
        return self.dst_dir.joinpath(self.job, self.rel_path)


    @property
    def progress(self):
        '''Progress: 0 = source, 1 = middle, 2 = destination'''
        return self.session.query(Files.progress).where( \
                Files.rel_path == self.rel_path, Files.job == self.job).one()[0]


    @property
    def checksum(self):
        try:
           return self.session.query(Files.checksum).where(\
                    Files.rel_path == self.rel_path, Files.job == self.job).one()[0]
        except:
            return hash_this(self.src_path)


    @property
    def size(self):
        try:
            return self.session.query(Files.size).where(\
                    Files.rel_path == self.rel_path, Files.job == self.job).one()[0]
        except:
            return self.src_path.stat().st_size


    @property
    def modtime(self):
        try:
            return self.session.query(Files.modtime).where(\
                    Files.rel_path == self.rel_path, Files.job == self.job).one()[0]
        except:
            return self.src_path.stat().st_mtime


    @property
    def free_space(self):
        '''Check if there's enough space to write file to next lock'''
        if self.progress == 0:
            space_needed = self.reserved + self.src_path.stat().st_size
            disk_free = disk_usage(self.mid_dir)[2]
        elif self.progress == 1:
            space_needed = self.reserved + self.mid_path.stat().st_size
            disk_free = disk_usage(self.dst_dir)[2]
        elif self.progress == -1:
            return True
        if space_needed < disk_free:
            return True
        else:
            return False


    def insert_db(self):
        '''Add new file to the database'''
        new_file = Files( \
            rel_path = self.rel_path,
            progress = 0,
            size = self.size,
            checksum = self.checksum,
            modtime = self.modtime,
            job = self.job)
        self.session.add(new_file)
        self.session.commit()
        return True


    def update_attrs(self):
        self.session.query(Files).where(Files.rel_path == self.rel_path, Files.job == self.job)\
                .update({'checksum': hash_this(self.src_path),
                        'size' : self.src_path.stat().st_size,
                        'modtime' : self.src_path.stat().st_mtime})
        self.session.commit()
        return True


    def mark_for_removal(self):
        self.session.query(Files).where(Files.rel_path == self.rel_path, Files.job == self.job)\
                .update({'progress': -1})
        self.session().commit()
        return True


    def set_progress(self, progress:int):
        self.session.query(Files).where(Files.rel_path == self.rel_path, Files.job == self.job)\
                .update({'progress': progress})
        self.session.commit()
        return self.progress


    def increment_progress(self):
        logging.debug('Incrementing Progress on %s', self.rel_path)
        self.session.query(Files).where(Files.rel_path == self.rel_path, Files.job == self.job)\
                .update({'progress': self.progress + 1})
        self.session.commit()
        return self.progress


    def verify_move(self):
        '''Hash destination file, update progress if hash matches source, otherwise
        set progress to 0 so it gets moved again and delete file on destination'''
        success = self.verify_dst()
        if success is True:
            self.increment_progress()
            return True
        else:
            logging.warning('Hashes do not match. \
                    Resetting progress and deleting %s', self.dst_path)
            self.set_progress(0)
            self.dst_path.unlink()
            return False


    def verify_dst(self):
        '''Hash destination file, check if it matches source checksum'''
        dst_checksum = hash_this(self.dst_path)
        if self.checksum == dst_checksum:
            return True
        else:
            return False


    def verify_mid(self):
        '''Verify the file exists on middle location, otherwise reset progress'''
        if self.progress == 1:
            if self.mid_path.exists() == False:
                logging.debug('File missing from middle. Resetting progress on %s', self.rel_path)
                self.set_progress(0)
        return True


    def archive_version(self):
        '''Move version to .archive folder, append mtime to filename, add to database'''
        version_time = self.dst_path.stat().st_mtime
        version_name = ''.join([Path(self.rel_path).name, '_', str(version_time)])
        path_variables = [self.job, '.archive', Path(self.rel_path).parent, version_name]
        version_path = self.dst_dir.joinpath(*path_variables)
        logging.info('Archiving previous file version to %s', str(version_path))
        version_path.parent.mkdir(parents=True, exist_ok=True)
        move(self.dst_path, version_path)

        if version_path.exists():
            new_version = Versions(
                    version_path = str(version_path),
                    rel_path = self.rel_path,
                    size = version_path.stat().st_size,
                    modtime = version_time,
                    job = self.job)

            self.session.add(new_version)
            self.session.commit()
            return True
        else:
            logging.warn('Move failed while archiving %s', str(version_path))
            return False


    def prune_versions(self, days):
        '''Delete file versions and database entries that are older than X days'''
        seconds = days * 86400
        cutoff = time() - seconds
        stale_versions = self.session.query(Versions.version_path).where(
                Versions.modtime < cutoff,
                Versions.rel_path == self.rel_path).all()
        for version in stale_versions:
            version = version[0]
            logging.info('Pruning old version of file: %s', version)
            Path(version).unlink()
            if Path(version).exists() == False:
                self.session.query(Versions).where(Versions.version_path == version).delete()
                self.session.commit()
        return True


    def sync_deletions(self, delete_now=False):
        '''Archive destination file if deleted on source,
        or delete entirely if delete flag is True'''
        if self.progress == -1 and delete_now is False:
            self.archive_version()
        elif self.progress == -1 and delete_now is True:
            logging.info('Deleting %s', self.dst_path)
            self.dst_path.unlink()
            self.session.query(Files).where(Files.rel_path == self.rel_path, Files.job == self.job).delete()
            self.session.commit()
        return True


    def check_exists(self):
        '''Check if an older file exists on the destination and archive the version.
        If newer file exists on destination, log an error'''
        if self.dst_path.exists():
            if self.dst_path.stat().st_mtime < self.mid_path.stat().st_mtime:
                self.archive_version()
                return False
            elif self.dst_path.stat().st_mtime > self.mid_path.stat().st_mtime:
                logging.error('Destination file newer than source file: %s', self.dst_path)
                return True
            elif self.dst_path.stat().st_size < self.mid_path.stat().st_size:
                logging.warning('Destination file exists but is smaller than expected.\
                        Likely due to an impartial copy. Replacing %s', self.dst_path)
                self.dst_path.unlink()
                return False
        else:
            return False


    def next_lock(self):
        '''Move file to the next step (e.g. middle or destination)
        and verify it got moved correctly.'''
        progress = self.progress
        if progress == 0:
            logging.debug('Moving file to middle: %s', self.rel_path)
            self.mid_path.parent.mkdir(parents=True, exist_ok=True)
            copy2(self.src_path, self.mid_path)
            if self.src_path.stat().st_size == self.mid_path.stat().st_size:
                self.increment_progress()
                return True

        elif progress == 1:
            if self.check_exists() == False:
                logging.debug('Moving file to destination: %s', self.rel_path)
                self.dst_path.parent.mkdir(parents=True, exist_ok=True)
                move(self.mid_path, self.dst_path)
                self.verify_move()
                return True
        else:
            return False


    def merge_destination(self):
        '''Check if file is in dst_files db,
        and if so update progress to 2 then delete from db'''
        if self.session.query(exists().where(and_(\
                    DstFiles.rel_path == self.rel_path,
                    DstFiles.checksum == self.checksum,
                    DstFiles.job == self.job)))\
                    .scalar() is True:
            self.set_progress(2)
            self.session.query(DstFiles).where(
                    DstFiles.rel_path == self.rel_path,
                    DstFiles.checksum == self.checksum,
                    DstFiles.job == self.job).delete()
            return True
        else:
            return False