from hashlib import blake2b
from pathlib import PurePosixPath
from shutil import copy2, disk_usage
from time import process_time

import os
import sqlite3

'''===== IF RUNNING AS SCRIPT CHANGE THE FOLLOWING FOLDERS ====='''

source_directory = '/PATH/TO/FOLDER/'
end_direcotry = '/PATH/TO/FOLDER/'
reserved_space = 1 # Enter value in Gibibytes

'''============================================================='''

class Waterlock():
    def __init__(self,
                source_directory='',
                middle_directory='cargo/',
                end_directory='',
                reserved_space=1):
        self.source_directory = source_directory
        self.middle_directory = middle_directory
        self.end_directory = end_directory
        os.makedirs(self.middle_directory, exist_ok=True)
        self.reserved_space = reserved_space * 2**30
        self.con, self.cur = self.connect_db()
        self.retry_count = 0



    def hash(self, file_path):
            file_hash = blake2b() 
            with open(file_path, 'rb') as f:
                fb = f.read(32768) 
                while len(fb) > 0:
                    file_hash.update(fb) 
                    fb = f.read(32768) 
            return str(file_hash.hexdigest())
    

    def connect_db(self):
        con = sqlite3.connect('waterlock.db')
        cur = con.cursor()
        cur.execute('CREATE TABLE IF NOT EXISTS data \
            (path TEXT UNIQUE, hash TEXT, middle INTEGER, end INTEGER)')
        con.commit()
        return con, cur


    def refresh_src_files(self):
        for folder, _, file_list in os.walk(self.source_directory):
            for file in file_list:
                folder = str(folder).replace("\\","/")
                full_path = str(folder) + '/' + str(file)
                self.cur.execute('INSERT OR IGNORE INTO data VALUES (?,?,?,?)', (full_path, '', 0, 0))
        self.con.commit()
        return True


    def detect_stage(self):
        if os.path.exists(self.source_directory) \
                and os.path.exists(self.middle_directory) \
                and os.path.exists(self.end_directory):
            raise Exception('All directories are already present. Aborting...')

        elif os.path.exists(self.source_directory) and os.path.exists(self.middle_directory):
            self.stage = "middle"
            print(f"Moving data to {self.middle_directory}")

        elif os.path.exists(self.middle_directory) and os.path.exists(self.end_directory):
            self.stage = "end"
            print(f"Moving data to {self.end_directory}")
        
        return self.stage


    def get_file_list(self):
        if self.stage == "middle":
            self.refresh_src_files()
            self.cur.execute('SELECT path FROM data WHERE middle = 0 and end = 0')
            self.file_list = self.cur.fetchall()

        elif self.stage == "end":
            self.cur.execute('SELECT path FROM data WHERE middle = 1 and end = 0')
            self.file_list = self.cur.fetchall()

        return True


    def format_paths(self, src):
        if self.stage == "middle":
            dst = src.replace(self.source_directory, self.middle_directory)
            return src, dst

        elif self.stage == "end":
            dst = src.replace(self.source_directory, self.end_directory)
            src = src.replace(self.source_directory, self.middle_directory)
            return src, dst


    def move(self, src, dst):
        dst_dir = os.path.dirname(dst)
        if os.path.exists(dst):
            if os.path.getsize(src) != os.path.getsize(dst):
                os.remove(dst)

        file_size = os.path.getsize(src)
        if file_size > (self.free_space - self.reserved_space):
            print(f'Only {int(self.free_space/2**30)} Gib left, stopping...')
            quit()
        
        file_hash = self.find_hash(src)

        os.makedirs(dst_dir, exist_ok=True)
        copy2(src,dst)
        verified = self.verify_move(dst, file_hash)

        if verified == False:
            os.remove(dst)
            self.retry_count += 1
            if self.retry_count < 5:
                self.move(src, dst)
            else:
                print('File hashes not matching after 5 attempts. Aborting...')
                quit()
        elif verified == True:
            return True


    def verify_move(self, file_path, hash1):
        hash2 = self.hash(file_path)
        if hash1 == hash2:    
            if self.stage == "middle":
                self.cur.execute('UPDATE data SET middle = 1 WHERE hash = ?', (hash1,))
            elif self.stage == "end":
                self.cur.execute('UPDATE data SET end = 1 WHERE hash = ?', (hash1,))
            self.con.commit()
            self.retry_count = 0
            return True
        else:
            return False


    def find_hash(self, src):
        if self.stage == "middle":
            path = src
        elif self.stage == "end":
            path = src.replace(self.middle_directory, self.source_directory)
        
        self.cur.execute("SELECT hash FROM data WHERE path = ?", (path,))
        file_hash = self.cur.fetchone()[0]
        
        if file_hash == '':
            file_hash = self.hash(src)
            self.cur.execute("UPDATE data SET hash = ? WHERE path = ? AND hash = ''", (file_hash, src))
            self.con.commit()
        return file_hash
        
    def check_space(self):
        if self.stage == "middle":
            return disk_usage(self.middle_directory)[2]
        if self.stage == "end":
            return disk_usage(self.end_directory)[2]

    def start(self):
        self.stage = self.detect_stage()
        self.get_file_list()
        files_left = len(self.file_list)
        file_count = 1
        start = process_time()
        for file in self.file_list:
            self.free_space = self.check_space()
            src, dst = self.format_paths(file[0])
            print(f'({file_count}/{files_left}):   {src}')
            self.move(src, dst)
            file_count += 1
        end = process_time() - start
        print(f"Complete! Finished in {end} seconds")
        return True

    def verify_destination(self):
        print("Beginning full file verification of destination")
        self.cur.execute('SELECT path, hash FROM data WHERE middle = 1 and end = 1')
        files_to_verify = self.cur.fetchall()
        for src, hash1 in files_to_verify:
            src, dst = self.format_paths(src)
            hash2 = self.hash(dst)
            if hash1 != hash2:
                raise Exception(f'Error: destination file hash does not match for source file: {src}')
        print('Success! Hashes of source and destination files match')
        return True

    def verify_middle(self):
        print("Beginning full file verification of middle")
        self.cur.execute('SELECT path, hash FROM data WHERE middle = 1 and end = 0')
        files_to_verify = self.cur.fetchall()
        for src, hash1 in files_to_verify:
            src, dst = self.format_paths(src)
            hash2 = self.hash(dst)
            if hash1 != hash2:
                raise Exception(f'Error: destination file hash does not match for source file: {src}')
        print('Success! Hashes of source and destination files match')
        return True

if __name__ == "__main__":
    wl = Waterlock( source_directory=source_directory, 
                    end_directory=end_direcotry, 
                    reserved_space=reserved_space
                    )
    wl.start()