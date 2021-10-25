from hashlib import blake2b
from shutil import copy2, disk_usage, rmtree
from time import process_time

import os
import sqlite3


'''===== IF RUNNING AS SCRIPT CHANGE THE FOLLOWING FOLDERS ====='''
# Absolute File Paths Only! Add comma-separated paths (e.g. a list of strings) to support multiple directories
# If using multiple source and end directories, ensure they are in the same order! See example in comment below.
source_directory = ['/ABSOLUTE/PATH/TO/FOLDER/'] # ['/ABSOLUTE/PATH/ONE', '/ABSOLUTE/PATH/TWO']
end_directory = ['/ABSOLUTE/PATH/TO/FOLDER/'] # ['/ABSOLUTE/PATH/ONE', '/ABSOLUTE/PATH/TWO']
reserved_space = 1 # Enter value in Gibibytes

'''============================================================='''


class Waterlock():
    def __init__(self,
                source_directory='',
                end_directory='',
                reserved_space=1):
        self.source_directory = self.sanitize(source_directory)
        self.end_directory = self.sanitize(end_directory)
        self.make_name()
        self.check_config()
        self.reserved_space = reserved_space * 2**30
        self.con, self.cur = self.connect_db()
        self.retry_count = 0

    def check_config(self):
        if 'ABSOLUTE/PATH/TO/FOLDER' in self.source_directory \
            or 'ABSOLUTE/PATH/TO/FOLDER' in self.end_directory:
            raise Exception("Error: change the configuration at the top fo the script first!")
        if os.path.isabs(self.source_directory) is False or os.path.isabs(self.end_directory) is False:
            raise Exception('Error: relative path detected. Waterlock only accepts absolute file paths!')

    def connect_db(self):
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        cur.execute('CREATE TABLE IF NOT EXISTS data \
            (path TEXT UNIQUE, last_modified INT, hash TEXT, middle INTEGER, end INTEGER)')
        con.commit()
        return con, cur


    def make_name(self):
        x = self.source_directory.split('/')[-1]
        self.middle_directory = 'cargo/' + str(x)
        if os.path.exists(self.middle_directory) is False:
            os.makedirs(self.middle_directory)
        self.db_name = 'config/' + str(x) + '.db'
        print(self.middle_directory)
        print(self.db_name)


    def sanitize(self, path):
        path = path.replace("\\","/").split('/')
        path = [x for x in path if x != '']
        path = '/'.join(path)
        return path


    def hash(self, file_path):
            file_hash = blake2b() 
            with open(file_path, 'rb') as f:
                fb = f.read(32768) 
                while len(fb) > 0:
                    file_hash.update(fb) 
                    fb = f.read(32768) 
            return str(file_hash.hexdigest())
    

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


    def sizeof(self, num, suffix="B"):
        for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
            if abs(num) < 1024.0:
                return f"{num:3.1f}{unit}{suffix}"
            num /= 1024.0
        return f"{num:.1f}Yi{suffix}"


    def check_space(self):
        if self.stage == "middle":
            return disk_usage(self.middle_directory)[2]
        if self.stage == "end":
            return disk_usage(self.end_directory)[2]


    def detect_stage(self):
        if os.path.exists(self.source_directory) \
                and os.path.exists(self.middle_directory) \
                and os.path.exists(self.end_directory):
            raise Exception('All directories are already present. Aborting...')

        elif os.path.exists(self.source_directory) and os.path.exists(self.middle_directory):
            self.stage = "middle"
            self.reset()
            self.check_modify()
            print(f"Moving data to {self.middle_directory}")

        elif os.path.exists(self.middle_directory) and os.path.exists(self.end_directory):
            self.stage = "end"
            print(f"Moving data to {self.end_directory}")
        
        elif os.path.exists(self.source_directory) is False:
            raise Exception('Source directory not found')
        return self.stage


    def refresh_src_files(self):
        for folder, _, file_list in os.walk(self.source_directory):
            for file in file_list:
                path = folder.replace("\\","/").split('/')
                path += [file]
                path = [x for x in path if x != '']
                path = '/'.join(path)
                modify_time = os.path.getmtime(path)
                self.cur.execute('INSERT OR IGNORE INTO data VALUES (?,?,?,?,?)', (path, modify_time, '', 0, 0))
        self.con.commit()
        return True


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

        elif self.stage == "end":
            dst = src.replace(self.source_directory, self.end_directory)
            src = src.replace(self.source_directory, self.middle_directory)

        dst = self.sanitize(dst)

        return src, dst


    def move(self, src, dst):
        dst_dir = os.path.dirname(dst)
        if os.path.exists(dst):
            if os.path.getsize(src) != os.path.getsize(dst):
                os.remove(dst)

        file_size = os.path.getsize(src)
        if file_size > (self.free_space - self.reserved_space):
            print(f'Only {int(self.free_space/2**30)} GiB left, stopping...')
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


    def start(self):
        print("Starting up Waterlock, preparing to transfer...")
        self.stage = self.detect_stage()
        self.get_file_list()
        files_left = len(self.file_list)
        file_count = 1
        start = process_time()
        for file in self.file_list:
            self.free_space = self.check_space()
            src, dst = self.format_paths(file[0])
            print(f'{file_count}/{files_left} ({self.sizeof(os.path.getsize(src))}):  {src}')
            self.move(src, dst)
            file_count += 1
        end = process_time() - start
        self.success = True
        print(f"Complete! Finished in {end} seconds")
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


    def verify_middle(self):
        print("Beginning full file verification of middle")
        self.cur.execute('SELECT path, hash FROM data WHERE middle = 1 and end = 0')
        files_to_verify = self.cur.fetchall()
        for src, hash1 in files_to_verify:
            src, dst = self.format_paths(src)
            hash2 = self.hash(dst)
            if hash1 != hash2:
                raise Exception(f'Error: destination file hash does not match for source file: {src}')
        print('Success! Middle files match stored hashes of source files')
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
        print('Success! Destination files match stored hashes of source files')
        return True


    def reset(self):
        self.cur.execute('SELECT path FROM data WHERE middle = 1 and end = 0')
        pending_paths = self.cur.fetchall()
        for file_path in pending_paths:
            file_path = file_path[0]
            mid_path = file_path.replace(self.source_directory, self.middle_directory)
            mid_path = self.sanitize(mid_path)
            if os.path.exists(mid_path) is False:
                self.cur.execute('UPDATE data SET middle = 0, end = 0 WHERE path = ?', (file_path,))
                self.con.commit() 
                print(f'{file_path} is missing from middle location, marking as unmoved in database')
        return True


    def check_modify(self):
        self.cur.execute('SELECT path, last_modified FROM data')
        files = self.cur.fetchall()
        for file, last_modified in files:
            modify_time = os.path.getmtime(file)
            if modify_time > last_modified:
                hash = self.hash(file)
                self.cur.execute('UPDATE data SET last_modified = ?, \
                middle = 0, end = 0, hash = ? WHERE path = ?', (modify_time, hash, file))
                print(f'{file} was modified since last transfer, marking as unmoved in database') 
        return True

    def dump_cargo(self):
        if self.stage == "end" and self.success == True:
            dump = input('Do you want to dump cargo? [Yes, No]: ')
            if dump == 'Yes':
                    rmtree('cargo/*')
                    os.mkdir(self.middle_directory)
            else:
                input('Press ENTER to exit')
                

if __name__ == "__main__":

    if len(source_directory) != len(end_directory):
        raise Exception("Error: different number of source and end directories.")

    for i in range(len(source_directory)):
        wl = Waterlock( source_directory=source_directory[i],
                        end_directory=end_directory[i], 
                        reserved_space=reserved_space
                        )
        wl.start()
        
    #   wl.dump_cargo()
    #   wl.verify_middle()
    #   wl.verify_destination()

        del wl
    