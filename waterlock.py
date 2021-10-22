import sqlite3
import os
from shutil import copy2, disk_usage
from hashlib import blake2b


'''===== CHANGE THE FOLLOWING FOLDERS ====='''
source_folder = '/PATH/TO/FILE'
end_folder = '/PATH/TO/FILE'
reserved_space = 1 # Enter value in Gibibytes
'''========================================'''

middle_folder = 'cargo/' 
retry_count = 0

def get_hash(path):
    file_hash = blake2b() 
    with open(path, 'rb') as f:
        fb = f.read(32768) 
        while len(fb) > 0:
            file_hash.update(fb) 
            fb = f.read(32768) 
    return str(file_hash.hexdigest())

def move_file(src, dst, stage):
    global retry_count
    global reserved_space
    global source_folder
    global middle_folder
    global end_folder
    con, cur = connect_db()
    file_size = os.path.getsize(src)
    if stage == "middle":
        free_space = disk_usage(middle_folder)[2]
    elif stage == "end":
        free_space = disk_usage(end_folder)[2]
    dst_file = dst + src.split('/')[-1]
    if check_size(src, dst_file) == 2:
        os.remove(dst_file)
    if file_size < (free_space - (reserved_space*2**30)):
        cur.execute("SELECT hash FROM data WHERE path = ?", (src.replace(middle_folder, source_folder),))
        file_hash = cur.fetchone()[0]
        if file_hash == '':
            file_hash = get_hash(src)
            cur.execute("UPDATE data SET hash = ? WHERE path = ? AND hash = ''", (file_hash, src))
            con.commit()
        os.makedirs(dst, exist_ok=True)
        copy2(src, dst_file)
        dst_hash = get_hash(dst_file)
        if file_hash == dst_hash:    
            if stage == "middle":
                cur.execute('UPDATE data SET middle = 1 WHERE hash = ?', (file_hash,))
            elif stage == "end":
                cur.execute('UPDATE data SET end = 1 WHERE hash = ?', (file_hash,))
            con.commit()
            retry_count = 0
            return True
        else:
            os.remove(dst_file)
            retry_count += 1
            if retry_count < 10:
                move_file(src, dst, stage)
            else:
                print('File hashes not matching after 10 attempts. Aborting...')
                quit()
    else:
        print('Low disk space, aborting...')
        quit()

def check_size(src,dst):
    if os.path.exists(dst):
        if os.path.getsize(src) != os.path.getsize(dst):
            return 2
        else:
            return 1
    else:
        return 0

def connect_db():
    con = sqlite3.connect('waterlock.db')
    cur = con.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS data \
        (path TEXT UNIQUE, hash TEXT, middle INTEGER, end INTEGER)')
    con.commit()
    return con, cur

def refresh_file_list(source_folder):
    con, cur = connect_db()
    for folder, _, file_list in os.walk(source_folder):
        for file in file_list:
            full_path = str(folder) + '/' + str(file)
            cur.execute('INSERT OR IGNORE INTO data VALUES (?,?,?,?)', \
                (full_path, '', 0, 0))
    con.commit()
    con.close()


con, cur = connect_db()

os.makedirs(middle_folder, exist_ok=True)

if os.path.exists(source_folder) and os.path.exists(middle_folder):
    print("Moving data to transfer-medium")
    refresh_file_list(source_folder)
    cur.execute('SELECT path FROM data WHERE middle = 0 and end = 0')
    unmoved_files = cur.fetchall()
    files_left = len(unmoved_files)
    file_count = 1
    print(f'There are {files_left} files to transfer. Starting up...')
    for file in unmoved_files:
        src = file[0]
        suffix = src.split('/')[-2]
        dst = middle_folder + suffix + '/'
        print(f'({file_count}/{files_left}):   {src}')
        move_file(src, dst, 'middle')
        file_count += 1
elif os.path.exists(middle_folder) and os.path.exists(end_folder):
    print("Moving data to final destination")
    cur.execute('SELECT path FROM data WHERE middle = 1 and end = 0')
    unmoved_files = cur.fetchall()
    files_left = len(unmoved_files)
    file_count = 1
    print(f'There are {files_left} files to transfer. Starting up...')
    for file in unmoved_files:
        src = file[0]
        suffix = src.split('/')[-2]
        src = src.replace(source_folder, middle_folder)
        dst = end_folder + suffix + '/'
        print(f'({file_count}/{files_left}):   {src}')
        move_file(src, dst, 'end')
        file_count += 1