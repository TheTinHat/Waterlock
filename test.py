from waterlock import Waterlock
import os
from itertools import product
from random import random
from shutil import rmtree

def create_dataset(n):
    x = product([i for i in range(n)], repeat=4)
    for y in x:
        os.makedirs(f'test/src/test_{y[0]}/{y[1]}/{y[2]}', exist_ok=True)
        with open(f'test/src/test_{y[0]}/{y[1]}/{y[2]}/{y[3]}.random', 'wb') as fout:
            fout.write(os.urandom(int(10240 * random())))


def clear_dataset():
    try:
        rmtree('test/')
        rmtree('cargo/')
    except:
        pass


def reset_db():
    if os.path.exists('config.db'):
        os.remove('config.db')


def make_paths():
    current_dir = str(os.path.dirname(os.path.realpath(__file__)))
    source_directory = current_dir + '/test/src/'
    destination_directory = current_dir + '/test/dst/'
    #destination_directory = '/home/david/pythontest'
    return source_directory, destination_directory


def batch_init(src, dst):
    global wl
    jobs = []
    for i in range(2):
        name = 'job_' + str(i)
        jobs.append(name)
        src_i = src + 'test_' + str(i)
        wl.initialize(job_name=name, \
            src_dir =src_i, \
            dst_dir =dst,
            prune_age = 0.02)
    return jobs


# reset_db()
# clear_dataset()
#create_dataset(3)
#os.makedirs('test/dst/job_1')

src, dst = make_paths()

wl = Waterlock()

jobs = batch_init(src,dst)


wl.start_job('job_0', same_system=True)
# wl.start_job('job_1', same_system=True)
# print("Starting jobs again")
wl.start_job('job_0', same_system=True)
# wl.start_job('job_1', same_system=True)