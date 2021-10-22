import os
from shutil import rmtree
from numpy import source
from waterlock2 import Waterlock
from itertools import product
from random import random

rmtree('test/')
os.mkdir('test')
if os.path.exists('waterlock.db'):
    os.remove('waterlock.db')

x = product([i for i in range(5)], repeat=3)
for y in x:
    os.makedirs(f'test/src/{y[0]}/{y[1]}', exist_ok=True)
    with open(f'test/src/{y[0]}/{y[1]}/{y[2]}.random', 'wb') as fout:
        fout.write(os.urandom(int(10240 * random())))


wl = Waterlock(source_directory='test/src/', middle_directory='test/cargo/', end_directory='test/dst/')
wl.start()
wl.verify_middle()
del wl


rmtree('test/src')
os.makedirs('test/dst')


wl = Waterlock(source_directory='test/src/', middle_directory='test/cargo/', end_directory='test/dst/')
wl.start()
wl.verify_destination()