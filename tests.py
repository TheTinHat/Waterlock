import os
from shutil import rmtree
from waterlock import Waterlock
from itertools import product
from random import random


rmtree('test/')
os.mkdir('test')
if os.path.exists('waterlock.db'):
    os.remove('waterlock.db')


x = product([i for i in range(2)], repeat=3)
for y in x:
    os.makedirs(f'test/src/{y[0]}/{y[1]}', exist_ok=True)
    with open(f'test/src/{y[0]}/{y[1]}/{y[2]}.random', 'wb') as fout:
        fout.write(os.urandom(int(10240 * random())))

try:
    wl = Waterlock(source_directory='test/src/', \
                    middle_directory='test/dst/', \
                    end_directory='test/dst/')
    raise Exception('Error: Waterlock is not rejecting relative paths!')
except:
    pass

try:
    wl = Waterlock(source_directory='ABSOLUTE/PATH/TO/FOLDER', \
                    middle_directory='ABSOLUTE/PATH/TO/FOLDER', \
                    end_directory='ABSOLUTE/PATH/TO/FOLDER')
    raise Exception('Error: Waterlock is not detecting and rejecting default config!')
except:
    pass



current_dir = str(os.path.dirname(os.path.realpath(__file__)))
source_directory = current_dir + '/test/src/'
middle_directory = current_dir + '/test/cargo/'
end_directory = current_dir + '/test/dst/'

wl = Waterlock(source_directory=source_directory, \
                middle_directory=middle_directory, \
                end_directory=end_directory)
wl.start()

rmtree('test/cargo/0')
wl.reset()
wl.start()

with open(f'test/src/0/0/0.random', 'wb') as fout:
        fout.write(os.urandom(int(10240 * random())))
wl.check_modify()
wl.start()
wl.verify_middle()
del wl

rmtree('test/src')
os.makedirs('test/dst')


wl = Waterlock(source_directory=source_directory, \
                middle_directory=middle_directory, \
                end_directory=end_directory)
wl.start()
wl.verify_destination()