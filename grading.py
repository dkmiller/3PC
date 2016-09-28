#!/usr/bin/env python

import os
from os.path import isfile, join
import shutil

test_output = 'test_output'
tests = 'tests'
try:
    shutil.rmtree(test_output)
except:
    pass
os.mkdir(test_output)
for f in os.listdir(tests):
    abs_f = join(tests, f)
    if isfile(abs_f):
        if f[len(f) - len('.input'):] == '.input':
            fn = f[:len(f) - len('.input')]
            print fn,
            os.system('./master.py < ' + abs_f + \
                    ' 2> ' + join(test_output, fn+'.err') +\
                    ' > ' + join(test_output, fn+'.output'))

            with open(join(test_output, fn+'.output')) as fi:
                    out = fi.read()
            with open(join(tests, fn+'.output')) as fi:
                    std = fi.read()
            if out == std:
                print 'correct'
            else:
                print 'wrong'
