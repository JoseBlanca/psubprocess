'''
Created on 16/07/2009

@author: jose
'''

# Copyright 2009 Jose Blanca, Peio Ziarsolo, COMAV-Univ. Politecnica Valencia
# This file is part of psubprocess.
# psubprocess is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.

# psubprocess is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR  PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with psubprocess. If not, see <http://www.gnu.org/licenses/>.

from tempfile import NamedTemporaryFile
import os, stat, shutil

TEST_BINARY = '''#!/usr/bin/env python
import sys, shutil, os

args = sys.argv

#-o something   send something to stdout
#-e something   send something to stderr
#-i some_file   send the file content to sdout
#-t some_file   copy the -i file to -t file
#-x some_file
#-z some_file   copy the -x file to -z file
#-s and stdin   write stdin to stout
#-r a number    return this retcode

#are the commands in the argv?
arg_indexes = {}
for param in ('-o', '-e', '-i', '-t', '-s', '-r', '-x', '-z'):
    try:
        arg_indexes[param] = args.index(param)
    except ValueError:
        arg_indexes[param] = None

#stdout, stderr
if arg_indexes['-o']:
    sys.stdout.write(args[arg_indexes['-o'] + 1])
if arg_indexes['-e']:
    sys.stderr.write(args[arg_indexes['-e'] + 1])
#-i -t
if arg_indexes['-i'] and not arg_indexes['-t']:
    sys.stdout.write(open(args[arg_indexes['-i'] + 1]).read())
elif arg_indexes['-i'] and arg_indexes['-t']:
    shutil.copy(args[arg_indexes['-i'] + 1], args[arg_indexes['-t'] + 1])

if arg_indexes['-x'] and arg_indexes['-z']:
    shutil.copy(args[arg_indexes['-x'] + 1], args[arg_indexes['-z'] + 1])
#stdin
if arg_indexes['-s']:
    stdin = sys.stdin.read()
    sys.stdout.write(stdin)
#retcode
if arg_indexes['-r']:
    retcode = int(args[arg_indexes['-r'] + 1])
else:
    retcode = 0
sys.exit(retcode)
'''

def create_test_binary():
    'It creates a file with a test python binary in it'
    fhand = NamedTemporaryFile(suffix='.py')
    fhand.write(TEST_BINARY)
    fhand.flush()
    os.chmod(fhand.name, stat.S_IXOTH | stat.S_IRWXU)
    fname = '/tmp/test_cmd.py'
    shutil.copy(fhand.name, fname)
    fhand.close()
    #it should be executable
    return fname
