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

import unittest
from tempfile import NamedTemporaryFile
import os

from psubprocess import Popen
from test_utils import create_test_binary

class PRunnerTest(unittest.TestCase):
    'It test that we can parallelize processes'

    @staticmethod
    def test_basic_behaviour():
        'It tests the most basic behaviour'
        bin = create_test_binary()
        #a simple job
        in_file = NamedTemporaryFile()
        in_file.write('hola')
        in_file.flush()

        cmd = [bin]
        cmd.extend(['-i', in_file.name])
        stdout = NamedTemporaryFile()
        stderr = NamedTemporaryFile()
        cmd_def = [{'options': ('-i', '--input'), 'io': 'in', 'splitter':''}]
        popen = Popen(cmd, stdout=stdout, stderr=stderr, cmd_def=cmd_def)
        print open(stderr.name).read()
        print open(stdout.name).read()
        print popen.wait()
        assert popen.wait() == 0 #waits till finishes and looks to the retcod
        assert open(stdout.name).read() == 'hola'
        os.remove(bin)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()