'''
Created on 22/12/2009

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

import unittest, os
from tempfile import NamedTemporaryFile

from test_utils import create_test_binary
from psubprocess import Popen
from psubprocess.cmd_def_from_cmd import get_cmd_def_from_cmd


class CmddefFromCmdTest(unittest.TestCase):
    'It checks that we can build cmd definitions looking at the command given'

    @staticmethod
    def test_no_cmd_def():
        'A command withtout the especial syntax returns and empty cmd_def'
        cmd = ['cat', 'hola.txt']
        processed_cmd, cmd_def = get_cmd_def_from_cmd(cmd)
        assert processed_cmd == cmd
        assert not cmd_def

    @staticmethod
    def test_splitter():
        'We can get the splitter from the cmd'
        #with splitter
        cmd = ['cat', '>splitter=>#hola.txt#']
        processed_cmd, cmd_def = get_cmd_def_from_cmd(cmd)
        assert processed_cmd == ['cat', 'hola.txt']
        assert cmd_def == [{'options': 1, 'io': 'in', 'splitter':'>'}]

        #with no splitter
        cmd = ['cat', '>#hola.txt#']
        processed_cmd, cmd_def = get_cmd_def_from_cmd(cmd)
        assert processed_cmd == ['cat', 'hola.txt']
        assert cmd_def == [{'options': 1, 'io': 'in'}]

        #with a parameter
        cmd = ['cat', '>#-i#', 'hola.txt']
        processed_cmd, cmd_def = get_cmd_def_from_cmd(cmd)
        assert processed_cmd == ['cat', '-i' ,'hola.txt']
        assert cmd_def == [{'options': ('-i',), 'io': 'in'}]


    @staticmethod
    def test_prunner_with_cmddef():
        'It tests that we can run the prunner setting the cmd_def in the cmd'
        bin = create_test_binary()
        #with infile
        in_file = NamedTemporaryFile()
        content = 'hola1\nhola2\nhola3\nhola4\nhola5\nhola6\nhola7\nhola8\n'
        content += 'hola9\nhola10|n'
        in_file.write(content)
        in_file.flush()
        out_file = NamedTemporaryFile()

        cmd = [bin]
        cmd.extend(['>splitter=#-i#', in_file.name,
                    '<#-t#', out_file.name])
        stdout = NamedTemporaryFile()
        stderr = NamedTemporaryFile()
        popen = Popen(cmd, stdout=stdout, stderr=stderr)
        assert popen.wait() == 0 #waits till finishes and looks to the retcod
        assert not open(stdout.name).read()
        assert not open(stderr.name).read()
        assert open(out_file.name).read() == content
        in_file.close()
        os.remove(bin)


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()