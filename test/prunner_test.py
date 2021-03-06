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
from psubprocess.streams import STDIN
from psubprocess.utils import DATA_DIR
from test_utils import create_test_binary

class PRunnerTest(unittest.TestCase):
    'It test that we can parallelize processes'

    @staticmethod
    def test_file_in():
        'It tests the most basic behaviour'
        bin = create_test_binary()
        #with infile
        in_file = NamedTemporaryFile()
        in_file.write('hola')
        in_file.flush()

        cmd = [bin]
        cmd.extend(['-i', in_file.name])
        stdout = NamedTemporaryFile()
        stderr = NamedTemporaryFile()
        cmd_def = [{'options': ('-i', '--input'), 'io': 'in', 'splitter':''}]
        popen = Popen(cmd, stdout=stdout, stderr=stderr, cmd_def=cmd_def)
        assert popen.wait() == 0 #waits till finishes and looks to the retcode
        assert open(stdout.name).read() == 'hola'
        in_file.close()
        os.remove(bin)

    @staticmethod
    def test_job_no_in_stream():
        'It test that a job with no in stream is run splits times'
        bin = create_test_binary()

        cmd = [bin]
        cmd.extend(['-o', 'hola', '-e', 'caracola'])
        stdout = NamedTemporaryFile()
        stderr = NamedTemporaryFile()
        cmd_def = [{'options': ('-i', '--input'), 'io': 'in', 'splitter':''}]
        splits = 4
        popen = Popen(cmd, stdout=stdout, stderr=stderr, cmd_def=cmd_def,
                      splits=splits)
        assert popen.wait() == 0 #waits till finishes and looks to the retcod
        assert open(stdout.name).read() == 'hola' * splits
        assert open(stderr.name).read() == 'caracola' * splits
        os.remove(bin)

    @staticmethod
    def test_stdin():
        'It test that stdin works as input'
        bin = create_test_binary()

        #with stdin
        content = 'hola1\nhola2\nhola3\nhola4\nhola5\nhola6\nhola7\nhola8\n'
        content += 'hola9\nhola10|n'
        stdin = NamedTemporaryFile()
        stdin.write(content)
        stdin.flush()

        cmd = [bin]
        cmd.extend(['-s'])
        stdout = NamedTemporaryFile()
        stderr = NamedTemporaryFile()
        cmd_def = [{'options':STDIN, 'io': 'in', 'splitter':''}]
        popen = Popen(cmd, stdout=stdout, stderr=stderr, stdin=stdin,
                      cmd_def=cmd_def)
        assert popen.wait() == 0 #waits till finishes and looks to the retcod
        assert open(stdout.name).read() == content
        assert open(stderr.name).read() == ''
        os.remove(bin)

    @staticmethod
    def test_infile_outfile():
        'It tests that we can set an input file and an output file'
        bin = create_test_binary()
        #with infile
        in_file = NamedTemporaryFile()
        content = 'hola1\nhola2\nhola3\nhola4\nhola5\nhola6\nhola7\nhola8\n'
        content += 'hola9\nhola10|n'
        in_file.write(content)
        in_file.flush()
        out_file = NamedTemporaryFile()

        cmd = [bin]
        cmd.extend(['-i', in_file.name, '-t', out_file.name])
        stdout = NamedTemporaryFile()
        stderr = NamedTemporaryFile()
        cmd_def = [{'options': ('-i', '--input'), 'io': 'in', 'splitter':''},
                   {'options': ('-t', '--output'), 'io': 'out'}]
        popen = Popen(cmd, stdout=stdout, stderr=stderr, cmd_def=cmd_def)
        assert popen.wait() == 0 #waits till finishes and looks to the retcod
        assert not open(stdout.name).read()
        assert not open(stderr.name).read()
        assert open(out_file.name).read() == content
        in_file.close()
        os.remove(bin)

    @staticmethod
    def test_retcode():
        'It tests that we get the correct returncode'
        bin = create_test_binary()
        cmd = [bin]
        cmd.extend(['-r', '20'])
        stdout = NamedTemporaryFile()
        stderr = NamedTemporaryFile()
        popen = Popen(cmd, stdout=stdout, stderr=stderr, cmd_def=[])
        assert popen.wait() == 20 #waits till finishes and looks to the retcod
        assert not open(stdout.name).read()
        assert not open(stderr.name).read()
        os.remove(bin)

    @staticmethod
    def xtest_infile_outfile_condor():
        'It tests that we can set an input file and an output file'
        bin = create_test_binary()
        #with infile
        in_file = NamedTemporaryFile()
        content = 'hola1\nhola2\nhola3\nhola4\nhola5\nhola6\nhola7\nhola8\n'
        content += 'hola9\nhola10|n'
        in_file.write(content)
        in_file.flush()
        out_file = NamedTemporaryFile()

        cmd = [bin]
        cmd.extend(['-i', in_file.name, '-t', out_file.name])
        stdout = NamedTemporaryFile()
        stderr = NamedTemporaryFile()
        cmd_def = [{'options': ('-i', '--input'), 'io': 'in', 'splitter':''},
                   {'options': ('-t', '--output'), 'io': 'out'}]
        from psubprocess import CondorPopen
        popen = Popen(cmd, stdout=stdout, stderr=stderr, cmd_def=cmd_def,
                      runner=CondorPopen,
                      runner_conf={'transfer_executable':True})
        assert popen.wait() == 0 #waits till finishes and looks to the retcode
        assert not open(stdout.name).read()
        assert not open(stderr.name).read()
        assert open(out_file.name).read() == content
        in_file.close()
        os.remove(bin)

    @staticmethod
    def test_stdin_real_splitter():
        'It test that stdin works as input'
        bin = create_test_binary()

        #with stdin
        content = '>hola1\nhola2\n>hola3\nhola4\n>hola5\nhola6\n>hola7\nhola8\n'
        content += '>hola9\nhola10|n'
        stdin = NamedTemporaryFile()
        stdin.write(content)
        stdin.flush()

        cmd = [bin]
        cmd.extend(['-s'])
        stdout = NamedTemporaryFile()
        stderr = NamedTemporaryFile()
        cmd_def = [{'options':STDIN, 'io': 'in', 'splitter':'>'}]
        popen = Popen(cmd, stdout=stdout, stderr=stderr, stdin=stdin,
                      cmd_def=cmd_def)
        assert popen.wait() == 0 #waits till finishes and looks to the retcode
        assert open(stdout.name).read() == content
        assert open(stderr.name).read() == ''
        os.remove(bin)

    @staticmethod
    def test_2_infile_outfile():
        'It tests that we can set 2 input files and an output file'
        bin = create_test_binary()
        #with infile
        content = 'hola1\nhola2\nhola3\nhola4\nhola5\nhola6\nhola7\nhola8\n'
        content += 'hola9\nhola10|n'
        in_file1 = NamedTemporaryFile()
        in_file1.write(content)
        in_file1.flush()
        in_file2 = NamedTemporaryFile()
        in_file2.write(content)
        in_file2.flush()
        out_file1 = NamedTemporaryFile()
        out_file2 = NamedTemporaryFile()

        cmd = [bin]
        cmd.extend(['-i', in_file1.name, '-t', out_file1.name])
        cmd.extend(['-x', in_file2.name, '-z', out_file2.name])
        stdout = NamedTemporaryFile()
        stderr = NamedTemporaryFile()
        cmd_def = [{'options': ('-i', '--input'), 'io': 'in', 'splitter':''},
                   {'options': ('-x', '--input'), 'io': 'in', 'splitter':''},
                   {'options': ('-t', '--output'), 'io': 'out'},
                   {'options': ('-z', '--output'), 'io': 'out'}]
        popen = Popen(cmd, stdout=stdout, stderr=stderr, cmd_def=cmd_def)
        assert popen.wait() == 0 #waits till finishes and looks to the retcod
        assert not open(stdout.name).read()
        assert not open(stderr.name).read()
        assert open(out_file1.name).read() == content
        assert open(out_file2.name).read() == content
        in_file1.close()
        in_file2.close()
        os.remove(bin)

    @staticmethod
    def test_kill_subjobs():
        'It tests that we can kill the subjobs'
        bin = create_test_binary()
        cmd = [bin]
        cmd.extend(['-w'])
        stdout = NamedTemporaryFile()
        stderr = NamedTemporaryFile()
        popen = Popen(cmd, stdout=stdout, stderr=stderr, cmd_def=[])
        assert popen.returncode is None
        popen.kill()
        assert not open(stdout.name).read()
        assert not open(stderr.name).read()
        os.remove(bin)

    @staticmethod
    def test_nosplit():
        'It test that we can set some input files to be not split'
        bin = create_test_binary()
        #with infile
        in_file = NamedTemporaryFile()
        content = 'hola1\nhola2\n'
        in_file.write(content)
        in_file.flush()
        out_file = NamedTemporaryFile()

        cmd = [bin]
        cmd.extend(['-i', in_file.name, '-t', out_file.name])
        stdout = NamedTemporaryFile()
        stderr = NamedTemporaryFile()
        cmd_def = [{'options': ('-i', '--input'), 'io': 'in',
                    'special':['no_split']},
                   {'options': ('-t', '--output'), 'io': 'out'}]
        splits = 4
        popen = Popen(cmd, stdout=stdout, stderr=stderr, cmd_def=cmd_def,
                      splits=splits)
        assert popen.wait() == 0 #waits till finishes and looks to the retcod
        assert not open(stdout.name).read()
        assert not open(stderr.name).read()
        assert open(out_file.name).read() == content * splits
        in_file.close()
        os.remove(bin)

    @staticmethod
    def test_lots_splits_outfile():
        'It tests that we can set 2 input files and an output file'
        bin = create_test_binary()

        splits = 200
        content = ['hola%d\n' % split for split in range(splits)]
        content = ''.join(content)
        in_file1 = NamedTemporaryFile()
        in_file1.write(content)
        in_file1.flush()
        in_file2 = NamedTemporaryFile()
        in_file2.write(content)
        in_file2.flush()
        out_file1 = NamedTemporaryFile()
        out_file2 = NamedTemporaryFile()

        cmd = [bin]
        cmd.extend(['-i', in_file1.name, '-t', out_file1.name])
        cmd.extend(['-x', in_file2.name, '-z', out_file2.name])
        stdout = NamedTemporaryFile()
        stderr = NamedTemporaryFile()
        cmd_def = [{'options': ('-i', '--input'), 'io': 'in', 'splitter':''},
                   {'options': ('-x', '--input'), 'io': 'in', 'splitter':''},
                   {'options': ('-t', '--output'), 'io': 'out'},
                   {'options': ('-z', '--output'), 'io': 'out'}]
        popen = Popen(cmd, stdout=stdout, stderr=stderr, cmd_def=cmd_def,
                      splits=splits)
        assert popen.wait() == 0 #waits till finishes and looks to the retcod
        assert not open(stdout.name).read()
        assert not open(stderr.name).read()
        assert open(out_file1.name).read() == content
        assert open(out_file2.name).read() == content
        in_file1.close()
        in_file2.close()
        os.remove(bin)

    @staticmethod
    def test_bam_infile_outfile():
        'It tests that we can set an bam  input and output file'
        bin = create_test_binary()
        #with infile
        in_file = open(os.path.join(DATA_DIR, 'seq.bam'))
        out_file = NamedTemporaryFile()

        cmd = [bin]
        cmd.extend(['-i', in_file.name, '-t', out_file.name])
        stdout = NamedTemporaryFile()
        stderr = NamedTemporaryFile()
        cmd_def = [{'options': ('-i', '--input'), 'io': 'in', 'splitter':'bam'},
                   {'options': ('-t', '--output'), 'io': 'out', 'joiner':'bam'}]
        popen = Popen(cmd, stdout=stdout, stderr=stderr, cmd_def=cmd_def)



        assert popen.wait() == 0 #waits till finishes and looks to the retcod

#        print open(out_file.name).read()
#        print out_file.name

        in_file.close()
        os.remove(bin)


if __name__ == "__main__":
#    import sys;sys.argv = ['', 'PRunnerTest.test_bam_infile_outfile']
    unittest.main()
