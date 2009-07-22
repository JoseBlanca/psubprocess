'''
Created on 14/07/2009

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
from StringIO import StringIO
import os

from psubprocess.condor_runner import (write_condor_job_file, Popen,
                                       get_default_splits)
from test_utils import create_test_binary

class CondorRunnerTest(unittest.TestCase):
    'It tests the condor runner'
    @staticmethod
    def test_write_condor_job_file():
        'It tests that we can write a condor job file with the right parameters'
        fhand1 = NamedTemporaryFile()
        fhand2 = NamedTemporaryFile()
        flog   = NamedTemporaryFile()
        stderr_ = NamedTemporaryFile()
        stdout_ = NamedTemporaryFile()
        stdin_  = NamedTemporaryFile()
        expected = '''Executable = /bin/ls
Arguments = "-i %s -j %s"
Universe = vanilla
Log = %s
When_to_transfer_output = ON_EXIT
Getenv = True
Transfer_executable = True
Transfer_input_files = %s,%s
Should_transfer_files = IF_NEEDED
Output = %s
Error = %s
Input = %s
Queue
''' % (fhand1.name, fhand2.name, flog.name, fhand1.name, fhand2.name,
       stdout_.name, stderr_.name, stdin_.name)
        fhand = StringIO()
        parameters = {'executable':'/bin/ls', 'log_file':flog,
                      'input_fnames':[fhand1.name, fhand2.name],
                      'arguments':'-i %s -j %s' % (fhand1.name, fhand2.name),
                      'transfer_executable':True, 'transfer_files':True,
                      'stdout':stdout_, 'stderr':stderr_, 'stdin':stdin_}
        write_condor_job_file(fhand, parameters=parameters)
        condor = fhand.getvalue()
        assert condor == expected

    @staticmethod
    def test_run_condor_stdout():
        'It test that we can run condor job and retrieve stdout and stderr'
        bin = create_test_binary()
        #a simple job
        cmd = [bin]
        cmd.extend(['-o', 'hola', '-e', 'caracola'])
        stdout = NamedTemporaryFile()
        stderr = NamedTemporaryFile()
        popen = Popen(cmd, runner_conf={'transfer_executable':True},
                      stdout=stdout, stderr=stderr)
        assert popen.wait() == 0 #waits till finishes and looks to the retcode
        assert open(stdout.name).read() == 'hola'
        assert open(stderr.name).read() == 'caracola'
        os.remove(bin)

    @staticmethod
    def test_run_condor_stdin():
        'It test that we can run condor job with stdin'
        bin = create_test_binary()
        #a simple job
        cmd = [bin]
        cmd.extend(['-s'])
        stdin  = NamedTemporaryFile()
        stdout = NamedTemporaryFile()
        stdin.write('hola')
        stdin.flush()
        popen = Popen(cmd, runner_conf={'transfer_executable':True},
                      stdout=stdout, stdin=stdin)
        assert popen.wait() == 0 #waits till finishes and looks to the retcode
        assert open(stdout.name).read() == 'hola'
        os.remove(bin)

    @staticmethod
    def test_run_condor_retcode():
        'It test that we can run condor job and get the retcode'
        bin = create_test_binary()
        #a simple job
        cmd = [bin]
        cmd.extend(['-r', '10'])
        popen = Popen(cmd, runner_conf={'transfer_executable':True})
        assert popen.wait() == 10 #waits till finishes and looks to the retcode
        os.remove(bin)

    @staticmethod
    def test_run_condor_in_file():
        'It test that we can run condor job with an input file'
        bin = create_test_binary()

        in_file = NamedTemporaryFile()
        in_file.write('hola')
        in_file.flush()

        cmd = [bin]
        cmd.extend(['-i', in_file.name])
        stdout = NamedTemporaryFile()
        stderr = NamedTemporaryFile()
        cmd_def = [{'options': ('-i', '--input'), 'io': 'in'}]
        popen = Popen(cmd, runner_conf={'transfer_executable':True},
                      stdout=stdout, stderr=stderr, cmd_def=cmd_def)

        assert popen.wait() == 0 #waits till finishes and looks to the retcod
        assert open(stdout.name).read() == 'hola'
        os.remove(bin)

    def test_run_condor_in_out_file(self):
        'It test that we can run condor job with an output file'
        bin = create_test_binary()

        in_file = NamedTemporaryFile()
        in_file.write('hola')
        in_file.flush()
        out_file = open('output.txt', 'w')

        cmd = [bin]
        cmd.extend(['-i', in_file.name, '-t', out_file.name])
        stdout = NamedTemporaryFile()
        stderr = NamedTemporaryFile()
        cmd_def = [{'options': ('-i', '--input'), 'io': 'in'},
                   {'options': ('-t', '--output'), 'io': 'out'}]
        popen = Popen(cmd, runner_conf={'transfer_executable':True},
                      stdout=stdout, stderr=stderr, cmd_def=cmd_def)
        popen.wait()
        assert popen.wait() == 0 #waits till finishes and looks to the retcod
        assert open(out_file.name).read() == 'hola'
        os.remove(out_file.name)

        #and output file with path won't be allowed unless the transfer file
        #mechanism is not used
        out_file = NamedTemporaryFile()

        cmd = [bin]
        cmd.extend(['-i', in_file.name, '-t', out_file.name])
        stdout = NamedTemporaryFile()
        stderr = NamedTemporaryFile()
        cmd_def = [{'options': ('-i', '--input'), 'io': 'in'},
                   {'options': ('-t', '--output'), 'io': 'out'}]
        try:
            popen = Popen(cmd, runner_conf={'transfer_executable':True},
                          stdout=stdout, stderr=stderr, cmd_def=cmd_def)
            self.fail('ValueError expected')
            #pylint: disable-msg=W0704
        except ValueError:
            pass
        os.remove(bin)

    @staticmethod
    def test_default_splits():
        'It tests that we can get a suggested number of splits'
        assert get_default_splits() > 0
        assert isinstance(get_default_splits(), int)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()