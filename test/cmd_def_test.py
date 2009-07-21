'''
Created on 13/07/2009

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

from psubprocess.streams import (get_streams_from_cmd,
                                 STDIN, STDOUT, STDERR)



def _check_streams(streams, expected_streams):
    'It checks that streams meet the requirements set by the expected streams'
    for stream, expected_stream in zip(streams, expected_streams):
        for key in expected_stream:
            assert stream[key] == expected_stream[key]

class StreamsFromCmdTest(unittest.TestCase):
    'It tests that we can get the input and output files from the cmd'
    @staticmethod
    def test_simple_case():
        'It tests the most simple cases'
        #a simple case
        cmd = ['hola', '-i', 'caracola.txt']
        cmd_def = [{'options': ('-i', '--input'), 'io': 'in'}]
        expected_streams = [{'fname': 'caracola.txt', 'io':'in',
                             'cmd_location':2}]
        streams = get_streams_from_cmd(cmd, cmd_def=cmd_def)
        _check_streams(streams, expected_streams)

        #a parameter not found in the cmd
        cmd = ['hola', '-i', 'caracola.txt']
        cmd_def = [{'options': ('-i', '--input'), 'io': 'in'},
                   {'options': ('-j', '--input2'), 'io': 'in'}]
        expected_streams = [{'fname': 'caracola.txt', 'io':'in',
                             'cmd_location': 2}]
        streams = get_streams_from_cmd(cmd, cmd_def=cmd_def)
        _check_streams(streams, expected_streams)

    @staticmethod
    def test_arguments():
        'It test that it works with cmd arguments, not options'
        #the options we want is in the pre_argv, after the binary
        cmd = ['hola', 'hola.txt', '-i', 'caracola.txt']
        cmd_def = [{'options': 1, 'io': 'in'}]
        expected_streams = [{'fname': 'hola.txt', 'io':'in',
                             'cmd_location':1}]
        streams = get_streams_from_cmd(cmd, cmd_def=cmd_def)
        _check_streams(streams, expected_streams)

        #the option we want is at the end of the cmd
        cmd = ['hola', '-i', 'caracola.txt', 'hola.txt']
        cmd_def = [{'options': -1, 'io': 'in'}]
        expected_streams = [{'fname': 'hola.txt', 'io':'in',
                             'cmd_location':3}]
        streams = get_streams_from_cmd(cmd, cmd_def=cmd_def)
        _check_streams(streams, expected_streams)

    @staticmethod
    def test_stdin():
        'We want stdin, stdout and stderr as streams'
        #stdin
        cmd = ['hola']
        cmd_def = [{'options':STDIN, 'io': 'in'},
                   {'options':STDOUT, 'io': 'out'}]
        stdout = 'stdout' #in the real world they will be files
        stderr = 'stderr'
        stdin  = 'stdin'

        expected_streams = [{'fhand': stdin,  'io':'in', 'cmd_location':STDIN,
                             'options': stdin},
                          {'fhand': stdout, 'io':'out', 'cmd_location':STDOUT,
                           'options':stdout},
                          {'fhand': stderr, 'io':'out', 'cmd_location':STDERR,
                           'options':stderr}]
        streams = get_streams_from_cmd(cmd, cmd_def=cmd_def, stdout=stdout,
                                       stderr=stderr, stdin=stdin)
        _check_streams(streams, expected_streams)
        assert 'fname' not in streams[0]
        assert 'fname' not in streams[1]
        assert 'fname' not in streams[2]


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()