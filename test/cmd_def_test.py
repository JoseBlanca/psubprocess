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

#What's a stream
#
#A command takes some input streams and creates some ouput streams
#An stream is a file-like object or a directory. In fact an stream can be
#composed by several files (e.g. a seq and a qual file that should be splitted
#together)
#
#Kinds of streams in a cmd
#cmd arg1 arg2 -i opt1 opt2 -j opt3 arg3 < stdin > stdout stderr retcode
#in this general command there are several types of streams:
#   - previous arguments. arguments (without options) located before the first
#   option (like arg1 and arg2)
#   - options with one option, like opt3
#   - options with several arguments, like -i that has opt1 and opt2
#   - arguments (aka post_arguments). arguments located after the last option
#   - stdin, stdout, stderr and retcode. The standard ones.
#
#How to define the streams
#An stream is defined by a dict with the following keys: options, io, splitter,
#value, special, location. All of them are optional except the options.
#Options: It defines in which options or arguments is the stream found. It
#should by just a value or a tuple.
#Options kinds:
#       - -i             the stream will be located after the parameter -i
#       - (-o, --output) the stream will be after -o or --output
#       - PRE_ARG        right after the cmd and before the first parameter
#       - POST_ARG       after the last option
#       - STDIN
#       - STDOUT
#       - STDERR
#io: It defines if it's an input or an output stream for the cmd
#splitter: It defines how the stream should be split. There are three ways of
#definint it:
#       - an str    that will be used to scan through the in streams, every
#                   line with the str in in will be considered a token start
#                   e.g '>' for the blast files
#       - a re      every line with a match will be considered a token start
#       - a function    the function should take the stream an return an
#                       iterator with the tokens
#joiner: A function that should take the out streams for all jobs and return
#the joined stream. If not given the output stream will be just concatenated.
#value: the value for the stream, this stream will not define the value in the
#command line, it will be implicit
#special: It defines some special treaments for the streams.
#   - no_split      It shouldn't be split
#   - no_transfer   It shouldn't be transfer to all nodes
#   - no_abspath    Its path shouldn't be converted to absolute
#   - create        It should be created before running the command
#   - no_support    An error should be raised if used.
#cmd_locations: If the location is not given the assumed location will be 0.
#That means that the stream will be located in the 0 position after the option.
#It can be either an int or an slice. In the slice case several substreams will
#be taken together in the stream. Useful for instance for the case of two fasta
#files with the seq an qual that should be split together and transfered
#together.

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