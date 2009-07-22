'''
Created on 21/07/2009

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

from optparse import OptionParser
import os.path, sys, signal

from psubprocess import CondorPopen

POPEN = None

def parse_options():
    'It parses the command line arguments'
    parser = OptionParser('usage: %prog -c "command"')
    parser.add_option('-c', '--command', dest='command',
                      help='The command to run')
    parser.add_option('-o', '--stdout', dest='stdout',
                      help='A file to store the stdout')
    parser.add_option('-e', '--stderr', dest='stderr',
                      help='A file to store the stderr')
    parser.add_option('-i', '--stdin', dest='stdin',
                      help='A file to store the stdin')
    parser.add_option('-d', '--cmd_def', dest='cmd_def',
                      help='The command line definition')
    return parser

def get_options():
    'It returns a dict with the options'
    parser = parse_options()
    cmd_options = parser.parse_args()[0]
    options = {}
    if cmd_options.command is None:
        raise parser.error('The command should be set')
    else:
        options['cmd'] = cmd_options.command.split()
    if cmd_options.stdout is not None:
        options['stdout'] = open(cmd_options.stdout, 'w')
    if cmd_options.stderr is not None:
        options['stderr'] = open(cmd_options.stderr, 'w')
    if cmd_options.stdin is not None:
        options['stdin'] = open(cmd_options.stdin)
    options['runner_conf'] = {'transfer_executable':False}
    if cmd_options.cmd_def is None:
        options['cmd_def'] = []
    else:
        cmd_def = cmd_options.cmd_def
        #it can be a file or an str
        if os.path.exists(cmd_def):
            cmd_def = open(cmd_def).read()
        options['cmd_def'] = eval(cmd_def)
    return options

def kill_process():
    'It kills the ongoing process'
    if POPEN is not None:
        POPEN.kill()
    sys.exit(-1)

def set_signal_handlers():
    'It sets the SIGTERM and SIGKILL signals'
    signal.signal(signal.SIGTERM, kill_process)
    signal.signal(signal.SIGABRT, kill_process)
    signal.signal(signal.SIGINT,  kill_process)

def main():
    'It runs a command in a condor cluster'
    set_signal_handlers()
    options = get_options()
    global POPEN
    POPEN = CondorPopen(**options)
    sys.exit(POPEN.wait())

if __name__ == '__main__':
    main()