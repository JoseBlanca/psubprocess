'''
What's a stream

A command takes some input streams and creates some output streams
An stream is a file-like object.

Kinds of streams in a cmd
cmd arg1 arg2 -i opt1 -j opt3 arg3 < stdin > stdout stderr retcode
in this general command there are several types of streams:
   - previous arguments. arguments (without options) located before the first
   option (like arg1 and arg2)
   - options with one option, like opt3
   - arguments (aka post_arguments). arguments located after the last option
   - stdin, stdout, stderr and retcode. The standard ones.

How to define the streams
To create the streams list we need the cmd and the cmd_def. The stream list
will be created using the cmd_def as a starting point and adding some extra
information based in the cmd given.
The cmd_def is defined by a dict with the following keys: options, io, splitter,
fhand, fname, special, location. All of them are optional except the options.

Options: It defines in which options or arguments is the stream found. It
should by just a value or a tuple.
Options kinds:
       - -i             the stream will be located after the parameter -i
       - (-o, --output) the stream will be after -o or --output
       - int            where in the cmd is the stream (useful for pre-args and
                        post-args)
       - STDIN
       - STDOUT
       - STDERR

io: It defines if it's an input or an output stream for the cmd

splitter: It defines how the stream should be split. There are three ways of
definint it:
       - an str    that will be used to scan through the in streams, every
                   line with the str in in will be considered a token start
                   e.g '>' for the blast files
       - a re      every line with a match will be considered a token start
       - a function    the function should take the stream an return an
                       iterator with the tokens

joiner: A function that should take the out streams for all jobs and return
the joined stream. If not given the output stream will be just concatenated.

fhand or fpath: the stream file. This information is not part of the cmd_def.
It will be added to the streams looking at the cmd

special: It defines some special treatments for the streams.
   - no_split      It shouldn't be split
   - no_transfer   It shouldn't be transfer to all nodes
   - no_support    An error should be raised if used.

cmd_location: Where in the cmd is the file that corresponds to this stream
is located. This information shouldn't be in the cmd_def it will be added to the
streams using the cmd and the cmd_def.
'''

# Copyright 2009 Jose Blanca, Peio Ziarsolo, COMAV-Univ. Politecnica Valencia
# This file is part of project.
# project is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.

# project is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR  PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with project. If not, see <http://www.gnu.org/licenses/>.

STDIN  = 'stdin'
STDOUT = 'stdout'
STDERR = 'stderr'

def _find_param_def_in_cmd(cmd, param_def):
    '''Given a cmd and a parameter definition it returns the index of the param
    in the cmd.

    If the param is not found in the cmd it will raise a ValueError.
    '''
    options = param_def['options']
    #options could be a list or an item
    if not isinstance(options, list) and not isinstance(options, tuple):
        options = (options,)

    #the standard options with command line options
    for index, item in enumerate(cmd):
        if item in options:
            return index
    raise ValueError('Parameter not found in the given cmd')

def _positive_int(index, sequence):
    '''It returns the same int index, but positive.'''
    if index is None:
        return None
    elif index < 0:
        return len(sequence) + index
    return index

def _add_std_cmd_defs(cmd_def, stdout, stdin, stderr):
    '''It adds the standard stream to the cmd_def.

    If they're already there it just completes them
    '''
    #which std streams are in the cmd_def?
    in_cmd_def = {}
    for param_def in cmd_def:
        option = param_def['options']
        if option in (STDOUT, STDIN, STDERR):
            in_cmd_def[option] = True
    #we create the missing ones
    if stdout is not None and STDOUT not in in_cmd_def:
        cmd_def.append({'options':STDOUT, 'io':'out'})
    if stderr is not None and STDERR not in in_cmd_def:
        cmd_def.append({'options':STDERR, 'io':'out'})
    if stdin is not None and STDIN not in in_cmd_def:
        cmd_def.append({'options':STDIN, 'io':'in'})

def get_streams_from_cmd(cmd, cmd_def, stdout=None, stdin=None, stderr=None):
    'Given a cmd and a cmd definition it returns the streams'
    #stdout and stderr might not be in the cmd_def
    _add_std_cmd_defs(cmd_def, stdout=stdout, stdin=stdin, stderr=stderr)

    streams = []
    for param_def in cmd_def:
        options = param_def['options']
        #where is this stream located in the cmd?
        location = None

        #we have to look for the stream in the cmd
        #where is the parameter in the cmd list?
        #if the param options is not an int, its a list of strings
        if isinstance(options, int):
            #for PRE_ARG (1) and POST_ARG (-1)
            #we take 1 unit because the options should be 1 to the right
            #of the value
            index = _positive_int(options, cmd) - 1
        elif options in (STDERR, STDOUT, STDIN):
            index = options
        else:
            #look for param in cmd
            try:
                index = _find_param_def_in_cmd(cmd, param_def)
            except ValueError:
                index = None

        if index == STDERR:
            location = STDERR
            fname = stderr
        elif index == STDOUT:
            location = STDOUT
            fname = stdout
        elif index == STDIN:
            location = STDIN
            fname = stdin
        elif index is not None:
            location = index + 1
            fname = cmd[location]

        #create the result dict
        stream = param_def.copy()

        if location is not None:
            if location in (STDIN, STDOUT, STDERR):
                stream['fhand']    = fname
            else:
                stream['fname']    = fname
            stream['cmd_location'] = location
        streams.append(stream)

    return streams