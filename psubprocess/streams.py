'''
Created on 13/07/2009

@author: jose
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

def get_streams_from_cmd(cmd, cmd_def, stdout=None, stdin=None, stderr=None):
    'Given a cmd and a cmd definition it returns the streams'
    streams = []
    for param_def in cmd_def:
        options = param_def['options']
        stream_values = None
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
        elif options == STDIN:
            index = STDIN
        else:
            #look for param in cmd
            try:
                index = _find_param_def_in_cmd(cmd, param_def)
            except ValueError:
                index = None

        #get the stream values
        if index == STDIN:
            location = STDIN
            stream_values = stdin
        elif index is not None:
            location = index + 1
            stream_values = cmd[location]

        #create the result dict
        stream = param_def.copy()
        if location is STDIN:
            stream['fhand']    = stream_values
        else:
            stream['fname']    = stream_values
        stream['cmd_location'] = location
        streams.append(stream)

    #We have to add also the stdout and stderr
    for stream_name, fhand in ((STDOUT, stdout), (STDERR, stderr)):
        if fhand is None:
            continue
        io_value = None
        if stream_name == STDIN:
            io_value = 'in'
        else:
            io_value = 'out'
        stream = {}
        stream['fhand'] = fhand
        stream['io']    = io_value
        stream['cmd_location'] = stream_name
        streams.append(stream)

    return streams