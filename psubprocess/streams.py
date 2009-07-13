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

def get_streams_from_cmd(cmd, cmd_def):
    'Given a cmd and a cmd definition it returns the streams'
    streams = []
    for param_def in cmd_def:
        options = param_def['options']
        stream_values = []
        #is the parameter defining an std stream?
        for stream in (STDIN, STDOUT, STDERR):
            #options can be an item or an iterable
            if stream == options or ('__in__' in dir(options) and
                                     stream in options):
                stream_values = [stream]
        #if it isn't an standard stream we have to look for the stream
        if not stream_values:
            #where are the stream values after the parameter?
            if 'cmd_locations' in param_def:
                locations = param_def['cmd_locations']
            else:
                #by the default we want the first item after the parameter
                locations = slice(0, 1)
            #if the location is not a list we make it
            try:
                #pylint: disable-msg=W0104
                #the statement has an effect because we're cheking
                #if the locations is a slice or an int
                locations.stop
                locations.start
            except AttributeError:
                locations = slice(locations, locations + 1)

            #where is the parameter in the cmd list?
            #if the param options is not an int or a slice, its a list of
            #strings
            if isinstance(options, int):
                #for PRE_ARG (1) and POST_ARG (-1)
                #we take 1 unit because the options should be 1 to the right
                #of the value
                index = _positive_int(options, cmd) - 1
            else:
                #look for param in cmd
                try:
                    index = _find_param_def_in_cmd(cmd, param_def)
                except ValueError:
                    index = None

            #get the stream values
            if index is not None:
                #now we add the index to the location, because the
                #location is relative to where the parameter is found
                locations = slice(locations.start + index + 1,
                                  locations.stop + index + 1)

                stream_values = cmd[locations]

        #create the result dict
        stream = param_def.copy()
        stream['streams'] = stream_values
        streams.append(stream)

    return streams