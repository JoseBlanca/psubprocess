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

def get_cmd_def_from_cmd(cmd):
    'Given a cmd it returns a cmd_def infered from it and a clean cmd'
    cmd_def = []
    new_cmd = []
    for index, arg in enumerate(cmd):
        #if this arg is not like >#whatever# or <#whatever# we don't process it
        if not (arg[0] in ('>', '<') and arg[-1] == '#'):
            new_cmd.append(arg)
            continue
        arg_def = {}
        #is an input or an output?
        if arg[0] == '>':
            arg_def['io'] = 'in'
        elif arg[0] == '<':
            arg_def['io'] = 'out'
        else:
            msg = 'Wrong format in the first character of the argument: %s', arg
            raise ValueError(msg)
        arg = arg[1:-1]
        #now we split the section that defines the original argument
        definition, arg = arg.split('#')
        #the arg is cleaned now, we can add it to the command
        new_cmd.append(arg)
        #from the arg we have to extract the definition option
        #if the arg begins with - we assume that it will be associated with the
        #next item in the cmd
        if arg[0] == '-':
            arg_def['options'] = (arg.rstrip('-'), )
        else:
            #is an argument at the begining or the end and has no option
            #associated
            #there's no -i or --input argument
            arg_def['options'] = index

        #now we can process the rest of the definiton
        #the items in the definiton should be separed by ';'
        for definition_item in definition.split(';'):
            if not definition_item:
                continue
            key, value = definition_item.split('=')
            arg_def[key] = value
        cmd_def.append(arg_def)

    return new_cmd, cmd_def
