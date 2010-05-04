'''
Created on 03/12/2009

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

import tempfile, os, shutil

class NamedTemporaryDir(object):
    '''This class creates temporary directories '''
    #pylint: disable-msg=W0622
    #we redifine the build in dir because temfile uses that inteface
    def __init__(self, dir=None):
        '''It initiates the class.'''
        self._name = tempfile.mkdtemp(dir=dir)
    def get_name(self):
        'Returns path to the dict'
        return self._name
    name = property(get_name)
    def close(self):
        '''It removes the temp dir'''
        if os.path.exists(self._name):
            shutil.rmtree(self._name)
    def __del__(self):
        '''It removes the temp dir when instance is removed and the garbage
        collector decides it'''
        self.close()

def copy_file_mode(fpath1, fpath2):
    'It copies the os.stats mode from file1 to file2'
    mode = os.stat(fpath1)[0]
    os.chmod(fpath2, mode)
