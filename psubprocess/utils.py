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

import tempfile, os, shutil, signal, subprocess, logging

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

def call(cmd, environment=None, stdin=None, raise_on_error=False,
         stdout=None, stderr=None, log=False):
    'It calls a command and it returns stdout, stderr and retcode'
    def subprocess_setup():
        ''' Python installs a SIGPIPE handler by default. This is usually not
        what non-Python subprocesses expect.  Taken from this url:
        http://www.chiark.greenend.org.uk/ucgi/~cjwatson/blosxom/2009/07/02#
        2009-07-02-python-sigpipe'''
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)

    if stdin is None:
        pstdin = None
    else:
        pstdin = subprocess.PIPE
    if stdout is None:
        stdout = subprocess.PIPE
    if stderr is None:
        stderr = subprocess.PIPE
    #we want to inherit the environment, and modify it
    if environment is not None:
        new_env = {}
        for key, value in os.environ.items():
            new_env[key] = value
        for key, value in environment.items():
            new_env[key] = value
        environment = new_env

    if log:
        logger = logging.getLogger('franklin')
        logger.info('Running command: ' + ' '.join(cmd))

    try:
        process = subprocess.Popen(cmd, stdout=stdout, stderr=stderr,
                                   env=environment, stdin=pstdin,
                                   preexec_fn=subprocess_setup)
    except OSError:
        #if it fails let's be sure that the binary is not on the system
        binary = cmd[0]
        if binary is None:
            raise OSError('The binary was not found: ' + cmd[0])
        #let's try with an absolute path, sometimes works
        cmd.pop(0)
        cmd.insert(0, binary)

        process = subprocess.Popen(cmd, stdout=stdout, stderr=stderr,
                                       env=environment, stdin=pstdin,
                                       preexec_fn=subprocess_setup)

    if stdin is None:
        stdout_str, stderr_str = process.communicate()
    else:
        stdout_str, stderr_str = process.communicate(stdin)
    retcode = process.returncode

    if stdout != subprocess.PIPE:
        stdout.flush()
        stdout.seek(0)
    if stderr != subprocess.PIPE:
        stderr.flush()
        stderr.seek(0)

    if raise_on_error and retcode:
        if stdout != subprocess.PIPE:
            stdout_str = open(stdout.name).read()
        if stderr != subprocess.PIPE:
            stderr_str = open(stderr.name).read()
        msg = 'Error running command: %s\n stderr: %s\n stdout: %s' % \
                                                (' '.join(cmd), stderr_str,
                                                 stdout_str)
        raise RuntimeError(msg)

    return stdout_str, stderr_str, retcode
