'''The main aim of this module is to provide an easy way to launch condor jobs.

Condor is a specialized workload management system for compute-intensive jobs.
Like other full-featured batch systems, Condor provides a job queueing
mechanism, scheduling policy, priority scheme, resource monitoring, and
resource management. More on condor on its web site:
http://www.cs.wisc.edu/condor/

The interface used is similar to the subprocess.Popen one.
Besides the standard parameters like cmd, stdout, stderr, and stdin, this condor
Popen takes a couple of extra paramteres cmd_def and runner_conf. The cmd_def
syntax is explained in the streams.py file. Condor Popen needs the cmd_def to
be able to get from the cmd which are the input and output files. The input
files should be specified in the condor job file, in the case that we want
to transfer them to the computing nodes. Besides the input and output files
in the cmd should have no paths, otherwise the command would fail in the other
machines. That's why we need cmd_def.

Created on 14/07/2009
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

from tempfile import NamedTemporaryFile
import subprocess, signal, os.path

from psubprocess.streams import get_streams_from_cmd

def call(cmd, env=None, stdin=None):
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

    process = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, env=env, stdin=pstdin,
                               preexec_fn=subprocess_setup)
    if stdin is None:
        stdout, stderr = process.communicate()
    else:
#        a = stdin.read()
#        print a
#        stdout, stderr = subprocess.Popen.stdin = stdin
#        print stdin.read()
        stdout, stderr = process.communicate(stdin)
    retcode = process.returncode
    return stdout, stderr, retcode

def write_condor_job_file(fhand, parameters):
    'It writes a condor job file using the given fhand'
    to_print = 'Executable = %s\nArguments = "%s"\nUniverse = vanilla\n' % \
               (parameters['executable'], parameters['arguments'])
    fhand.write(to_print)
    to_print = 'Log = %s\n' %  parameters['log_file'].name
    fhand.write(to_print)
    if parameters['transfer_files']:
        to_print = 'When_to_transfer_output = ON_EXIT\n'
        fhand.write(to_print)
    to_print = 'Getenv = True\n'
    fhand.write(to_print)
    if ('transfer_executable' in parameters and
        parameters['transfer_executable']):
        to_print = 'Transfer_executable = %s\n' % \
                                               parameters['transfer_executable']
        fhand.write(to_print)
    if 'input_fnames' in parameters and parameters['input_fnames']:
        ins = ','.join(parameters['input_fnames'])
        to_print = 'Transfer_input_files = %s\n' % ins
        fhand.write(to_print)
        if parameters['transfer_files']:
            to_print = 'Should_transfer_files = IF_NEEDED\n'
            fhand.write(to_print)
    if 'requirements' in parameters:
        to_print = "Requirements = %s\n" % parameters['requirements']
        fhand.write(to_print)
    if 'stdout' in parameters:
        to_print = 'Output = %s\n' % parameters['stdout'].name
        fhand.write(to_print)
    if 'stderr' in parameters:
        to_print = 'Error = %s\n' % parameters['stderr'].name
        fhand.write(to_print)
    if 'stdin' in parameters:
        to_print = 'Input = %s\n' % parameters['stdin'].name
        fhand.write(to_print)
    to_print = 'Queue\n'
    fhand.write(to_print)

    fhand.flush()

class Popen(object):
    '''It launches and controls a condor job.

    The job is launched when an instance is created. After that we can get the
    cluster id with the method.pid. The rest of the interface is very similar
    to the subprocess.Popen one. There's no communicate method because there's
    no support for PIPE.
    '''
    def __init__(self, cmd, cmd_def=None, runner_conf=None, stdout=None,
                 stderr=None, stdin=None):
        '''It launches a condor job.

        The interface is similar to the subprocess.Popen one, although there are
        some differences.
        stdout, stdin and stderr should be file handlers, there's no support for
        PIPEs. The extra parameter cmd_def is required if we need to transfer
        the input and output files to the computing nodes of the cluster using
        the condor file transfer mechanism. The cmd_def syntax is explained in
        the streams.py file.
        runner_conf is a dict that admits several parameters that control how
        condor is run:
            - transfer_files: do we want to transfer the files using the condor
                              transfer file mechanism? (default True)
            - condor_log: the condor log file. If it's not given Popen will
                          create a condor log file in the tempdir.
            - transfer_executable: do we want to transfer the executable?
                                   (default False)
            - requirements: The requirements line for the condor job file.
                            (default None)
        '''
        #we use the same parameters as subprocess.Popen
        #pylint: disable-msg=R0913
        if cmd_def is None:
            cmd_def = []

        #runner conf
        if runner_conf is None:
            runner_conf = {}
        #some defaults
        if 'transfer_files' not in runner_conf:
            runner_conf['transfer_files'] = True

        if 'condor_log' not in runner_conf:
            self._log_file = NamedTemporaryFile(suffix='.log')
        else:
            self._log_file = runner_conf['condor_log']
        #create condor job file
        condor_job_file = self._create_condor_job_file(cmd, cmd_def,
                                                      self._log_file,
                                                      runner_conf,
                                                      stdout, stderr, stdin)
        self._condor_job_file = condor_job_file
        #print open(condor_job_file.name).read()

        #launch condor
        self._retcode = None
        self._cluster_number = None
        self._launch_condor(condor_job_file)

    def _launch_condor(self, condor_job_file):
        'Given the condor_job_file it launches the condor job'
        try:
            stdout, stderr, retcode = call(['condor_submit', condor_job_file.name])
        except OSError:
            raise OSError('condor_submit not found in your path')
        if retcode:
            msg = 'There was a problem with condor_submit: ' + stderr
            raise RuntimeError(msg)
        #the condor cluster number is given by condor_submit
        #1 job(s) submitted to cluster 15.
        for line in stdout.splitlines():
            if 'submitted to cluster' in line:
                self._cluster_number = line.strip().strip('.').split()[-1]

    def _get_pid(self):
        'It returns the condor cluster number'
        return self._cluster_number
    pid = property(_get_pid)

    def _get_returncode(self):
        'It returns the return code'
        return self._retcode
    returncode = property(_get_returncode)

    @staticmethod
    def _remove_paths_from_cmd(cmd, streams, conf):
        '''It removes the absolute and relative paths from the cmd,
        it returns the modified cmd'''
        cmd_mod = cmd[:]
        for stream in streams:
            if 'fname' not in stream:
                continue
            fpath = stream['fname']
            #for the output files we can't deal with transfering files with
            #paths. Condor will deliver those files into the initialdir, not
            #where we expected.
            if (stream['io'] != 'in' and conf['transfer_files']
                and os.path.split(fpath)[-1] != fpath):
                msg = 'output files with paths are not transferable'
                raise ValueError(msg)

            index = cmd_mod.index(fpath)
            fpath = os.path.split(fpath)[-1]
            cmd_mod[index] = fpath
        return cmd_mod

    def _create_condor_job_file(self, cmd, cmd_def, log_file, runner_conf,
                                stdout, stderr, stdin):
        'Given a cmd and the cmd_def it returns the condor job file'
        #streams
        streams = get_streams_from_cmd(cmd, cmd_def, stdout=stdout,
                                       stderr=stderr, stdin=stdin)
        #we need some parameters to write the condor file
        parameters = {}
        #the executable
        binary = cmd[0]
        #the binary should be an absolute path
        if not os.path.isabs(binary):
            #the path to the binary could be relative
            if os.sep in binary:
                #we make the path absolute
                binary = os.path.abspath(binary)
            else:
                #we have to look in the system $PATH
                binary = call(['which', binary])[0].strip()
        parameters['executable'] = binary

        parameters['log_file'] = log_file
        #the cmd shouldn't have absolute path in the files because they will be
        #transfered to another node in the condor working dir and they wouldn't
        #be found with an absolute path
        cmd_no_path = self._remove_paths_from_cmd(cmd, streams, runner_conf)
        parameters['arguments'] = ' '.join(cmd_no_path[1:])
        if stdout is not None:
            parameters['stdout'] = stdout
        if stderr is not None:
            parameters['stderr'] = stderr
        if stdin is not None:
            parameters['stdin'] = stdin

        transfer_bin = False
        if 'transfer_executable' in runner_conf:
            transfer_bin = runner_conf['transfer_executable']
        parameters['transfer_executable'] = transfer_bin

        transfer_files = runner_conf['transfer_executable']
        parameters['transfer_files'] = str(transfer_files)

        if 'requirements' in runner_conf:
            parameters['requirements'] = runner_conf['requirements']

        in_fnames = []
        for stream in streams:
            if stream['io'] == 'in':
                fname = None
                if 'fname' in stream:
                    fname = stream['fname']
                else:
                    fname = stream['fhand'].name
                in_fnames.append(fname)
        parameters['input_fnames'] = in_fnames

        #now we can create the job file
        condor_job_file = NamedTemporaryFile()
        write_condor_job_file(condor_job_file, parameters=parameters)
        return condor_job_file

    def _update_retcode(self):
        'It updates the retcode looking at the log file, it returns the retcode'
        for line in open(self._log_file.name):
            if 'return value' in line:
                ret = line.split('return value')[1].strip().strip(')')
                self._retcode = int(ret)
        return self._retcode

    def poll(self):
        'It checks if condor has run ower condor cluster'
        cluster_number = self._cluster_number
        cmd = ['condor_q', cluster_number,
               '-format', '"%d.\n"', 'ClusterId']
        stdout, stderr, retcode =  call(cmd)
        if retcode:
            msg = 'There was a problem with condor_q: ' + stderr
            raise RuntimeError(msg)
        if cluster_number not in stdout:
            #the job is finished
            return self._update_retcode()
        return self._retcode

    def wait(self):
        'It waits until the condor job is finished'
        try:
            stderr, retcode = call(['condor_wait', self._log_file.name])[1:]
        except OSError:
            raise OSError('condor_wait not found in your path')
        if retcode:
            msg = 'There was a problem with condor_wait: ' + stderr
            raise RuntimeError(msg)
        return self._update_retcode()

    def kill(self):
        'It runs condor_rm for the condor job'
        try:
            stderr, retcode = call(['condor_rm', self.pid])[1:]
        except OSError:
            raise OSError('condor_rm not found in your path')

        if retcode:
            msg = 'There was a problem with condor_rm: ' + stderr
            raise RuntimeError(msg)
        return self._update_retcode()

    def terminate(self):
        'It runs condor_rm for the condor job'
        self.kill()

def get_default_splits():
    'It returns a suggested number of splits for this Popen runner'
    try:
        stdout, stderr, retcode = call(['condor_status', '-total'])
    except OSError:
        raise OSError('condor_status not found in your path')
    if retcode:
        msg = 'There was a problem with condor_status: ' + stderr
        raise RuntimeError(msg)
    for line in stdout.splitlines():
        line = line.strip().lower()
        if line.startswith('total') and 'owner' not in line:
            return int(line.split()[1]) * 2
