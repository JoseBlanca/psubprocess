'''It launches parallel processes with an interface similar to Popen.

It divides jobs into subjobs and launches the subjobs.

This module is useful when we have a non-parallel command to run in a
multiprocessor computer or in a multinode cluster. It will take the input files,
it will split them and it will run a subjob for everyone of the splits. It will
wait for the subjobs to finnish and it will join the output files generated
by all subjobs. At the end of the process will get the same output files as if
the command wasn't run in parallel.

This approach will work with commands that process a lot of items. This module
divides the items in sereval set and it assigns each of this sets to one new
subjob. These are the subjobs that will be run in parallel.

To do it requires the parameters used by popen: cmd, stdin, stdout, stderr and
some extra information: runner, splits and cmd_def.

runner is optional and it should be a subprocess.Popen like class. If it's not
given subprocess.Popen will be used. This Popen be the class used to run the
subjobs. If subprocess.Popen is used the subjobs will run in the processors of
the local node on several independent processes. If the Condor Popen is used
the subjobs will run in a condor cluster.

splits is the number of subjobs that we want to generate. If it's not given the
runner will provide a suitable number.

cmd_def is a dict that defines how the cmd defines the input and output files.
We need to tell Popen which are the input and output files in order to split
them and join them. The syntax for cmd_def is explained in the stream.py module
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

from subprocess import Popen as StdPopen
import os, copy

from psubprocess.streams import get_streams_from_cmd, STDOUT, STDERR, STDIN
from psubprocess.condor_runner import call
from psubprocess import condor_runner
from psubprocess.splitters import (create_file_splitter_with_re,
                                   create_non_splitter_splitter)
from psubprocess.utils import NamedTemporaryDir, copy_file_mode

RUNNER_MODULES = {}
RUNNER_MODULES['condor_runner'] = condor_runner


class Popen(object):
    '''It paralellizes the given processes dividing them into subprocesses.

    The interface is similar to subprocess.Popen to ease the use of this class,
    although the functionality of this class is much mor limited.
    When an instance of this class is created a series of subjobs is launched.
    When all subjobs are finished returncode will have an int, if they're still
    running returncode will be None.
    We can wait for all subjobs to finnish using the wait method or we can
    kill or terminate them using kill and terminate.
    '''
    def __init__(self, cmd, cmd_def=None, runner=None, runner_conf=None,
                 stdout=None, stderr=None, stdin=None, splits=None):
        '''It inits the a Popen instance, it creates and runs the subjobs.

        Like the subprocess.Popen it accepts stdin, stdout, stderr, but in this
        case all of them should be files, PIPE will not work.

        In the cmd_def list we have to tell this Popen how to locate the
        input and output files in the cmd and how to split and join them. Look
        for the cmd_format in the streams.py file.

        keyword arguments:
        cmd -- a list with the cmd to parallelize
        cmd_def -- the cmd definition list (default [])
        runner -- which runner to use  (default subprocess.Popen)
        runner_conf -- extra parameters for the runner (default {})
        stdout -- a fhand to store the stdout (default None)
        stderr -- a fhand to store the stderr (default None)
        stdin -- a fhand with the stdin (default None)
        splits -- number of subjobs to generate
        '''
        #we want the same interface as subprocess.popen
        #pylint: disable-msg=R0913
        self._retcode = None
        self._outputs_collected = False
        #some defaults
        #if the runner is not given, we use subprocess.Popen
        if runner is None:
            runner = StdPopen
        if cmd_def is None:
            if stdin is not None:
                raise ValueError('No cmd_def given but stdin present')
            cmd_def = []
        #if the number of splits is not given we calculate them
        if splits is None:
            splits = self.default_splits(runner)

        #we need a work dir to create the temporary split files
        self._work_dir = NamedTemporaryDir()
        copy_file_mode('.', self._work_dir.name)

        #the main job
        self._job = {'cmd': cmd, 'work_dir': self._work_dir}
        #we create the new subjobs
        self._jobs = self._split_jobs(cmd, cmd_def, splits, self._work_dir,
                                      stdout=stdout, stderr=stderr, stdin=stdin)

        #launch every subjobs
        self._launch_jobs(self._jobs, runner=runner, runner_conf=runner_conf)

    @staticmethod
    def _launch_jobs(jobs, runner, runner_conf):
        'It launches all jobs and it adds its popen instance to them'
        jobs['popens'] = []
        cwd = os.getcwd()
        for job_index, (cmd, streams, work_dir) in enumerate(zip(jobs['cmds'],
                                          jobs['streams'], jobs['work_dirs'])):
            #the std stream can be present or not
            stdin, stdout, stderr = None, None, None
            if jobs['stdins']:
                stdin = jobs['stdins'][job_index]
            if jobs['stdouts']:
                stdout = jobs['stdouts'][job_index]
            if jobs['stderrs']:
                stderr = jobs['stderrs'][job_index]
            #for every job we go to its dir to launch it
            os.chdir(work_dir.name)
            #we have to be sure that stdin is open for read
            if stdin:
                stdin = open(stdin.name)
            #we launch the job
            if runner == StdPopen:
                popen = runner(cmd, stdout=stdout, stderr=stderr, stdin=stdin)
            else:
                popen = runner(cmd, cmd_def=streams, stdout=stdout,
                               stderr=stderr, stdin=stdin,
                               runner_conf=runner_conf)
            #we record it's popen instane
            jobs['popens'].append(popen)
        os.chdir(cwd)

    def _split_jobs(self, cmd, cmd_def, splits, work_dir, stdout=None,
                    stderr=None, stdin=None,):
        ''''I creates one job for every split.

        Every job has a cmd, work_dir and streams, this info is in the jobs dict
        with the keys: cmds, work_dirs, streams
        '''
        #too many arguments, but similar interface to our __init__
        #pylint: disable-msg=R0913
        #pylint: disable-msg=R0914
        #the main job streams
        main_job_streams = get_streams_from_cmd(cmd, cmd_def, stdout=stdout,
                                                stderr=stderr, stdin=stdin)
        self._job['streams'] = main_job_streams

        streams, work_dirs = self._split_streams(main_job_streams, splits,
                                                 work_dir.name)

        #now we have to create a new cmd with the right in and out streams for
        #every split
        cmds, stdins, stdouts, stderrs = self._create_cmds(cmd, streams)

        jobs = {'cmds': cmds, 'work_dirs': work_dirs, 'streams': streams,
                'stdins':stdins, 'stdouts':stdouts, 'stderrs':stderrs}
        return jobs

    @staticmethod
    def _create_cmds(cmd, streams):
        '''Given a base cmd and a steams list it creates one modified cmds for
        every stream'''
        #the streams is a list of streams
        streamss = streams
        cmds = []
        stdouts = []
        stdins  = []
        stderrs = []
        for streams in streamss:
            new_cmd = copy.deepcopy(cmd)
            for stream in streams:
                #is the stream in the cmd or in is a std one?
                if 'cmd_location' in stream:
                    location = stream['cmd_location']
                else:
                    location = None
                if location is None:
                    continue
                elif location == STDIN:
                    stdins.append(stream['fhand'])
                elif location == STDOUT:
                    stdouts.append(stream['fhand'])
                elif location == STDERR:
                    stderrs.append(stream['fhand'])
                else:
                    #we modify the cmd[location] with the new file
                    #we use the fname and no path because the jobs will be
                    #launched from the job working dir
                    location = stream['cmd_location']
                    fpath    = stream['fname']
                    fname    = os.path.split(fpath)[-1]
                    new_cmd[location] = fname
            cmds.append(new_cmd)
        return cmds, stdins, stdouts, stderrs

    @staticmethod
    def _split_streams(streams, splits, work_dir):
        '''Given a list of streams it splits every stream in the given number of
        splits'''
        #which are the input and output streams?
        input_stream_indexes = []
        output_stream_indexes = []
        for index, stream in enumerate(streams):
            if stream['io'] == 'in':
                input_stream_indexes.append(index)
            elif stream['io'] == 'out':
                output_stream_indexes.append(index)

        #we create one work dir for every split
        work_dirs = []
        for index in range(splits):
            dir_ = NamedTemporaryDir(dir=work_dir)
            work_dirs.append(dir_)
            copy_file_mode('.', dir_.name)

        #we have to do first the input files because the number of splits could
        #be changed by them
        #we split the input stream files into several splits
        #we have to sort the input_stream_indexes, first we should take the ones
        #that have an input file to be split
        def do_we_have_to_split(stream_index):
            'If the stream has to split a file it will return True'
            split = None
            stream = streams[stream_index]
            #maybe they shouldn't be split
            if 'special' in stream and 'no_split' in stream['special']:
                split = False
            #maybe there is no file to split
            if (('fhand' in stream and stream['fhand'] is None) or
                ('fname' in stream and stream['fname'] is None) or
                ('fname' not in stream and 'fhand' not in stream)):
                split = False
            elif (('fhand' in stream and stream['fhand'] is not None) or
                  ('fname' in stream and stream['fname'] is not None)):
                split = True
            return split
        def to_be_split_first(stream1, stream2):
            'It sorts the streams, the ones to be split go first'
            split1 = do_we_have_to_split(stream1)
            split2 = do_we_have_to_split(stream2)
            return int(split1) - int(split2)
        input_stream_indexes = sorted(input_stream_indexes, to_be_split_first)

        first = True
        split_files = {}
        for index in input_stream_indexes:
            stream = streams[index]
            #splitter
            splitter = None
            if 'special' in stream and 'no_split' in stream['special']:
                splitter = create_non_splitter_splitter(copy_files=True)
            elif 'splitter' not in stream:
                msg = 'An splitter should be provided for every input stream'
                msg += 'missing for: ' + str(stream)
                raise ValueError(msg)
            else:
                splitter = stream['splitter']
            #the splitter can be a re, in that case with create the function
            if '__call__' not in dir(splitter):
                splitter = create_file_splitter_with_re(splitter)
            #we split the input files in the splits, every file will be in one
            #of the given work_dirs
            #the stream can have fname or fhands
            if 'fhand' in stream:
                file_ = stream['fhand']
            elif 'fname' in stream:
                file_ = stream['fname']
            else:
                file_ = None
            if file_ is None:
                #the stream migth have no file associated
                files = [None] * len(work_dirs)
            else:
                files = splitter(file_, work_dirs)
            #the files len can be different than splits, in that case we modify
            #the splits or we raise an error
            if len(files) != splits:
                if first:
                    splits = len(files)
                    #we discard the empty temporary dirs
                    work_dirs = work_dirs[0:splits]
                else:
                    msg = 'Not all input files were divided in the same number'
                    msg += ' of splits'
                    raise RuntimeError(msg)
            first = False
            split_files[index] = files   #a list of files for every in stream

        #we split the ouptut stream files into several splits
        output_splitter = create_non_splitter_splitter(copy_files=False)
        for index in output_stream_indexes:
            stream = streams[index]
            #for th output we just create the new names, but we don't split
            #any file
            if 'fhand' in stream:
                fname = stream['fhand']
            else:
                fname = stream['fname']
            files = output_splitter(fname, work_dirs)
            split_files[index] = files   #a list of files for every in stream

        new_streamss = []
        #we need one new stream for every split
        for split_index in range(splits):
            #the streams for one job
            new_streams = []
            for stream_index, stream in enumerate(streams):
                #we duplicate the original stream
                new_stream = stream.copy()
                #we set the new files
                if 'fhand' in stream:
                    new_stream['fhand'] = split_files[stream_index][split_index]
                else:
                    new_stream['fname'] = split_files[stream_index][split_index]
                new_streams.append(new_stream)
            new_streamss.append(new_streams)

        return new_streamss, work_dirs

    @staticmethod
    def default_splits(runner):
        'Given a runner it returns the number of splits recommended by default'
        if runner is StdPopen:
            #the number of processors
            return os.sysconf('SC_NPROCESSORS_ONLN')
        else:
            module = runner.__module__.split('.')[-1]
            module = RUNNER_MODULES[module]
            return module.get_default_splits()

    def wait(self):
        'It waits for all the works to finnish'
        #we wait till all jobs finish
        for job in self._jobs['popens']:
            job.wait()
        #now that all jobs have finished we join the results
        self._collect_output_streams()
        #we join now the retcodes
        self._collect_retcodes()
        return self._retcode

    def _collect_output_streams(self):
        '''It joins all the output streams into the output files and it removes
        the work dirs'''
        if self._outputs_collected:
            return
        #for each file in the main job cmd
        for stream_index, stream in enumerate(self._job['streams']):
            if stream['io'] == 'in':
                #now we're dealing only with output files
                continue
            #every subjob has a part to join for this output stream
            part_out_fnames = []
            for streams in self._jobs['streams']:
                this_stream = streams[stream_index]
                if 'fname' in this_stream:
                    part_out_fnames.append(this_stream['fname'])
                else:
                    part_out_fnames.append(this_stream['fhand'])
            #we need a function to join this stream
            joiner = None
            if joiner in stream:
                joiner = stream['joiner']
            else:
                joiner = default_cat_joiner
            if 'fname' in stream:
                out_file = stream['fname']
            else:
                out_file = stream['fhand']
            default_cat_joiner(out_file, part_out_fnames)

        #now we can delete the tempdirs
        for work_dir in self._jobs['work_dirs']:
            work_dir.close()

        self._outputs_collected = True

    def _collect_retcodes(self):
        'It gathers the retcodes from all processes'
        retcode = None
        for popen in self._jobs['popens']:
            job_retcode = popen.returncode
            if job_retcode is None:
                #if some job is yet to be finished the main job is not finished
                retcode = None
                break
            elif job_retcode != 0:
                #if one job has finnished badly the main job is badly finished
                retcode = job_retcode
                break
            #it should be 0 at this point
            retcode = job_retcode

        #if the retcode is not None the jobs have finished and we have to
        #collect the outputs
        if retcode is not None:
            self._collect_output_streams()
        self._retcode = retcode
        return retcode

    def _get_returncode(self):
        'It returns the return code'
        if self._retcode is None:
            self._collect_retcodes()
        return self._retcode
    returncode = property(_get_returncode)

    def kill(self):
        'It kills all jobs'
        if 'popens' not in self._jobs:
            return
        for popen in self._jobs['popens']:
            #untill 2.6 subprocess.popen do not support kill
            if 'kill' in dir(popen):
                popen.kill()
            else:
                pid = popen.pid
                call(['kill', '-9', str(pid)])

    def terminate(self):
        'It kills all jobs'
        if 'popens' not in self._jobs:
            return
        for popen in self._jobs['popens']:
            #untill 2.6 subprocess.popen do not support terminate
            if 'terminate' in dir(popen):
                popen.terminate()
            else:
                pid = popen.pid
                call(['kill', '-6', str(pid)])


def default_cat_joiner(out_file_, in_files_):
    '''It joins the given in files into the given out file.

    It works with fnames or fhands.
    '''
    #are we working with fhands or fnames?
    file_is_str = None
    if isinstance(out_file_, str):
        file_is_str = True
    else:
        file_is_str = False

    #the output fhand
    if file_is_str:
        out_fhand = open(out_file_, 'w')
    else:
        out_fhand = open(out_file_.name, 'w')
    for in_file_ in in_files_:
        #the input fhand
        if file_is_str:
            in_fhand = open(in_file_, 'r')
        else:
            in_fhand = open(in_file_.name, 'r')
        for line in in_fhand:
            out_fhand.write(line)
        in_fhand.close()
    out_fhand.close()
