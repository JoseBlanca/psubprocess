'''It launches parallel processes with an interface similar to Popen.

It divides jobs into subjobs and launches the subjobs.

Created on 16/07/2009

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

from subprocess import Popen as StdPopen
import os, tempfile, shutil, copy

from psubprocess.streams import get_streams_from_cmd, STDOUT, STDERR, STDIN

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
        '''It removes de temp dir when instance is removed and the garbaje
        colector decides it'''
        self.close()

def NamedTemporaryFile(dir=None, delete=False, suffix=''):
    '''It creates a temporary file that won't be deleted when close

    This behaviour can be done with tempfile.NamedTemporaryFile in python > 2.6
    '''
    fpath = tempfile.mkstemp(dir=dir, suffix=suffix)[1]
    return open(fpath, 'w')

def _calculate_divisions(num_items, splits):
    '''It calculates how many items should be in every split to divide
    the num_items into splits.
    Not all splits will have an equal number of items, it will return a tuple
    with two tuples inside:
    ((num_fragments_1, num_items_1), (num_fragments_2, num_items_2))
    splits = num_fragments_1 + num_fragments_2
    num_items_1 = num_items_2 + 1
    num_fragments_1 could be equal to 0.
    This is the best way to create as many splits as possible as similar as
    possible.
    '''
    if splits >= num_items:
        return ((0, 1), (splits, 1))
    num_fragments1 = num_items % splits
    num_fragments2 = splits - num_fragments1
    num_items2 = num_items // splits
    num_items1 = num_items2 + 1
    res = ((num_fragments1, num_items1), (num_fragments2, num_items2))
    return res

def _write_file(dir, contents, suffix):
    '''It creates a new file with the given contents. It returns the path'''
    ofh = NamedTemporaryFile(dir=dir, delete=False, suffix=suffix)
    ofh.write(contents)
    #the file won't be deleted, just closed,
    #it will be deleted because is created in a
    #temporary directory
    fname = ofh.name
    ofh.flush()
    ofh.close()
    return fname

def _create_file_splitter_with_re(expression):
    '''Given an expression it creates a file splitter.

    The expression can be a regex or an str.
    The item in the file will be defined everytime a line matches the
    expression.
    '''
    expression_kind = None
    if isinstance(expression, str):
        expression_kind = 'str'
    else:
        expression_kind = 're'
    def splitter(fname, work_dirs):
        '''It splits the given file into several splits.

        Every split will be located in one of the work_dirs, although it is not
        guaranteed to create as many splits as work dirs. If in the file there
        are less items than work_dirs some work_dirs will be left empty.
        It returns a list with the fpaths for the splitted files.
        '''
        #how many splits do we want?
        nsplits = len(work_dirs)
        #how many items are in the file? We assume that all files have the same
        #number of items
        nitems = 0
        for line in open(fname, 'r'):
            if ((expression_kind == 'str' and expression in line) or
                 expression.search(line)):
                nitems += 1

        #how many splits a we going to create? and how many items will be in
        #every split
        #if there are more items than splits we create as many splits as items
        if nsplits > nitems:
            nsplits = nitems
        (nsplits1, nitems1), (nsplits2, nitems2) = _calculate_divisions(nitems,
                                                                       nsplits)
        #we have to create nsplits1 files with nitems1 in it and nsplits2 files
        #with nitems2 items in it
        splits_made = 0
        new_fpaths  = []
        fhand = open(fname, 'r')
        for index_splits, nsplits in enumerate((nsplits1, nsplits2)):
            nitems = (nitems1, nitems2)[index_splits]
            #we have to create nsplits files with nitems in it
            for index in range(nsplits):
                items_sofar = 0
                sofar = fhand.readline()
                suffix = os.path.splitext(fname)[-1]
                splits_made = 0
                for line in fhand:
                    #has the file the required pattern?
                    if ((expression_kind == 'str' and expression in line) or
                         expression.search(line)):
                        items_sofar += 1
                        if items_sofar >= nitems:
                            #how many splits do we have now for this file?
                            splits_now = splits_made
                            #which is the work dir in which this file
                            #should be located?
                            work_dir = work_dirs[splits_made]
                            ofname = _write_file(work_dir.name, sofar, suffix)
                            new_fpaths.append(ofname)
                            sofar = line
                            items_sofar = 0
                            splits_made += 1
                        else:
                            sofar += line
                    else:
                        sofar += line
                #now we write the last file
                work_dir = work_dirs[splits_made]
                ofname = _write_file(work_dir.name, sofar, suffix)
                new_fpaths.append(ofname)
        return new_fpaths
    return splitter

def _output_splitter(fname, work_dirs):
    '''It creates one output file for every splits.

    Every split will be located in one of the work_dirs.
    It returns a list with the fpaths for the new output files.
    '''
    #how many splits do we want?
    nsplits = len(work_dirs)

    new_fpaths  = []
    #we have to create nsplits
    for split_index in range(nsplits):
        suffix = os.path.splitext(fname)[-1]
        work_dir = work_dirs[split_index]
        ofh = tempfile.NamedTemporaryFile(dir=work_dir.name, suffix=suffix)
        #the file will be deleted
        #it will be deleted because we just need the name in the temporary
        #directory. tempfile.mktemp would be better for this use, but it is
        #deprecated
        new_fpaths.append(ofh.name)
        ofh.close()
    return new_fpaths

def default_cat_joiner(out_fname, in_fnames):
    '''It joins the given in files into the given out file.

    It requieres fnames, not fhands.
    '''
    out_fhand = open(out_fname, 'w')
    for in_fname in in_fnames:
        in_fhand = open(in_fname, 'r')
        for line in in_fhand:
            out_fhand.write(line)
        in_fhand.close()
    out_fhand.close()

class Popen(object):
    'It paralellizes the given processes divinding them into subprocesses.'

    def __init__(self, cmd, cmd_def=None, runner=None, runner_conf=None,
                 stdout=None, stderr=None, stdin=None, splits=None):
        '''
        Constructor
        '''
        self._retcode = None
        self._outputs_collected = False
        #some defaults
        #if the runner is not given, we use subprocess.Popen
        if runner is None:
            runner = StdPopen
        #if the number of splits is not given we calculate them
        if splits is None:
            splits = self.default_splits(runner)

        #we need a work dir to create the temporary split files
        self._work_dir = NamedTemporaryDir()

        #the main job
        self._job = {'cmd': cmd, 'work_dir': self._work_dir}
        #we create the new subjobs
        self._jobs = self._split_jobs(cmd, cmd_def, splits, self._work_dir,
                                      stdout=stdout, stderr=stderr, stdin=stdin)

        #launch every subjobs
        self._launch_jobs(self._jobs, runner=runner)

    @staticmethod
    def _launch_jobs(jobs, runner):
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
                stdouts = jobs['stdouts'][job_index]
            if jobs['stderrs']:
                stderr = jobs['stderrs'][job_index]
            #for every job we go to its dir to launch it
            os.chdir(work_dir.name)
            #we launch the job
            if runner == StdPopen:
                popen = runner(cmd, stdout=stdout, stderr=stderr, stdin=stdin)
            else:
                popen = runner(cmd, cmd_def=streams, stdout=stdout,
                               stderr=stderr, stdin=stdin)
            #we record it's popen instane
            jobs['popens'].append(popen)
        os.chdir(cwd)

    def _split_jobs(self, cmd, cmd_def, splits, work_dir, stdout=None,
                    stderr=None, stdin=None,):
        ''''I creates one job for every split.

        Every job has a cmd, work_dir and streams, this info is in the jobs dict
        with the keys: cmds, work_dirs, streams
        '''
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
                location = stream['cmd_location']
                if location == STDIN:
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
            work_dirs.append(NamedTemporaryDir(dir=work_dir))

        #we have to do first the input files because the number of splits could
        #be changed by them
        #we split the input stream files into several splits
        first = True
        split_files = {}
        for index in input_stream_indexes:
            stream = streams[index]
            #splitter
            splitter = stream['splitter']
            #the splitter can be a re, in that case with create the function
            if '__call__' not in dir(splitter):
                splitter = _create_file_splitter_with_re(splitter)
            #we split the input files in the splits, every file will be in one
            #of the given work_dirs
            #the stream can have fname or fhands
            if 'fhand' in stream:
                fname = stream['fhand'].name
            else:
                fname = stream['fname']
            files = splitter(fname, work_dirs)
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
        for index in output_stream_indexes:
            stream = streams[index]
            #for th output we just create the new names, but we don't split
            #any file
            if 'fhand' in stream:
                fname = stream['fhand'].name
            else:
                fname = stream['fname']
            files = _output_splitter(fname, work_dirs)
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
                new_stream['fhand'] = split_files[stream_index]
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
            return runner.default_splits()

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
            if stream['io'] != 'in':
                #now we're dealing only with output files
                continue
            #every subjob has a part to join for this output stream
            part_out_fnames = []
            for streams in self._jobs['streams']:
                part_out_fnames.append(streams[stream_index]['fname'])
            #we need a function to join this stream
            joiner = None
            if joiner in stream:
                joiner = stream['joiner']
            else:
                joiner = default_cat_joiner
            default_cat_joiner(stream['fname'], part_out_fnames)

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