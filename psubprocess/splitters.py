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

import re, os, shutil
from psubprocess.utils import NamedTemporaryFile, copy_file_mode
from Bio.SeqIO.QualityIO import FastqGeneralIterator

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

def _items_in_file(fhand, expression):
    '''Given an fhand and an expression it yields the items cutting where the
    line matches the expression'''
    sofar = fhand.readline()
    for line in fhand:
        if expression.search(line):
            yield sofar
            sofar = line
        else:
            sofar += line
    else:
        #the last item
        yield sofar

def _re_item_counter(fhand, expression):
    'It counts how many times the expression is found in the file'
    nitems = 0
    for line in fhand:
        if expression.search(line):
            nitems += 1
    return nitems

def _items_in_fastq(fhand, expression=None):
    'It returns the fastq items'
    for item in FastqGeneralIterator(fhand):
        yield '@%s\n%s\n+\n%s\n' % (item)

def _fastq_items_counter(fhand, expression=None):
    nitems = 0
    for item in FastqGeneralIterator(fhand):
        nitems += 1
    return nitems

def _create_file_splitter(kind, expression=None):
    '''Given an expression it creates a file splitter.

    The expression can be a regex or an str.
    The item in the file will be defined everytime a line matches the
    expression.
    '''
    item_counters = {'re': _re_item_counter,
                     'fastq': _fastq_items_counter}
    item_splitters = {'re':_items_in_file,
                      'fastq':_items_in_fastq}

    item_counter = item_counters[kind]
    item_splitter = item_splitters[kind]

    if expression is not None and isinstance(expression, str):
        expression = re.compile(expression)

    def splitter(file_, work_dirs):
        '''It splits the given file into several splits.

        Every split will be located in one of the work_dirs, although it is not
        guaranteed to create as many splits as work dirs. If in the file there
        are less items than work_dirs some work_dirs will be left empty.
        It returns a list with the fpaths or fhands for the splitted files.
        file_ can be an fhand or an fname.
        '''
        #the file_ can be an fname or an fhand. which one is it?
        file_is_str = None
        if isinstance(file_, str):
            fname = file_
            file_is_str = True
        else:
            fname = file_.name
            file_is_str = False

        #how many splits do we want?
        nsplits = len(work_dirs)
        #how many items are in the file? We assume that all files have the same
        #number of items

        fhand = open(fname, 'r')
        nitems = item_counter(fhand, expression)

        #how many splits a we going to create? and how many items will be in
        #every split
        #if there are more items than splits we create as many splits as items
        if nsplits > nitems:
            nsplits = nitems
        (nsplits1, nitems1), (nsplits2, nitems2) = _calculate_divisions(nitems,
                                                                       nsplits)
        #we have to create nsplits1 files with nitems1 in it and nsplits2 files
        #with nitems2 items in it
        new_files  = []
        fhand = open(fname, 'r')
        items = item_splitter(fhand, expression)
        splits_made = 0
        for nsplits, nitems in ((nsplits1, nitems1), (nsplits2, nitems2)):
            #we have to create nsplits files with nitems in it
            #we don't need the split_index for anything
            #pylint: disable-msg=W0612
            for split_index in range(nsplits):
                suffix = os.path.splitext(fname)[-1]
                work_dir = work_dirs[splits_made]
                ofh = NamedTemporaryFile(dir=work_dir.name, delete=False,
                                         suffix=suffix)
                copy_file_mode(fhand.name, ofh.name)
                for item_index in range(nitems):
                    ofh.write(items.next())
                ofh.flush()
                #we have to close the files otherwise we can run out of files
                #in the os filesystem
                if file_is_str:
                    new_files.append(ofh.name)
                else:
                    new_files.append(ofh)
                ofh.close()
                splits_made += 1
        return new_files
    return splitter

fastq_splitter = _create_file_splitter(kind='fastq')

def create_file_splitter_with_re(expression):
    '''Given an expression it creates a file splitter.

    The expression can be a regex or an str.
    The item in the file will be defined everytime a line matches the
    expression.
    '''
    return _create_file_splitter(kind='re', expression=expression)

def get_splitter(expression):
    '''If the expression is a known splitter kind it returns it, otherwise it
    creates a regular expression based splitter'''
    if expression == 'fastq':
        return fastq_splitter
    else:
        return create_file_splitter_with_re(expression)

def create_non_splitter_splitter(copy_files=False):
    '''It creates an splitter function that will not split the given file.

    The created splitter will create one file for every work_dir given. This
    file can be empty (useful for the output streams, or a copy of the given
    file (useful for the no_split input streams).
    '''

    def splitter(file_, work_dirs):
        '''It creates one output file for every splits.

        Every split will be located in one of the work_dirs.
        It returns a list with the fpaths for the new files.
        '''
        #the file_ can be an fname or an fhand. which one is it?
        file_is_str = None
        if isinstance(file_, str):
            fname = file_
            file_is_str = True
        else:
            fname = file_.name
            file_is_str = False
        #how many splits do we want?
        nsplits = len(work_dirs)

        new_fpaths  = []
        #we have to create nsplits
        suffix = os.path.splitext(fname)[-1]
        for split_index in range(nsplits):
            work_dir = work_dirs[split_index]
            #we use delete=False because this temp file is in a temp dir that
            #will be completely deleted. If we use delete=True we get an error
            #because the file might be already deleted when its __del__ method
            #is called
            ofh = NamedTemporaryFile(dir=work_dir.name, suffix=suffix,
                                              delete=False)
            os.remove(ofh.name)
            ofh_name = ofh.name
            #we have to close the files otherwise we can run out of files
            #in the os filesystem
            ofh.close()

            if copy_files:
                #i've tried with os.symlink but condor does not like it
                shutil.copyfile(fname, ofh_name)

            #the file will be deleted
            #what do we need the fname or the fhand?
            if file_is_str:
                new_fpaths.append(ofh.name)
            else:
                new_fpaths.append(ofh)
        return new_fpaths
    return splitter
