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

import unittest, os
from tempfile import NamedTemporaryFile
from psubprocess.utils import DATA_DIR
from psubprocess.prunner import NamedTemporaryDir
from psubprocess.splitters import (create_file_splitter_with_re, fastq_splitter,
                                   bam_splitter, blank_line_splitter)

class SplitterTest(unittest.TestCase):
    'It test that we can split the input files'

    @staticmethod
    def test_re_splitter():
        'It tests the general regular expression based splitter'
        fastq = '@seq1\nACTG\n+\nmoco\n@seq2\nGTCA\n+\nhola\n'
        file_ = NamedTemporaryFile()
        file_.write(fastq)
        file_.flush()

        splitter = create_file_splitter_with_re(expression='^@')
        dir1 = NamedTemporaryDir()
        dir2 = NamedTemporaryDir()
        dir3 = NamedTemporaryDir()
        new_files = splitter(file_, [dir1, dir2, dir3])
        assert len(new_files) == 2
        assert open(new_files[0].name).read() == '@seq1\nACTG\n+\nmoco\n'
        assert open(new_files[1].name).read() == '@seq2\nGTCA\n+\nhola\n'
        dir1.close()
        dir2.close()
        dir3.close()

    @staticmethod
    def test_fastq_splitter():
        'It tests the fastq splitter'
        fastq = '@seq1\nACTG\n+\nmoco\n@seq2\nGTCA\n+\nhola\n'
        file_ = NamedTemporaryFile()
        file_.write(fastq)
        file_.flush()

        splitter = fastq_splitter
        dir1 = NamedTemporaryDir()
        dir2 = NamedTemporaryDir()
        dir3 = NamedTemporaryDir()
        new_files = splitter(file_, [dir1, dir2, dir3])
        assert len(new_files) == 2
        assert open(new_files[0].name).read() == '@seq1\nACTG\n+\nmoco\n'
        assert open(new_files[1].name).read() == '@seq2\nGTCA\n+\nhola\n'
        dir1.close()
        dir2.close()
        dir3.close()

    @staticmethod
    def test_blank_line_splitter():
        'It tests the blank line splitter'
        fastq = 'hola\n\ncaracola\n\n'
        file_ = NamedTemporaryFile()
        file_.write(fastq)
        file_.flush()

        splitter = blank_line_splitter
        dir1 = NamedTemporaryDir()
        dir2 = NamedTemporaryDir()
        dir3 = NamedTemporaryDir()
        new_files = splitter(file_, [dir1, dir2, dir3])
        assert len(new_files) == 2

        assert open(new_files[0].name).read() == 'hola\n\n'
        assert open(new_files[1].name).read() == 'caracola\n\n'
        dir1.close()
        dir2.close()
        dir3.close()

    @staticmethod
    def test_bam_splitter():
        'It test bam splitter'

        bam_fhand = os.path.join(DATA_DIR, 'seq.bam')

        splitter = bam_splitter
        dir1 = NamedTemporaryDir()
        dir2 = NamedTemporaryDir()
        dir3 = NamedTemporaryDir()
        new_files = splitter(bam_fhand, [dir1, dir2, dir3])
        assert  len(new_files) == 2

        dir1.close()
        dir2.close()
        dir3.close()

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
