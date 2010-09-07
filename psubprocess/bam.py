'''
Utils to split and join bams

Created on 06/09/2010

@author: peio
'''
from psubprocess.utils import call, get_fhand
from tempfile import NamedTemporaryFile


def bam2sam(bam_fhand, sam_fhand, header=False):
    '''It converts between bam and sam.'''
    bam_fhand.seek(0)

    cmd = ['samtools', 'view', bam_fhand.name, '-o', sam_fhand.name]
    if header:
        cmd.insert(2, '-h')
    call(cmd, raise_on_error=True)
    sam_fhand.flush()

def sam2bam(sam_fhand, bam_fhand, header=None):
    'It converts between bam and sam.'
    sam_fhand.seek(0)
    if header is not None:
        pass
    cmd = ['samtools', 'view', '-bSh', '-o', bam_fhand.name, sam_fhand.name]
    call(cmd, raise_on_error=True)
    bam_fhand.flush()

def get_bam_header(bam_fhand, header_fhand):
    'It gets the header of the bam'
    cmd = ['samtools', 'view', '-H', bam_fhand.name, '-o', header_fhand.name]
    call(cmd, raise_on_error=True)

def bam_unigene_counter(fhand, expression=None):
    'It count the unigene number of a bam'
    unigenes = set()
    for line in fhand:
        unigene = line.split()[2]
        unigenes.add(unigene)
    return len(unigenes)

def unigenes_in_bam(fhand, expression=None):
    'It yields the bam mapping by joined by unigene'
    unigene_prev   = None
    unigene_lines = ''
    for line in fhand:
        unigene = line.split()[2]
        if unigene_prev is not None and unigene_prev != unigene:
            yield unigene_lines
            unigene_lines = ''

        unigene_lines += line
        unigene_prev = unigene
    else:
        yield unigene_lines

def bam_joiner(out_file, in_files):
    'It joins bam files'
    #are we working with fhands or fnames?
    out_fhand = get_fhand(out_file, writable=True)
    sam_fhand = NamedTemporaryFile(suffix='.sam')

    first = True
    for file_ in in_files:
        file_ = get_fhand(file_)
        if first:
            first = False
            bam2sam(file_, sam_fhand, header=True)

        else:
            sam_fhand_temp = NamedTemporaryFile(suffix='.sam')
            bam2sam(file_, sam_fhand_temp)
            sam_fhand_temp.seek(0)
            sam_fhand2 = open(sam_fhand.name, 'a')
            sam_fhand2.write(open(sam_fhand_temp.name).read())
            sam_fhand2.close()

    sam_fhand.flush()
    sam2bam(sam_fhand, out_fhand)
    out_fhand.flush()









