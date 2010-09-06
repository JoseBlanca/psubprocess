'''
Utils to split and join bams

Created on 06/09/2010

@author: peio
'''
from psubprocess.utils import call


def bam2sam(bam_fhand, sam_fhand):
    '''It converts between bam and sam.'''
    bam_fhand.seek(0)
    cmd = ['samtools', 'view', bam_fhand.name, '-o', sam_fhand.name]
    call(cmd, raise_on_error=True)

def sam2bam(sam_fhand, bam_fhand, header=None):
    'It converts between bam and sam.'
    sam_fhand.seek(0)
    if header is not None:
        pass
    cmd = ['samtools', 'view', '-bSh', '-o', bam_fhand.name, sam_fhand.name]
    call(cmd, raise_on_error=True)

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


