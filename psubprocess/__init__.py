'Some magic to get a nicer interface'

__version__ = '0.1.1'

from . import prunner
from . import condor_runner

Popen = prunner.Popen
CondorPopen = condor_runner.Popen
