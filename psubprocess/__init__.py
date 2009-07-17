'Some magic to get a nicer interface'

from . import prunner
from . import condor_runner

Popen = prunner.Popen
CondorPopen = condor_runner.Popen