import tempfile
import unittest
import os

from toil.job import Job


class DockerCallTest(unittest.TestCase):
    """
    This class handles creating a tmpdir and Toil options suitable for a unittest.
    """
    def setUp(self):
        super(DockerCallTest, self).setUp()
        # the test tmpdir needs to be in the home directory so files written onto mounted
        # directories from a Docker container will be visible on the host
        # https://docs.docker.com/docker-for-mac/osxfs/
        home = os.path.expanduser("~") + '/'
        self.tmpdir = tempfile.mkdtemp(prefix=home)
        self.options = Job.Runner.getDefaultOptions(os.path.join(str(self.tmpdir), 'jobstore'))
        self.options.clean = 'always'

    def tearDown(self):
        # delete temp
        super(DockerCallTest, self).tearDown()
        for file in os.listdir(self.tmpdir):
            os.remove(os.path.join(self.tmpdir, file))
        os.removedirs(self.tmpdir)
