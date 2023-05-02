"""
Tests for YarnCleaner
"""
import os
import unittest
import tempfile
from yarncleaner import YarnCleaner


class TestYarnCleaner(unittest.TestCase):
    """
    Test Yarn Cleaner class
    """
    def setUp(self):
        self.tmp_ssh_key_file = tempfile.NamedTemporaryFile(delete=False)
        self.tmp_ssh_key_file.write(b"ssh-private-key")
        self.tmp_ssh_key_file.close()

    def tearDown(self):
        os.unlink(self.tmp_ssh_key_file.name)

    def test_init_1(self):
        """
        Test the initialization of YarnCleaner
        """
        ycleaner = YarnCleaner(usercache_dir="/data/usercache", workers=["worker1", "worker2"],
                               ssh_key_file=self.tmp_ssh_key_file, ssh_username="ssh-private-key")
        self.assertEqual(ycleaner.usercache_dir, "/data/usercache")
        self.assertEqual(ycleaner.workers, ["worker1", "worker2"])

    def test_init_2(self):
        """
        Test the initialization of YarnCleaner with worker param as numbers
        """
        ycleaner = YarnCleaner(usercache_dir="/data/usercache", workers=6,
                               ssh_key_file=self.tmp_ssh_key_file,
                               ssh_username="ssh-private-key")
        ycleaner.worker_prefix = "worker"
        self.assertEqual(ycleaner.workers, ["worker01", "worker02", "worker03", "worker04", "worker05", "worker06"])
