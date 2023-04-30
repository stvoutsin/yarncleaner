import unittest
from unittest.mock import Mock, patch
import paramiko
from yarncleaner import YarnCleaner
import tempfile
from unittest.mock import patch, Mock
from paramiko import SSHClient
import os, subprocess

class TestYarnCleaner(unittest.TestCase):

    def setUp(self):
        self.tmp_ssh_key_file = tempfile.NamedTemporaryFile(delete=False)
        self.tmp_ssh_key_file.write(b"ssh-private-key")
        self.tmp_ssh_key_file.close()

    def tearDown(self):
        os.unlink(self.tmp_ssh_key_file.name)
        yc = YarnCleaner(threshold_percent=75, usercache_dir="/data/usercache", workers=["worker1", "worker2"])

    def test_init(self):
        yc = YarnCleaner(threshold_percent=75, usercache_dir="/data/usercache", workers=["worker1", "worker2"])
        self.assertEqual(yc.threshold_percent, 75)
        self.assertEqual(yc.usercache_dir, "/data/usercache")
        self.assertEqual(yc.workers, ["worker1", "worker2"])

