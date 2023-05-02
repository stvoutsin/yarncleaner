"""
This module can be used to monitor and clean the
temporary directories that are populated during Yarn jobs
"""
from __future__ import annotations
import os
import argparse
import subprocess
import logging
import paramiko
logging.basicConfig()
logging.getLogger().setLevel(logging.ERROR)


def check_empty_params(func) -> object:
    """
    Checks that the params are not empty

    :param func: A function
    :return: The object
    """
    def wrapper(*args, **kwargs):
        for arg in args:
            if not arg:
                raise ValueError(f"Empty parameter passed")
        for key, val in kwargs.items():
            if not val:
                raise ValueError(f"Empty parameter passed")
        func(*args, **kwargs)

    return wrapper


def validate(func) -> object:
    """
    Validate params

    :param func: A function
    :return: object
    """
    def wrapper(*args, **kwargs):
        for key, val in kwargs.items():
            if key == "workers" and (not isinstance(val, list) and not isinstance(val, int)):
                raise ValueError(f"List expected for 'workers' parameter")
        func(*args, **kwargs)

    return wrapper


class SSHConnector:
    """
    Connect to ssh node to run commands
    """

    def __init__(self, worker: str, ssh_username: str, ssh_key_file: str) -> None:
        self.client = paramiko.SSHClient()
        self.worker = worker
        self.ssh_username = ssh_username
        self.ssh_key_file = ssh_key_file

    def __enter__(self):
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(self.worker, username=self.ssh_username,
                            key_filename=self.ssh_key_file)
        return self.client

    def __exit__(self, *args):
        self.client.close()


class YarnCleaner:
    """
    Checks disk usage of Yarn, and kills application that is using up more than a given threshold
    """
    COMMANDS = {"df": "df -P %s | awk 'NR==2 {print $5}'",
                "get_app_id": "yarn application -list | grep %s | awk '{print $1}'",
                "kill_app": "yarn application -kill %s",
                "list_dirs": "ls -1 %s"}

    @check_empty_params
    @validate
    def __init__(self, workers: list | int, ssh_username: str,
                 ssh_key_file: str,
                 usercache_dir: str = "/var/hadoop/data/usercache") -> None:
        self.usercache_dir = usercache_dir
        self._worker_prefix = "worker"
        self.workers = workers
        self.ssh_username = ssh_username
        self.ssh_key_file = ssh_key_file

    @property
    def worker_prefix(self):
        """
        Get workers prefix

        :return: worker_prefix
        :rtype: str
        """
        return self._worker_prefix

    @worker_prefix.setter
    def worker_prefix(self, prefix: str):
        """
        Set worker prefix str

        :param str prefix : Workers
        """
        self._worker_prefix = prefix

    @property
    def workers(self):
        """
        Get workers

        :return: The workers
         :rtype: list | int
        """
        return self._workers

    @workers.setter
    def workers(self, workers_param: int | list):
        """
        Set workers
        :param int | list workers_param:
        """
        def _generate_workers(num: int) -> list:
            for i in range(1, num+1):
                res = ""
                if i < 10:
                    res = "0"
                res += str(i)
                yield f"{self.worker_prefix}{res}"

        if isinstance(workers_param, list):
            self._workers = workers_param
        elif isinstance(workers_param, int):
            self._workers = list(_generate_workers(workers_param))
        else:
            raise ValueError(f"Unknown type of workers: {str(type(workers_param))}")

    def clean(self, threshold_percent: int = 50):
        """
        Clean the Yarn temp directory
        :param threshold_percent:
        :type threshold_percent: int
        """

        def get_disk_usage(client: paramiko.SSHClient, path: str) -> int:
            """
            Get the disk usage for a path on a remote host.

            :param client: paramiko.SSHClient
                The SSH client to use. The client that will
                be used to connect to the remote host.
            :param path: str
                The path to check. The path on the remote host
                to check for disk usage.

            :return: int
                The disk usage as a percentage.
                The percentage of disk space used on the remote host for the specified path.
            """
            output = run_command(client, self.COMMANDS['df'] % path)
            return int(output.strip().replace('%', ''))

        def get_user_directories(client: paramiko.SSHClient, path: str) -> list:
            """
            Get the list of usercache directories

            :param paramiko.SSHClient client: The SSH client to use.
            :param str path: The base path for the user directories
            :return: list
            """
            usercache_dirs = run_command(client, self.COMMANDS["list_dirs"] % path).splitlines()
            return usercache_dirs

        def get_application_id(client: paramiko.SSHClient, app_name: str) -> str:
            """
            Get the application ID

            :param paramiko.SSHClient client:
            :param str app_name:
            :return: The application id
            :rtype: str
            """
            try:
                app = run_command(client, self.COMMANDS["get_app_id"] % app_name).strip()
            except subprocess.CalledProcessError as exc:
                logging.error("No Yarn application found with name %s", app_name)
                raise exc
            return app

        def kill_application(client: paramiko.SSHClient, app: str) -> None:
            """
            Kill the application with name: app_id

            :param paramiko.SSHClient client:
            :param str app:
            """
            logging.info("Killing Yarn application %s", app)
            run_command(client, self.COMMANDS["kill_app"] % app)

        def run_command(client, command):
            """
            Run a shell command on a remote host via SSH and return the output as a string.

            :param paramiko.SSHClient client: The SSH client to use.
            :param str command: The command to run.

            :return: The output of the command.
            :rtype: str

            """
            _, stdout, stderr = client.exec_command(command)

            output = stdout.read().decode("utf-8").strip()
            error = stderr.read().decode("utf-8").strip()

            if error and len(error) > 0 and "INFO" not in error:
                raise subprocess.CalledProcessError(returncode=1,
                                                    cmd=command, output=output, stderr=error,)
            return output

        killed_apps = {}
        # Connect to each worker node and run the script
        for worker_node in self.workers:
            with SSHConnector(worker_node, self.ssh_username, self.ssh_key_file) as ssh_client:
                try:
                    user_directories = get_user_directories(ssh_client, self.usercache_dir)
                    # Iterate over the usercache directories
                    for user_dir in filter(lambda x: x not in killed_apps, user_directories):
                        disk_usage = get_disk_usage(ssh_client,
                                                    os.path.join(self.usercache_dir, user_dir))
                        # Check if the disk usage is above the threshold
                        if disk_usage > threshold_percent:
                            app_id = get_application_id(ssh_client, "spark-" + user_dir)
                            killed_apps[user_dir] = True
                            kill_application(ssh_client, app_id)
                            logging.info("Application killed: %s", app_id)
                except subprocess.CalledProcessError as cpe:
                    logging.exception(cpe)
                    continue

        if not killed_apps:
            logging.info("Disk usage is stable, no apps killed")


if __name__ == "__main__":

    # Define command line arguments
    PARSER = argparse.ArgumentParser(
        description="Monitor disk usage for Yarn applications and kill if necessary")
    PARSER.add_argument("--sshuser", type=str, required=True,
                        help="SSH Username for the remote machine")
    PARSER.add_argument("--sshkeyfile", type=str,
                        required=True, help="SSH Key file for the remote machine")
    PARSER.add_argument("--workers", type=str,
                        required=True, help="Comma-separated list of worker nodes")
    PARSER.add_argument("--threshold", type=int, required=True,
                        help="Percentage disk usage threshold that triggers a Yarn kill")
    PARSER.add_argument("--usercache_dir", type=str, required=True, help="User cache directory")

    # Parse command line arguments
    ARGUMENTS = PARSER.parse_args()

    if ARGUMENTS.workers:
        WORKERS = ARGUMENTS.workers.split(',')
    else:
        raise ValueError("No workers provided")
    CLEANER = YarnCleaner(ssh_username=ARGUMENTS.sshuser, ssh_key_file=ARGUMENTS.sshkeyfile,
                          workers=WORKERS, usercache_dir=ARGUMENTS.usercache_dir)

    # Check the disk usage and kill any Yarn applications as necessary
    CLEANER.clean(threshold_percent=ARGUMENTS.threshold)
