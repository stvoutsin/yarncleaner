import os
import subprocess
import paramiko
import argparse
import logging
logging.basicConfig()
logging.getLogger().setLevel(logging.ERROR)


def check_empty_params(func) -> object:
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
    def wrapper(*args, **kwargs):
        for key, val in kwargs.items():
            if key == "workers" and not type(val) == list:
                raise ValueError(f"List expected for 'workers' parameter")
        func(*args, **kwargs)

    return wrapper


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
    def __init__(self, threshold_percent: int = 50, usercache_dir: str = "/var/hadoop/data/usercache",
                 workers: list = None, ssh_username: str = None, ssh_key_file: str = None) -> None:
        self.threshold_percent = threshold_percent
        self.usercache_dir = usercache_dir
        self.workers = workers
        self.ssh_username = ssh_username
        self.ssh_key_file = ssh_key_file

    def clean(self):
        """
        Clean the Yarn temp directory

        Returns:
             None
        """

        class SSHConnector:
            def __init__(self, worker: str, ssh_username: str, ssh_key_file: str) -> None:
                self.client = paramiko.SSHClient()
                self.worker = worker
                self.ssh_username = ssh_username
                self.ssh_key_file = ssh_key_file

            def __enter__(self):
                self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                self.client.connect(self.worker, username=self.ssh_username, key_filename=self.ssh_key_file)
                return self.client

            def __exit__(self, *args):
                self.client.close()

        def get_disk_usage(client: paramiko.SSHClient, path: str) -> int:
            """
            Get the disk usage for a path on a remote host.

            Args:
                client (paramiko.SSHClient): The SSH client to use.
                path (str): The path to check.

            Returns:
                int: The disk usage as a percentage.
            """
            output = run_command(client, self.COMMANDS['df'] % path)
            return int(output.strip().replace('%', ''))

        def get_user_directories(client: paramiko.SSHClient, path: str) -> list:
            """
            Get the list of usercache directories

            Args:
                client (paramiko.SSHClient): The SSH client to use.
                path (str): The base path for the user directories

            Returns:

            """
            usercache_dirs = run_command(client, self.COMMANDS["list_dirs"] % path).splitlines()
            return usercache_dirs

        def get_application_id(client: paramiko.SSHClient, app_name: str) -> str:
            """
            Get the application id

            Args:
                client (paramiko.SSHClient): The SSH client to use.
                app_name (str): The name of the app.

            Returns:
                str: The application id
            """
            try:
                app = run_command(client, self.COMMANDS["get_app_id"] % app_name).strip()
            except subprocess.CalledProcessError as cpe:
                logging.error("No Yarn application found with name %s" % app_name)
                raise cpe
            return app

        def kill_application(client: paramiko.SSHClient, app: str) -> None:
            """
            Kill the application with name: app_id

            Args:
                client (paramiko.SSHClient): The SSH client to use.
                app (str): The app to delete.

            Returns:
                None
            """
            logging.info("Killing Yarn application %s" % app)
            run_command(client, self.COMMANDS["kill_app"] % app)
            return

        def run_command(client, command):
            """
            Run a shell command on a remote host via SSH and return the output as a string.

            Args:
                client (paramiko.SSHClient): The SSH client to use.
                command (str): The command to run.

            Returns:
                str: The output of the command.
            """
            stdin, stdout, stderr = client.exec_command(command)
            output = stdout.read().decode("utf-8").strip()
            error = stderr.read().decode("utf-8").strip()
            # TODO: Incorrectly getting error for when checking yarn list
            if error and len(error) > 0 and "INFO" not in error:
                raise subprocess.CalledProcessError(returncode=1, cmd=command, output=output, stderr=error)
            return output

        killed_apps = {}
        # Connect to each worker node and run the script
        for worker_node in self.workers:
            with SSHConnector(worker_node, self.ssh_username, self.ssh_key_file) as ssh_client:
                try:
                    user_directories = get_user_directories(ssh_client, self.usercache_dir)
                    # Iterate over the usercache directories
                    for user_dir in filter(lambda x: x not in killed_apps, user_directories):
                        usercache_path = os.path.join(self.usercache_dir, user_dir)
                        disk_usage = get_disk_usage(ssh_client, usercache_path)
                        # Check if the disk usage is above the threshold
                        if disk_usage > self.threshold_percent:
                            app_id = get_application_id(ssh_client, "spark-" + user_dir)
                            killed_apps[user_dir] = True
                            kill_application(ssh_client, app_id)
                            logging.info(f"Application killed: {app_id}")
                except Exception as e:
                    logging.exception(e)
                    continue

        if not killed_apps:
            logging.info("Disk usage is stable, no apps killed")


if __name__ == "__main__":

    # Define command line arguments
    parser = argparse.ArgumentParser(description="Monitor disk usage for Yarn applications and kill if necessary")
    parser.add_argument("--sshuser", type=str, required=True, help="SSH Username for the remote machine")
    parser.add_argument("--sshkeyfile", type=str, required=True, help="SSH Key file for the remote machine")
    parser.add_argument("--workers", type=str, required=True, help="Comma-separated list of worker nodes")
    parser.add_argument("--threshold", type=int, required=True,
                        help="Percentage disk usage threshold that triggers a Yarn kill")
    parser.add_argument("--usercache_dir", type=str, required=True, help="User cache directory")

    # Parse command line arguments
    arguments = parser.parse_args()

    if arguments.workers:
        workers_list = arguments.workers.split(',')
    else:
        raise ValueError("No workers provided")
    cleaner = YarnCleaner(threshold_percent=arguments.threshold,
                          ssh_username=arguments.sshuser, ssh_key_file=arguments.sshkeyfile,
                          workers=workers_list, usercache_dir=arguments.usercache_dir)

    # Check the disk usage and kill any Yarn applications as necessary
    cleaner.clean()
