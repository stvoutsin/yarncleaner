# Yarn Cleaner

This module can be used to monitor and clean the temporary directories that are populated during Yarn jobs.


## Features

* Checks disk usage of Yarn and kills the application that is using up more than a given threshold
* Connects to a remote node via SSH and runs commands

## Prerequisites

* python3
* paramiko
* argparse

## Installation

Clone this repository:

    git clone https://github.com/<USERNAME>/<REPO_NAME>.git
    
cd into the repository: 

    cd <REPO_NAME>

Install dependencies: 
  
      pip install -r requirements.txt

## Usage

Using the Command Line
This module can be run from the command line with the following command:

### sh
    python yarn_cleaner.py [--workers [WORKERS [WORKERS ...]]] [--ssh-username SSH_USERNAME] [--ssh-key-file SSH_KEY_FILE] [--usercache-dir USERCACHE_DIR] [--threshold-percent THRESHOLD_PERCENT]

## Arguments
  
| Argument  | Description |
| ------------- | ------------- |
| --workers  | A list of workers  |
| --ssh-username  | The username to use for SSH  |
| --ssh-key-file | The path to the private key file to use for SSH  |
| --usercache-dir | The directory where the usercache is located  |
| --threshold-percent | The percentage of disk usage to trigger a clean  |
      	
  
## Using the API

This module can also be used as an API:
  
    from yarn_cleaner import YarnCleaner
    workers = ["worker01", "worker02", "worker03"]
    ssh_username = "username"
    ssh_key_file = "/path/to/key/file.pem"
    cleaner = YarnCleaner(workers=workers, ssh_username=ssh_username, ssh_key_file=ssh_key_file)
    cleaner.clean()


## License

This module is licensed under the GNU GENERAL PUBLIC LICENSE.
