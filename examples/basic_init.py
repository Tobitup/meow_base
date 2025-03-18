import os
import shutil
import sys
import time

import meow_base.core
import argparse



from pathlib import Path

from meow_base.patterns import FileEventPattern, WatchdogMonitor
from meow_base.recipes import PythonRecipe, PythonHandler
from meow_base.conductors import LocalPythonConductor
from meow_base.core import MeowRunner


FILE_BASE = "runner_base"
INPUT_DIR = "input_dir"


# print("CWD:", os.getcwd())
# print("FILE_BASE is:", FILE_BASE)

parser = argparse.ArgumentParser()
parser.add_argument("--start", help="start the remote runner", action="store_true")
parser.add_argument("--network", help="reset network to 0", action="store_const", const=1, default=0)
args = parser.parse_args()


# Setup pattern and recipe
hello_pattern = FileEventPattern(
    "hello_pattern", 
    os.path.join(INPUT_DIR, "*"), 
    "hello_recipe", 
    "infile", 
)


hello_recipe = PythonRecipe(
    "hello_recipe", 
    [
        "infile = 'placeholder'",
        "message = f'Hello as triggered by {infile}\\n'",
        "print(message)"
    ]
)

# Collect together all patterns and recipes
patterns = {
    hello_pattern.name: hello_pattern,
}
recipes = {
    hello_recipe.name: hello_recipe,
}

# Reset the monitored file directory and runner directories
for f in [FILE_BASE, "job_queue", "job_output"]:
     if os.path.exists(f):
        shutil.rmtree(f)
os.makedirs(os.path.join(FILE_BASE, INPUT_DIR))

# Setup the runner
""" hello_runner = MeowRunner(
    WatchdogMonitor(
        FILE_BASE,
        patterns,
        recipes
    ),
    PythonHandler(
        pause_time=1
    ),
    LocalPythonConductor(
        pause_time=1
    ),
    logging = 10,
    name="Local Runner"
    
) """




local_runner = MeowRunner(
    WatchdogMonitor(
        FILE_BASE,
        patterns,
        recipes
    ),
    PythonHandler(
        pause_time=1
    ),
    LocalPythonConductor(
        pause_time=1
    ),
    logging = 10,
    name="Local Runner", network = 1, ssh_config_alias="Own-System" # Docker-Nix
)


# Start the runner
""" if not args.start:
    hello_runner.start() """

""" if args.start:
    remote_runner.start() """

""" if not args.start:
    hello_runner.stop() """


# local_runner.start()
# local_runner.check_remote_runner_alive()
# local_runner.ip_addr = "172.20.28.89"
# local_runner.debug_port = 10001
# local_runner.send_message("Hello from local!")
# time.sleep(5)
host_ip = os.environ.get("HOST_IP")
local_runner.ip_addr = "127.0.1.1"

local_runner.start()
# time.sleep(2)  
# local_runner.open_local_handshake_socket_async("127.0.1.1", 10002)
# time.sleep(2)  

local_runner.send_attached_conductors()

time.sleep(5)
local_runner.remote_alive = False
local_runner.stop()