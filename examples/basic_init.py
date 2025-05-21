import os
import shutil
import sys
import time


from pathlib import Path

from meow_base.patterns import FileEventPattern, WatchdogMonitor
from meow_base.recipes import PythonRecipe, PythonHandler
from meow_base.conductors import LocalPythonConductor
from meow_base.core import MeowRunner


FILE_BASE = "runner_base"
INPUT_DIR = "input_dir"


# Setup pattern and recipe
hello_pattern = FileEventPattern(
    "hello_pattern", 
    os.path.join(INPUT_DIR, "*"), 
    "hello_recipe", 
    "infile", 
)

new_pattern = FileEventPattern(
    "new_pattern", 
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


local_runner = MeowRunner(
    WatchdogMonitor(
        FILE_BASE,
        patterns,
        recipes,
        name="monitor_1",
    ),
    PythonHandler(
        pause_time=1
    ),
    LocalPythonConductor(
        pause_time=1
    ),
    logging = 10,
    name="Local Runner", role = "local", network = 1, ssh_config_alias="Own-System"
)

second_monitor = WatchdogMonitor(
        FILE_BASE,
        patterns,
        recipes,
        name="monitor_2",
    )
local_runner.start()

time.sleep(10)

#local_runner.get_queue()
#local_runner.get_attached_conductors()
#local_runner.get_attached_handlers()
# local_runner.get_attached_monitors()
time.sleep(2)
#local_runner.add_monitor(target = "remote",monitor = second_monitor)
time.sleep(2)
#local_runner.get_attached_monitors()

#local_runner.add_pattern(target="remote", monitor_name = "monitor_1", pattern = new_pattern)
#local_runner.get_attached_patterns(target="remote", monitor = "monitor_1")
time.sleep(10)



local_runner.stop()