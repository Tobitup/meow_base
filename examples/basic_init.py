import os
import shutil
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
    logging = 20,
    name="Local Runner", role = "local", network = 1, ssh_config_alias="Container", runners_to_start=5)

second_monitor = WatchdogMonitor(
        FILE_BASE,
        patterns,
        recipes,
        name="monitor_2",
    )

local_runner.start()
time.sleep(5) # Sleep to let runner initialize

# Gets the attached monitors, patterns, recipes, and handlers from all runners
local_runner.get_queue(target="remote")
local_runner.get_attached_conductors(target="remote")
local_runner.get_attached_handlers(target="remote")
local_runner.get_attached_monitors(target="remote")
time.sleep(2) # Simuates work

# Add a pattern and monitor to the remote runner
local_runner.add_monitor(target = "remote", monitor = second_monitor)
time.sleep(2)
local_runner.get_attached_monitors("remote")
time.sleep(2)
local_runner.add_pattern(target="remote", monitor_name = "monitor_1", pattern = new_pattern)
local_runner.get_attached_patterns(target="remote", monitor = "monitor_1")
time.sleep(10) # Sleep to let remotes send a heartbeat

local_runner.stop()