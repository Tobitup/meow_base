import os
from meow_base.patterns import FileEventPattern, WatchdogMonitor
from meow_base.recipes import PythonRecipe, PythonHandler
from meow_base.conductors import LocalPythonConductor
from meow_base.core import MeowRunner
import time

# A Runner example file that can be started as a remote on a remote machine.

FILE_BASE = "runner_base"
INPUT_DIR = "input_dir"

remote_hello_pattern = FileEventPattern(
    "hello_pattern", 
    os.path.join(INPUT_DIR, "*"), 
    "hello_recipe", 
    "infile", 
)


remote_hello_recipe = PythonRecipe(
    "hello_recipe", 
    [
        "infile = 'placeholder'",
        "message = f'Hello as triggered by {infile}\\n'",
        "print(message)"
    ]
)

# Collect together all patterns and recipes
patterns = {
    remote_hello_pattern.name: remote_hello_pattern,
}
recipes = {
    remote_hello_recipe.name: remote_hello_recipe,
}

remote_runner = MeowRunner(
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
    role="remote",
    network = 1 # Set to active networking options
)

remote_runner.start()
time.sleep(1) # Gives runner time to start up


# Simulate work being done
while remote_runner.network:
   time.sleep(1)

