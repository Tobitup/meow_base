import os
import shutil
import time

from pathlib import Path

from meow_base.patterns import FileEventPattern, WatchdogMonitor
from meow_base.recipes import PythonRecipe, PythonHandler
from meow_base.conductors import LocalPythonConductor
from meow_base.core import MeowRunner

FILE_BASE = os.path.join("meow_base", "runner_base")
INPUT_DIR = "input_dir"


# print("CWD:", os.getcwd())
# print("FILE_BASE is:", FILE_BASE)

# Setup pattern and recipe
hello_pattern = FileEventPattern(
    "hello_pattern", 
    os.path.join(INPUT_DIR, "*"), 
    "hello_recipe", 
    "infile", 
)

print("line 27")

hello_recipe = PythonRecipe(
    "hello_recipe", 
    [
        
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

print("line 61")

# Setup the runner
hello_runner = MeowRunner(
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
    )
)

# Start the runner
hello_runner.start()

print("line 81")

# Create the triggering file
Path(os.path.join(os.path.join(FILE_BASE, INPUT_DIR, "A.txt"))).touch()

# Give time for the runner to run
time.sleep(5)
hello_runner.stop()

print("line 90")
