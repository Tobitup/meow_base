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
MIDDLE_DIR = "middle_dir"
FINAL_DIR = "final_dir"

# Setup patterns and recipes
first_pattern = FileEventPattern(
    "first_pattern", 
    os.path.join(INPUT_DIR, "*"), 
    "recipe", 
    "infile", 
    parameters={
        "name":"Alice",
        "outfile":os.path.join("{BASE}", MIDDLE_DIR, "{FILENAME}")
    })
second_pattern = FileEventPattern(
    "second_pattern", 
    os.path.join(MIDDLE_DIR, "*"), 
    "recipe", 
    "infile", 
    parameters={
        "name":"Bruce",
        "outfile":os.path.join("{BASE}", FINAL_DIR, "{FILENAME}")
    }) 
recipe = PythonRecipe(
    "recipe", 
    [
        "outfile = 'placeholder'",
        "name = 'placeholder'",
        "with open(outfile, 'w') as f:",
        "   f.write(f'Hello from {name}\\n')",
    ]
)

# Collect together all patterns and recipes
patterns = {
    first_pattern.name: first_pattern,
    second_pattern.name: second_pattern
}
recipes = {
    recipe.name: recipe,
}

# Reset the monitored file directory and runner directories
for f in [FILE_BASE, "job_queue", "job_output"]:
    shutil.rmtree(f)
for f in [INPUT_DIR, MIDDLE_DIR, FINAL_DIR]:
    os.makedirs(os.path.join(FILE_BASE, f))

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

# Create the triggering file
Path(os.path.join(os.path.join(FILE_BASE, INPUT_DIR, "A.txt"))).touch()

# Give time for the runner to run
time.sleep(10)
hello_runner.stop()
