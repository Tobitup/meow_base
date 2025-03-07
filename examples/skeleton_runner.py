import os
from meow_base.patterns import FileEventPattern, WatchdogMonitor
from meow_base.recipes import PythonRecipe, PythonHandler
from meow_base.conductors import LocalPythonConductor
from meow_base.core import MeowRunner


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
        recipes
    ),
    PythonHandler(
        pause_time=1
    ),
    LocalPythonConductor(
        pause_time=1
    ),
    logging = 10,
    name="Remote Runner"
)



remote_runner.start()
remote_runner.send_attached_conductors()
remote_runner.send_attached_handlers()
remote_runner.send_attached_monitors()

#remote_runner.check_remote_runner_alive()

#remote_runner.stop()