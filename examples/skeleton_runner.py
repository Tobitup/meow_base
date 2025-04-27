import os
from meow_base.patterns import FileEventPattern, WatchdogMonitor
from meow_base.recipes import PythonRecipe, PythonHandler
from meow_base.conductors import LocalPythonConductor
from meow_base.core import MeowRunner
import time

#host_ip = os.environ.get("HOST_IP")

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
    name="Remote Runner",
    role="remote",
    network = 1
)

# Currnetly needed to set the host IP back to the local machine
#remote_runner.ip_addr = host_ip

# remote_runner.ip_addr = "127.0.0.1"
# remote_runner.debug_port = 10002


remote_runner.start()
# remote_runner.send_message("Hello from remote!")
# remote_runner.open_remote_handshake_socket_async()

#remote_runner.send_attached_conductors()
#remote_runner.send_attached_handlers()
#remote_runner.send_attached_monitors()

#print(f"Name of Local Runner: {remote_runner.local_runner_name}")
#print(f"IP of Local Runner: {remote_runner.local_runner_ip}")

time.sleep(20)

# Used tomorrow to test if we can keep this running indefinitely, and then find a way to send a kill command to the remote, probably by saying when .stop is called on local, remotes should stop too.
while remote_runner.network:
   time.sleep(1)

print("Remote Runner is alive:", remote_runner.remote_alive)

#remote_runner.check_remote_runner_alive()

# remote_runner.stop()