
"""
This file contains the defintion for the MeowRunner, the main construct used 
for actually orchestration MEOW analysis. It is intended as a modular system, 
with monitors, handlers, and conductors being swappable at initialisation.

Author(s): David Marchant
"""
import os
import socket
import sys
import threading
import paramiko
import select
import time
import json
import pickle
import base64
from multiprocessing import Pipe
from typing import Any, Optional, Union, Dict, List, Type, Tuple
from ..patterns.file_event_pattern import WatchdogMonitor, BasePattern
from .base_conductor import BaseConductor
from .base_handler import BaseHandler
from .base_monitor import BaseMonitor
from .vars import DEBUG_WARNING, DEBUG_INFO, \
    VALID_CHANNELS, META_FILE, DEFAULT_JOB_OUTPUT_DIR, DEFAULT_JOB_QUEUE_DIR, \
    JOB_STATUS, STATUS_QUEUED
from ..functionality.validation import check_type, valid_list, \
    valid_dir_path
from ..functionality.debug import setup_debugging, print_debug
from ..functionality.file_io import make_dir, threadsafe_read_status, \
    threadsafe_update_status
from ..functionality.process_io import wait


class MeowRunner:
    # A collection of all monitors in the runner
    monitors:List[BaseMonitor]
    # A collection of all handlers in the runner
    handlers:List[BaseHandler]
    # A collection of all conductors in the runner
    conductors:List[BaseConductor]
    # A collection of all inputs for the event queue
    event_connections: List[Tuple[VALID_CHANNELS,Union[BaseMonitor,BaseHandler]]]
    # A collection of all inputs for the job queue
    job_connections: List[Tuple[VALID_CHANNELS,Union[BaseHandler,BaseConductor]]]
    # Directory where queued jobs are initially written to
    job_queue_dir:str
    # Directory where completed jobs are finally written to
    job_output_dir:str
    # A queue of all events found by monitors, awaiting handling by handlers
    event_queue:List[Dict[str,Any]]
    # A queue of all jobs setup by handlers, awaiting execution by conductors
    job_queue:List[str]
    def __init__(self, monitors:Union[BaseMonitor,List[BaseMonitor]], 
            handlers:Union[BaseHandler,List[BaseHandler]], 
            conductors:Union[BaseConductor,List[BaseConductor]],
            job_queue_dir:str=DEFAULT_JOB_QUEUE_DIR,
            job_output_dir:str=DEFAULT_JOB_OUTPUT_DIR,
            print:Any=sys.stdout, logging:int=0,

            # Naming option for Runners
            name: Optional[str] = None, role:str="local",

            # Added Network Options
            network:int=0, ssh_config_alias:Any=None, ssh_private_key_dir:Any=os.path.expanduser("~/.ssh/id_ed25519"), msg_port:int=10001, rr_port:int=10002, hb_port:int=10005,
            runner_file_name:str=None, runners_to_start:int=1 )->None:
        """MeowRunner constructor. This connects all provided monitors, 
        handlers and conductors according to what events and jobs they produce 
        or consume."""

        # Assigning names to Runners
        if not name:
            self.name = f"Unnamed_Runner{os.getpid()}:{time.time():.0f}"
        else:
            self.name = f"{name}:{os.getpid()}"
        self.network = network
        self.ssh_config_alias = ssh_config_alias
        self.runners_to_start = runners_to_start
        self.role = role
        self.ssh_private_key_dir = ssh_private_key_dir
        self.runner_file_name = runner_file_name
        self.remote_runners: Dict[str, Dict[str, Any]] = {}  # Remote Runner dictonary

        self.msg_port = msg_port        # Port local Runner listens to for messages from remotes
        self.rr_port = rr_port          # Port set by remote Runners to store the IP assigned to them by the OS


        self._is_valid_job_queue_dir(job_queue_dir)
        self._is_valid_job_output_dir(job_output_dir)

        self.job_connections = []
        self.event_connections = []

        self._is_valid_monitors(monitors)
        # If monitors isn't a list, make it one
        if not type(monitors) == list:
            monitors = [monitors]
        self.monitors = monitors
        for monitor in self.monitors:
            # Create a channel from the monitor back to this runner
            monitor_to_runner_reader, monitor_to_runner_writer = Pipe()
            monitor.to_runner_event = monitor_to_runner_writer
            self.event_connections.append((monitor_to_runner_reader, monitor))

        self._is_valid_handlers(handlers)
        # If handlers isn't a list, make it one
        if not type(handlers) == list:
            handlers = [handlers]
        for handler in handlers:            
            handler.job_queue_dir = job_queue_dir

            # Create channels from the handler back to this runner
            h_to_r_event_runner, h_to_r_event_handler = Pipe(duplex=True)
            h_to_r_job_reader, h_to_r_job_writer = Pipe()

            handler.to_runner_event = h_to_r_event_handler
            handler.to_runner_job = h_to_r_job_writer
            self.event_connections.append((h_to_r_event_runner, handler))
            self.job_connections.append((h_to_r_job_reader, handler))
        self.handlers = handlers

        self._is_valid_conductors(conductors)
        # If conductors isn't a list, make it one
        if not type(conductors) == list:
            conductors = [conductors]
        for conductor in conductors:
            conductor.job_output_dir = job_output_dir
            conductor.job_queue_dir = job_queue_dir

            # Create a channel from the conductor back to this runner
            c_to_r_job_runner, c_to_r_job_conductor = Pipe(duplex=True)

            conductor.to_runner_job = c_to_r_job_conductor
            self.job_connections.append((c_to_r_job_runner, conductor))
        self.conductors = conductors

        # Create channel to send stop messages to monitor/handler thread
        self._stop_mon_han_pipe = Pipe()
        self._mon_han_worker = None

        # Create channel to send stop messages to handler/conductor thread
        self._stop_han_con_pipe = Pipe()
        self._han_con_worker = None

        # Create new channel for sending stop messages to listener threads
        self._stop_listener_pipe = Pipe()
        self._network_listener_worker = None
        self._stop_remote_listener_pipe = Pipe()
        self._remote_network_listener_worker = None

        # Create new channel for sending stop messages to heartbeat listener and sender threads
        self._stop_heartbeat_listener_pipe = Pipe()
        self._heartbeat_listener_worker = None
        self._heartbeat_sender_worker = None

        # Setup debugging
        self._print_target, self.debug_level = setup_debugging(print, logging)

        # Setup queues
        self.event_queue = []
        self.job_queue = []

        # Get the local IP address, so each runner instantly knows its own IP
        self.local_ip_addr = self._get_local_ip()

        # Contained variables that only the Remote Runner should set
        self.local_runner_name = None
        self.local_runner_ip = None

        # Used by remote Runners to log time of last heartbeat recived from the local Runner
        self.last_heartbeat_from_local = time.time()
        self.last_network_communication = time.time()
        self.hb_port = hb_port  # Standard 10005

        # Threads active on Remote side
        self.hb_sender_thread_active = False
        self.hb_timeout_thead = None
        
        # Path to a Runner if a user wishes to send one over to the remote
        self.runner_file_path = None

    def _add_local_pattern(self, monitor_name:str, pattern:BasePattern)->None:
        """Function to add a local pattern to the runner."""
        desired_monitor = self.get_monitor_by_name(monitor_name)
        if desired_monitor is None:
            msg = f"Monitor with name {monitor_name} not found."
            print_debug(self._print_target, self.debug_level, 
                msg, DEBUG_WARNING)
            raise ValueError(msg)
        desired_monitor.add_pattern(pattern)
        print_debug(self._print_target, self.debug_level,
            f"Added pattern {pattern.name} to monitor {monitor_name}.", DEBUG_INFO)
        return


    def _delete_local_pattern(self, monitor_name:str, pattern:str)->None:
        """Function to delete a local pattern from a Monitor attached to a runner"""
        desired_monitor = self.get_monitor_by_name(monitor_name)
        if desired_monitor is None:
            msg = f"Monitor with name {monitor_name} not found."
            print_debug(self._print_target, self.debug_level,
                        msg, DEBUG_WARNING)
            raise ValueError(msg)
        desired_monitor.remove_pattern(pattern)
        print_debug(self._print_target, self.debug_level,
                    f"Deleted pattern {pattern} from monitor {monitor_name}.", DEBUG_INFO)


    def _add_local_monitor(self, monitor:BaseMonitor)->None:
        """Function to add a local monitor to the runner."""
        if not isinstance(monitor, BaseMonitor):
            msg = "Monitor must be of type BaseMonitor."
            print_debug(self._print_target, self.debug_level, 
                msg, DEBUG_WARNING)
            raise TypeError(msg)
        # Add the monitor and create a channel to it
        self.monitors.append(monitor)
        monitor_to_runner_reader, monitor_to_runner_writer = Pipe()
        monitor.to_runner_event = monitor_to_runner_writer
        self.event_connections.append((monitor_to_runner_reader, monitor))
        print_debug(self._print_target, self.debug_level,
            f"Added monitor {monitor.name} to runner.", DEBUG_INFO)
        monitor.start()
        print_debug(self._print_target, self.debug_level,
            f"Started monitor {monitor.name}.", DEBUG_INFO)
        return


    def run_monitor_handler_interaction(self)->None:
        """Function to be run in its own thread, to handle any inbound messages
        from monitors. These will be events, which should be matched to an 
        appropriate handler and handled."""
        all_inputs = [i[0] for i in self.event_connections] \
                     + [self._stop_mon_han_pipe[0]]
        while True:
            ready = wait(all_inputs)

            # If we get a message from the stop channel, then finish
            if self._stop_mon_han_pipe[0] in ready:
                return
            else:
                for connection, component in self.event_connections:
                    if connection not in ready:
                        continue
                    message = connection.recv()

                    # Recieved an event
                    if isinstance(component, BaseMonitor):
                        self.event_queue.append(message)
                        continue
                    # Recieved a request for an event
                    if isinstance(component, BaseHandler):
                        valid = False
                        for event in self.event_queue:
                            try:
                                valid, _ = component.valid_handle_criteria(event)
                            except Exception as e:
                                print_debug(
                                    self._print_target, 
                                    self.debug_level, 
                                    "Could not determine validity of "
                                    f"event for handler {component.name}. {e}", 
                                    DEBUG_INFO
                                )
                            
                            if valid:
                                self.event_queue.remove(event)
                                connection.send(event)
                                break
                        
                        # If nothing valid then send a message
                        if not valid:
                            connection.send(1)

    def run_handler_conductor_interaction(self)->None:
        """Function to be run in its own thread, to handle any inbound messages
        from handlers. These will be jobs, which should be matched to an 
        appropriate conductor and executed."""
        all_inputs = [i[0] for i in self.job_connections] \
                     + [self._stop_han_con_pipe[0]]
        while True:
            ready = wait(all_inputs)

            # If we get a message from the stop channel, then finish
            if self._stop_han_con_pipe[0] in ready:
                return
            else:
                for connection, component in self.job_connections:
                    if connection not in ready:
                        continue

                    message = connection.recv()

                    # Recieved a job
                    if isinstance(component, BaseHandler):
                        self.job_queue.append(message)
                        threadsafe_update_status(
                            {
                                JOB_STATUS: STATUS_QUEUED
                            },
                            os.path.join(message, META_FILE)
                        )
                        continue
                    # Recieved a request for a job
                    if isinstance(component, BaseConductor):
                        valid = False
                        for job_dir in self.job_queue:
                            try:
                                metafile = os.path.join(job_dir, META_FILE)
                                job = threadsafe_read_status(metafile)
                            except Exception as e:
                                print_debug(
                                    self._print_target, 
                                    self.debug_level, 
                                    "Could not load necessary job definitions "
                                    f"for job at '{job_dir}'. {e}", 
                                    DEBUG_INFO
                                )

                            try:
                                valid, _ = component.valid_execute_criteria(job)
                            except Exception as e:
                                print_debug(
                                    self._print_target, 
                                    self.debug_level, 
                                    "Could not determine validity of "
                                    f"job for conductor {component.name}. {e}", 
                                    DEBUG_INFO
                                )
                            
                            if valid:
                                self.job_queue.remove(job_dir)
                                connection.send(job_dir)
                                break

                        # If nothing valid then send a message
                        if not valid:
                            connection.send(1)

    def setup_listener_thread(self) -> None:
        """Function to setup a listener thread to listen for messages from 
        the remote machine, and spin up new threads for each new connection request."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.local_ip_addr, self.msg_port))
            s.listen(128)
            print_debug(self._print_target, self.debug_level,
                "Listener thread listening...", DEBUG_INFO)
            
            all_inputs = [s, self._stop_listener_pipe[0]]
            while True:
                ready, _, _ = select.select(all_inputs, [], [])
                print(f"DEBUG: Ready: {ready}")

                # If we get a message from the stop channel, then finish
                if self._stop_listener_pipe[0] in ready:
                    print_debug(self._print_target, self.debug_level,
                        "Listener thread stopped", DEBUG_INFO)
                    return
                
                if s in ready:
                    conn, addr = s.accept()
                    print_debug(self._print_target, self.debug_level,
                        f"Accepted connection from {addr}", DEBUG_INFO)
                    listener_thread = threading.Thread(target=self.handle_listener_thread, args=(conn, addr))
                    listener_thread.daemon = True
                    listener_thread.start()


    def setup_remote_listener_thread(self) -> None:
        """Function similar to setup_listener_thread, customized for the remote runner."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.local_ip_addr, 0))
            # OS assigns an available port, and that port is stored for later transmition to the local Runner
            actual_port = s.getsockname()[1]
            self.rr_port = actual_port
            print_debug(self._print_target, self.debug_level,
                f"Remote listener thread bound to port {actual_port}", DEBUG_INFO)
            s.listen(128)
            print_debug(self._print_target, self.debug_level,
                "Remote listener thread listening...", DEBUG_INFO)
            
            all_inputs = [s, self._stop_remote_listener_pipe[0]]
            while True:
                ready, _, _ = select.select(all_inputs, [], [])

                # If we get a message from the stop channel, then finish
                if self._stop_remote_listener_pipe[0] in ready:
                    print_debug(self._print_target, self.debug_level,
                        "Remote listener thread stopped", DEBUG_INFO)
                    return
                if s in ready:
                    conn, addr = s.accept()
                    print_debug(self._print_target, self.debug_level,
                        f"Remote accepted connection from {addr}", DEBUG_INFO)
                    listener_thread = threading.Thread(target=self.handle_listener_thread, args=(conn, addr))
                    listener_thread.daemon = True
                    listener_thread.start()
                    print_debug(self._print_target, self.debug_level,
                        "Remote listener thread started", DEBUG_INFO)


    def _setup_heartbeat_listener(self):
        """Function to setup a heartbeat listener thread to listen for heartbeats
        from remote runners."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            s.bind((self.local_ip_addr, self.hb_port))
            s.listen(128)
            print_debug(self._print_target, self.debug_level,
                        "Heartbeat listener thread listening...", DEBUG_INFO)

            all_inputs = [s, self._stop_heartbeat_listener_pipe[0]]
            while True:
                ready, _, _ = select.select(all_inputs, [], [])
                # If we get a message from the stop channel, then finish
                if self._stop_heartbeat_listener_pipe[0] in ready:
                    print_debug(self._print_target, self.debug_level,
                        "HB listener thread stopped", DEBUG_INFO)
                    return
                if s in ready:
                    conn, addr = s.accept()
                    listener_thread = threading.Thread(target=self.heartbeat_listener, args=(conn, addr))
                    listener_thread.daemon = True
                    listener_thread.start()


    def heartbeat_listener(self, conn, addr):
        """Function to handle incoming heartbeats from remote runners.
        Always started in a new thread by the setup_heartbeat_listener function."""
        with conn:
            while self.network == 1:
                try:
                    conn.settimeout(5)
                    data = conn.recv(1024)
                    if not data:
                        break
                    heartbeat = json.loads(data.decode())
                    rname = heartbeat.get("name")
                    hb_from_remote = heartbeat.get("timestamp", time.time())
                    print_debug(self._print_target, self.debug_level,
                                f"Received heartbeat from {heartbeat.get('name')}", DEBUG_INFO)
                    # If some runner was overlooked register it here as extra safety
                    if rname not in self.remote_runners:
                        self.remote_runners[rname] = {
                            "ip": addr[0],
                            "last_hb": time.time(),
                            "hb_checker": None
                        }
                    else:
                        self.remote_runners[rname]["last_hb"] = time.time()
                    
                    # When heartbeat is recieved, prepare and send an ack back, so remote knows local is recieving heartbeats, and remote
                    # can update its last comunication recieved from local Runner
                    ack_response = {
                        "type": "hb_ack",
                        "role": self.role,
                        "name": self.name,
                        "remote_timestamp": hb_from_remote,
                        "timestamp": time.time()
                    }
                    conn.sendall(json.dumps(ack_response).encode())
                    return
                except Exception as e:
                    print_debug(self._print_target, self.debug_level,
                                f"Error sending Heartbeat ACK back: {e}", DEBUG_WARNING)
                    break
    
    def start(self)->None:
        """Function to start the runner by starting all of the constituent 
        monitors, handlers and conductors, along with managing interaction 
        threads."""

        # Start all monitors
        for monitor in self.monitors:
            monitor.start()

        # Start all handlers
        for handler in self.handlers:
            handler.start()

        # Start all conductors
        for conductor in self.conductors:
            conductor.start()
        
        # If we've not started the monitor/handler interaction thread yet, then
        # do so
        if self._mon_han_worker is None:
            self._mon_han_worker = threading.Thread(
                target=self.run_monitor_handler_interaction,
                args=[])
            self._mon_han_worker.daemon = True
            self._mon_han_worker.start()
            print_debug(self._print_target, self.debug_level, 
                "Starting MeowRunner event handling...", DEBUG_INFO)
        else:
            msg = "Repeated calls to start MeowRunner event handling have " \
                "no effect."
            print_debug(self._print_target, self.debug_level, 
                msg, DEBUG_WARNING)
            raise RuntimeWarning(msg)

        # If we've not started the handler/conductor interaction thread yet, 
        # then do so
        if self._han_con_worker is None:
            self._han_con_worker = threading.Thread(
                target=self.run_handler_conductor_interaction,
                args=[])
            self._han_con_worker.daemon = True
            self._han_con_worker.start()
            print_debug(self._print_target, self.debug_level, 
                "Starting MeowRunner job conducting...", DEBUG_INFO)
        else:
            msg = "Repeated calls to start MeowRunner job conducting have " \
                "no effect."
            print_debug(self._print_target, self.debug_level, 
                msg, DEBUG_WARNING)
            raise RuntimeWarning(msg)
        
        print_debug(self._print_target, self.debug_level,
                f"Started {self.name}", DEBUG_INFO)


        # If we're in network mode, then setup the network connection

        # Local side
        if self.network == 1 and self.role == "local":
            print_debug(self._print_target, self.debug_level,
                "Setting up local network connection...", DEBUG_INFO)
            if self._network_listener_worker is None:
                # Setting up listening thread on Local system
                self._network_listener_worker = threading.Thread(
                    target=self.setup_listener_thread,
                    args=[])
                self._network_listener_worker.daemon = True
                self._network_listener_worker.start()
                print_debug(self._print_target, self.debug_level, 
                    "Starting Local MeowRunner network listener...", DEBUG_INFO)
            else:
                msg = "Repeated calls to start MeowRunner network listener " \
                    "have no effect."
                print_debug(self._print_target, self.debug_level, 
                    msg, DEBUG_WARNING)
                raise RuntimeWarning(msg)
            self.heartbeat_thread_dealer()
            self.setup_ssh_connection_to_remote()


        # Remote side
        if self.network == 1 and self.role == "remote":
            print_debug(self._print_target, self.debug_level,
                "Setting up remote network connection...", DEBUG_INFO)
            # Setup the listener threads on the Remote system
            if self._remote_network_listener_worker is None:
                self._remote_network_listener_worker = threading.Thread(
                    target=self.setup_remote_listener_thread,
                    args=[])
                self._remote_network_listener_worker.daemon = True
                self._remote_network_listener_worker.start()
                print_debug(self._print_target, self.debug_level,
                    "Starting Remote MeowRunner network listener...", DEBUG_INFO)
            else:
                msg = "Repeated calls to start MeowRunner remote network listener " \
                    "have no effect."
                print_debug(self._print_target, self.debug_level, 
                    msg, DEBUG_WARNING)
                raise RuntimeWarning(msg)
            time.sleep(0.5)     # Wait for the remote listener to start
            self.load_transfered_network_config()
            self.send_handshake_to_local()
            self.heartbeat_thread_dealer()


    def stop(self)->None:
        """Function to stop the runner by stopping all of the constituent 
        monitors, handlers and conductors, along with managing interaction 
        threads."""

        # Shutdown logic to ensure all remote Runners are stopped before the local is
        if self.role == "local" and self.network == 1:
            time_to_wait_to_send_another_stop = 15
            time_to_wait_for_shutdown = 45
            start = time.time()
            for rname in list(self.remote_runners.keys()):
                self._send_stop_cmd_to_remote(rname)
                continue
            # Wait for all remote Runners to shutdown
            while len(self.remote_runners) > 0:
                print_debug(self._print_target, self.debug_level,
                    "Waiting for all remote runners to shutdown...", DEBUG_INFO)
                time.sleep(2)
                time_elaped = time.time() - start

                if time_elaped > time_to_wait_to_send_another_stop:
                    print_debug(self._print_target, self.debug_level,
                        "Sending stop command to all remote runners again", DEBUG_INFO)
                    for rname in list(self.remote_runners.keys()):
                        self._send_stop_cmd_to_remote(rname)

                if time_elaped > time_to_wait_for_shutdown:
                    print_debug(self._print_target, self.debug_level,
                        "Timed out waiting for remote runners to shutdown, shutting down local", DEBUG_WARNING)
                    self.network = 0
                    break
            if len(self.remote_runners) == 0:
                print_debug(self._print_target, self.debug_level,
                    "All Remote Runners Shutdown - Shutting down Local", DEBUG_INFO)
                self.network = 0

        # Stop all the monitors
        for monitor in self.monitors:
            monitor.stop()

        # Stop all handlers, if they need it
        for handler in self.handlers:
            handler.stop()

        # Stop all conductors, if they need it
        for conductor in self.conductors:
            conductor.stop()

        # If we've started the monitor/handler interaction thread, then stop it
        if self._mon_han_worker is None:
            msg = "Cannot stop event handling thread that is not started."
            print_debug(self._print_target, self.debug_level, 
                msg, DEBUG_WARNING)
            raise RuntimeWarning(msg)
        else:
            self._stop_mon_han_pipe[1].send(1)
            self._mon_han_worker.join()
        print_debug(self._print_target, self.debug_level,
            "Event handler thread stopped", DEBUG_INFO)

        # If we've started the handler/conductor interaction thread, then stop 
        # it
        if self._han_con_worker is None:
            msg = "Cannot stop job conducting thread that is not started."
            print_debug(self._print_target, self.debug_level, 
                msg, DEBUG_WARNING)
            raise RuntimeWarning(msg)
        else:
            self._stop_han_con_pipe[1].send(1)
            self._han_con_worker.join()
        print_debug(self._print_target, self.debug_level, 
            "Job conductor thread stopped", DEBUG_INFO)

        if self.role == "remote" and self.network == 1 and self.local_runner_ip:
            self._confirm_remote_runner_shutdown()
            self.network = 0
        
        # Closes local network threads
        if self._network_listener_worker is None and self.role == "local" and self.network == 1:
            msg = "Cannot stop remote network listener thread that is not started."
            print_debug(self._print_target, self.debug_level,
                msg, DEBUG_WARNING)
            raise RuntimeWarning(msg)
        else:
            if self.role == "local" and self.network == 1:
                self._stop_listener_pipe[1].send(1)
                self._network_listener_worker.join()
                
                self._stop_heartbeat_listener_pipe[1].send(1)
                self._heartbeat_listener_worker.join()
                if self.hb_timeout_thead is not None:
                    self.hb_timeout_thead.join()
                print_debug(self._print_target, self.debug_level,
                    "Local Network listener thread stopped", DEBUG_INFO)
        
        # Closes remote network threads
        if self._remote_network_listener_worker is None and self.role == "remote" and self.network == 1:
            msg = "Cannot stop network listener thread that is not started."
            print_debug(self._print_target, self.debug_level,
                msg, DEBUG_WARNING)
            raise RuntimeWarning(msg)
        else:
            if self.role == "remote" and self.network == 1:
                self._stop_remote_listener_pipe[1].send(1)
                self._remote_network_listener_worker.join()
                self._heartbeat_sender_worker.join()
                if self.hb_timeout_thead is not None:
                    self.hb_timeout_thead.join()
                print_debug(self._print_target, self.debug_level,
                    "Remote Network listener thread stopped", DEBUG_INFO)

        
    def _send_stop_cmd_to_remote(self, rname) -> None:
        """Function to send a stop command to the remote machine,
        used when the local calls .stop()."""

        if self.role != "local":
            print_debug(self._print_target, self.debug_level,
                "Stop commands can only be sent from a local Runner", DEBUG_WARNING)
            return
        
        stop_msg = {
            "type": "runner_shutdown",
            "requested by": self.name,
            "reason": "Local Runner called .stop()"
        }
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(15)
                runner_ip = self.remote_runners[rname]["ip"]
                runner_port = self.remote_runners[rname]["port"]
                s.connect((runner_ip, runner_port))
                s.sendall(json.dumps(stop_msg).encode())
                self.remote_runners[rname]["last_hb"] = time.time()
                print_debug(self._print_target, self.debug_level,
                    f"Stop message sent to {rname}", DEBUG_INFO)
        except Exception as e:
            print_debug(self._print_target, self.debug_level,
                f"Failed to send stop command to Remote: {e}", DEBUG_WARNING)
            del self.remote_runners[rname] # Remove the remote Runner from dictionary and asume it dead
            return



    def _confirm_remote_runner_shutdown(self) -> None:
        """Function to confirm the shutdown of the remote Runner,
        sent from remote Runners when they recieve a shutdown request
        from a local Runner."""
        if self.role != "remote":
            print_debug(self._print_target, self.debug_level,
                "Stop ack command can only be sent from remote runner", DEBUG_WARNING)
            return
        
        confirm_shtdwn_msg = {
            "type": "runner_shutdown_ack",
            "runner stopped": self.name,
            "timestamp": time.time()
        }
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect((self.local_runner_ip, self.msg_port))
                s.sendall(json.dumps(confirm_shtdwn_msg).encode())
                self.last_network_communication = time.time()
                print_debug(self._print_target, self.debug_level,
                    "Ack of shutdown sent to Local machine", DEBUG_INFO)
                return
        except Exception as e:
            print_debug(self._print_target, self.debug_level,
                f"Failed to send ack of shutdown command to local: {e}", DEBUG_WARNING)
            return
        

    def get_monitor_by_name(self, queried_name:str)->BaseMonitor:
        """Gets a runner monitor with a name matching the queried name. Note 
        in the case of multiple monitors having the same name, only the first 
        match is returned."""
        return self._get_entity_by_name(queried_name, self.monitors)

    def get_monitor_by_type(self, queried_type:Type)->BaseMonitor:
        """Gets a runner monitor with a type matching the queried type. Note 
        in the case of multiple monitors having the same name, only the first 
        match is returned."""
        return self._get_entity_by_type(queried_type, self.monitors)

    def get_handler_by_name(self, queried_name:str)->BaseHandler:
        """Gets a runner handler with a name matching the queried name. Note 
        in the case of multiple handlers having the same name, only the first 
        match is returned."""
        return self._get_entity_by_name(queried_name, self.handlers)

    def get_handler_by_type(self, queried_type:Type)->BaseHandler:
        """Gets a runner handler with a type matching the queried type. Note 
        in the case of multiple handlers having the same name, only the first 
        match is returned."""
        return self._get_entity_by_type(queried_type, self.handlers)

    def get_conductor_by_name(self, queried_name:str)->BaseConductor:
        """Gets a runner conductor with a name matching the queried name. Note 
        in the case of multiple conductors having the same name, only the first 
        match is returned."""
        return self._get_entity_by_name(queried_name, self.conductors)

    def get_conductor_by_type(self, queried_type:Type)->BaseConductor:
        """Gets a runner conductor with a type matching the queried type. Note 
        in the case of multiple conductors having the same name, only the first 
        match is returned."""
        return self._get_entity_by_type(queried_type, self.conductors)

    def _get_entity_by_name(self, queried_name:str, 
            entities:List[Union[BaseMonitor,BaseHandler,BaseConductor]]
            )->Union[BaseMonitor,BaseHandler,BaseConductor]:
        """Base function inherited by more specific name query functions."""
        for entity in entities:
            if entity.name == queried_name:
                return entity
        return None

    def _get_entity_by_type(self, queried_type:Type, 
            entities:List[Union[BaseMonitor,BaseHandler,BaseConductor]]
            )->Union[BaseMonitor,BaseHandler,BaseConductor]:
        """Base function inherited by more specific type query functions."""
        for entity in entities:
            if isinstance(entity, queried_type):
                return entity
        return None

    def _is_valid_monitors(self, 
            monitors:Union[BaseMonitor,List[BaseMonitor]])->None:
        """Validation check for 'monitors' variable from main constructor."""
        check_type(
            monitors, 
            BaseMonitor, 
            alt_types=[List], 
            hint="MeowRunner.monitors"
        )
        if type(monitors) == list:
            valid_list(monitors, BaseMonitor, min_length=1)

    def _is_valid_handlers(self, 
            handlers:Union[BaseHandler,List[BaseHandler]])->None:
        """Validation check for 'handlers' variable from main constructor."""
        check_type(
            handlers, 
            BaseHandler, 
            alt_types=[List], 
            hint="MeowRunner.handlers"
        )
        if type(handlers) == list:
            valid_list(handlers, BaseHandler, min_length=1)

    def _is_valid_conductors(self, 
            conductors:Union[BaseConductor,List[BaseConductor]])->None:
        """Validation check for 'conductors' variable from main constructor."""
        check_type(
            conductors, 
            BaseConductor, 
            alt_types=[List], 
            hint="MeowRunner.conductors"
        )
        if type(conductors) == list:
            valid_list(conductors, BaseConductor, min_length=1)

    def _is_valid_job_queue_dir(self, job_queue_dir)->None:
        """Validation check for 'job_queue_dir' variable from main 
        constructor."""
        valid_dir_path(job_queue_dir, must_exist=False)
        if not os.path.exists(job_queue_dir):
            make_dir(job_queue_dir)

    def _is_valid_job_output_dir(self, job_output_dir)->None:
        """Validation check for 'job_output_dir' variable from main 
        constructor."""
        valid_dir_path(job_output_dir, must_exist=False)
        if not os.path.exists(job_output_dir):
            make_dir(job_output_dir)


    def deconstruct_ssh_alias(self, ssh_config_path:Any=os.path.expanduser("~/.ssh/config")):
        """Function to deconstruct the SSH alias and return its components:
        hostname, username, port."""
        try:
            ssh_config = paramiko.SSHConfig.from_path(ssh_config_path)
            found_conf = ssh_config.lookup(self.ssh_config_alias)

            # Get the values if present in the config file
            conf_host_name = found_conf.get("hostname")
            conf_user = found_conf.get("user")
            conf_port = found_conf.get("port")
            return conf_host_name, conf_user, conf_port
        except Exception as e:
            print_debug(self._print_target, self.debug_level,
                        f"Error reading SSH config file: {e}", DEBUG_WARNING)
            return None, None, None


    def handle_listener_thread(self, conn, addr):
        """Function to handle incoming messages from the remote Runners.
        Always started in a new thread by the setup_listener_thread function,
        and is the "big listening thread"."""
        with conn:
            while self.network == 1:
                try:
                    conn.settimeout(15)
                    data = conn.recv(1024)
                    if not data:
                        break
                    msg = data.decode()
                    try:
                        msg_data = json.loads(msg)
                    except Exception:
                        msg_data = {}

                    # Handles handshake messages
                    if msg_data.get("type") == "handshake" and msg_data.get("role") == "remote":
                        remote_runner_name = msg_data.get("name")
                        remote_runner_ip = msg_data.get("ip")
                        remot_runner_port = msg_data.get("port")
                        print_debug(self._print_target, self.debug_level,
                                    f"Local runner received remote handshake: Name = {remote_runner_name}, IP = {remote_runner_ip}, Port = {remot_runner_port}",
                                    DEBUG_INFO)
                        hostname, user, port = self.deconstruct_ssh_alias()
                        old_value = self.remote_runners.get(remote_runner_name, {})
                        # Ensures info about a remote Runner having been restarted is not overwritten
                        restart_flag = old_value.get("restart_attempted", False)
                        self.remote_runners[remote_runner_name] = {
                            "ip": remote_runner_ip,
                            "port": remot_runner_port,
                            "last_hb": time.time(),
                            "ssh_hostname" : hostname,
                            "ssh_user" : user,
                            "ssh_port" : port,
                            "runner_file": self.runner_file_path,
                            "restart_attempted": restart_flag
                        }         
                        # send a handshake-ack back to the remote Runner
                        ack_response = "Handshake Acknowledged" 
                        conn.sendall(ack_response.encode())
                        break


                    # Handles shutdown of remote Runners
                    elif msg_data.get("type") == "runner_shutdown" and self.role == "remote":
                        print_debug(self._print_target, self.debug_level,
                                    f"Remote runner shutdown requested by {msg_data.get('requested by')}: {msg_data.get('reason')}",
                                    DEBUG_INFO)
                        self.stop()
                        break


                    # Handles registering a successfull remote Runner shutdown
                    elif msg_data.get("type") == "runner_shutdown_ack" and self.role == "local":
                        print_debug(self._print_target, self.debug_level,
                                f"Local Runner shutdown acknowledged by {msg_data.get('runner stopped')}",DEBUG_INFO)
                        del self.remote_runners[msg_data.get("runner stopped")]
                        break
                        

                    # Handles getting the job queue of the remote Runner
                    elif msg_data.get("type") == "get_queue" and self.role == "remote":
                        print_debug(self._print_target, self.debug_level,
                                    f"Job queue of Remote Runenr requested by: {msg_data.get('requested by')}",
                                    DEBUG_INFO)
                        job_queue = json.dumps(self.job_queue).encode()
                        conn.sendall(job_queue)
                        self.last_network_communication = time.time()
                        print_debug(self._print_target, self.debug_level,
                                    f"Remote runner job queue sent to {msg_data.get('requested by')}",
                                    DEBUG_INFO)
                        break


                    # Handles getting conductors attached to the remote Runner
                    elif msg_data.get("type") == "get_conductors" and self.role == "remote":
                        print_debug(self._print_target, self.debug_level,
                                    f"Getting conductors attached to: {self.name} - Requested by: {msg_data.get('requested by')}",
                                    DEBUG_INFO)
                        if self.conductors is None:
                            msg = f"No Conductors attached to {self.name}"
                            conn.sendall(msg.encode())
                            self.last_network_communication = time.time()
                        else:
                            conductor_names = [f"({conductor.__class__.__name__}): {conductor.name}" for conductor in self.conductors]
                            msg = f"Attached Conductors to {self.name}: {', '.join(conductor_names)}"
                            conn.sendall(msg.encode())
                            self.last_network_communication = time.time()
                            print_debug(self._print_target, self.debug_level,
                                        f"Conductors attached to {self.name} sent to {msg_data.get('requested by')}",
                                        DEBUG_INFO)
                        break


                    # Handles getting handlers attached to the remote Runner
                    elif msg_data.get("type") == "get_handlers" and self.role == "remote":
                        print_debug(self._print_target, self.debug_level,
                                    f"Getting handlers attached to:{self.name} - Requested by: {msg_data.get('requested by')}",
                                    DEBUG_INFO)
                        if self.handlers is None:
                            msg = f"No Handlers attached to {self.name}"
                            conn.sendall(msg.encode())
                            self.last_network_communication = time.time()
                        else:
                            handler_names = [f"({handler.__class__.__name__}): {handler.name}" for handler in self.handlers]
                            msg = f"Attached Handlers to {self.name}: {', '.join(handler_names)}"
                            conn.sendall(msg.encode())
                            self.last_network_communication = time.time()
                            print_debug(self._print_target, self.debug_level,
                                        f"Handlers attached to {self.name} sent to {msg_data.get('requested by')}",
                                        DEBUG_INFO)
                        break
                            

                    # Handles getting monitors attached to the remote Runner
                    elif msg_data.get("type") == "get_monitors" and self.role == "remote":
                        print_debug(self._print_target, self.debug_level,
                                    f"Getting monitors attached to:{self.name} - Requested by: {msg_data.get('requested by')}",
                                    DEBUG_INFO)
                        if self.monitors is None:
                            msg = f"No Monitors attached to {self.name}"
                            conn.sendall(msg.encode())
                            self.last_network_communication = time.time()
                        else:
                            monitor_names = [f" ({monitor.__class__.__name__}): {monitor.name}" for monitor in self.monitors]
                            msg = f"Attached Monitors to {self.name}: {', '.join(monitor_names)}"
                            conn.sendall(msg.encode())
                            self.last_network_communication = time.time()
                            print_debug(self._print_target, self.debug_level,
                                        f"Monitors attached to {self.name} sent to {msg_data.get('requested by')}",
                                        DEBUG_INFO)
                        break


                    # Handeles adding a monitor to the remote Runner
                    elif msg_data.get("type") == "add_monitor" and self.role == "remote":
                        try:
                            # Unpack the serialized patterns and recipes
                            recv_patterns = pickle.loads(base64.b64decode(msg_data.get("patterns")))
                            recv_recipes = pickle.loads(base64.b64decode(msg_data.get("recipes")))
                            base_dir = msg_data.get("base_dir")
                            mon_name = msg_data.get("name")

                            monitor_to_add = WatchdogMonitor(
                                base_dir, 
                                recv_patterns, 
                                recv_recipes, 
                                name = mon_name
                            )
                            self._add_local_monitor(monitor_to_add)
                            conn.sendall(json.dumps({"Success":"Added monitor to specified Runner"}).encode())
                            self.last_network_communication = time.time()
                        except Exception as e:
                            print_debug(self._print_target, self.debug_level,
                                        f"Failed to add monitor: {e}", DEBUG_WARNING)
                            conn.sendall(json.dumps({"Failed":"Unable to add monitor"}).encode())
                            self.last_network_communication = time.time()
                        break


                    # Handles adding a pattern to an existing monitor on the remote Runner
                    elif msg_data.get("type") == "add_pattern" and self.role == "remote":
                        try:
                            monitor_name = msg_data.get("monitor")
                            recv_pattern = pickle.loads(base64.b64decode(msg_data.get("pattern")))
                            self._add_local_pattern(monitor_name, recv_pattern)
                            conn.sendall(json.dumps({"Success":"Added pattern to specified monitor"}).encode())
                            self.last_network_communication = time.time()
                        except Exception as e:
                            print_debug(self._print_target, self.debug_level,
                                        f"Failed to add pattern: {e}", DEBUG_WARNING)
                            conn.sendall(json.dumps({"Failed":"Unable to add pattern"}).encode())
                            self.last_network_communication = time.time()
                        break    

                    # Handles deleing a pattern attached to a monitor on the remote Runner
                    elif msg_data.get("type") == "delete_pattern" and self.role == "remote":
                        try:
                            monitor_name = msg_data.get("monitor")
                            recv_pattern = msg_data.get("pattern_name")
                            self._delete_local_pattern(monitor_name, recv_pattern)
                            conn.sendall(json.dumps({"Success":"Deleted pattern from specified monitor"}).encode())
                            self.last_network_communication = time.time()
                        except Exception as e:
                            print_debug(self._print_target, self.debug_level,
                                        f"Failed to delete pattern: {e}", DEBUG_WARNING)
                            conn.sendall(json.dumps({"Failed":"Unable to delete pattern"}).encode())
                            self.last_network_communication = time.time()
                        break 


                    # Handles getting all patterns attached to a monitor on the remote Runner
                    elif msg_data.get("type") == "get_patterns" and self.role == "remote":
                        monitor_name = msg_data["monitor"]
                        monitor = self.get_monitor_by_name(monitor_name)
                        if monitor is None:
                            responds = json.dumps([]).encode()
                        else:
                            names = list(monitor.get_patterns().keys())
                            responds = json.dumps(names).encode()
                        conn.sendall(responds)
                        self.last_network_communication = time.time()
                        break

                    # If the message is not a JSON message, just return it as is and do nothing
                    else:
                        conn.sendall(data)
                        self.last_network_communication = time.time()
                        break
                except Exception as e:
                    print_debug(self._print_target, self.debug_level,
                        f"Failed to process incoming message: {e}", DEBUG_WARNING)
                    break
            
        
    def _get_local_ip(self):
        """Function to get the local IP address."""
        local_ip = socket.gethostbyname(socket.gethostname())
        print_debug(self._print_target, self.debug_level,
            f"Machines local IP: {local_ip}", DEBUG_INFO)
        return local_ip
    

    def send_handshake_to_local(self):
        """Function to send a handshake message to the local Runner."""
        handshake_msg = {
            "type": "handshake",
            "role": self.role,
            "name": self.name,
            "ip": self.local_ip_addr,
            "port": self.rr_port
        }
        print_debug(self._print_target, self.debug_level,
            "Sending handshake", DEBUG_INFO)
        responds = self.send_and_recieve_json_msg(self.local_runner_ip, self.msg_port, handshake_msg)
        if responds is None:
            print_debug(self._print_target, self.debug_level,
                "Handshake failed - No response from Local", DEBUG_WARNING)
            return
        elif responds == "Handshake Acknowledged":
            print_debug(self._print_target, self.debug_level,
                "Handshake successful", DEBUG_INFO)
            self.last_network_communication = time.time()
        return


    def heartbeat_thread_dealer(self, hb_interval:int=30, hb_timeout:int=90):
        """Function in charge of starting the heartbeat threads, depending on
        the role of the Runner, it either start a heartbeat_listener or heartbeat_sender thread."""
        if self.network != 1:
            print_debug(self._print_target, self.debug_level,
                "Network mode not enabled - Hearbeat thread can't be started", DEBUG_WARNING)
            return

        if self.role == "local" and self.network == 1 and self._heartbeat_listener_worker == None:
            # Start Heartbeat Listener Thread
            self._heartbeat_listener_worker= threading.Thread(target=self._setup_heartbeat_listener, daemon=True)
            self._heartbeat_listener_worker.start()
            print_debug(self._print_target, self.debug_level,
                f"{self.role} Heartbeat: Listener started", DEBUG_INFO)
            
            # Start Heartbeat Timeout Thread
            self.hb_timeout_thead = threading.Thread(target=self.heartbeat_timeout_check, args=(hb_timeout,), daemon=True)
            self.hb_timeout_thead.start()
            print_debug(self._print_target, self.debug_level,
                f"{self.role} Heartbeat: Timeout thread started", DEBUG_INFO)


        if self.role == "remote" and self.network == 1 and self.hb_sender_thread_active == False and self._heartbeat_sender_worker == None and self.hb_timeout_thead == None:
            # Start Heartbeat Timeout Thread
            self.hb_timeout_thead = threading.Thread(target=self.heartbeat_timeout_check, args=(hb_timeout,), daemon=True)
            self.hb_timeout_thead.start()
            print_debug(self._print_target, self.debug_level,
                f"{self.role} Heartbeat: Timeout thread started", DEBUG_INFO)
            
            # Start Heartbeat Sender Threads
            self.hb_sender_thread_active = True
            self._heartbeat_sender_worker= threading.Thread(target=self.send_heartbeat, args=(hb_interval,), daemon=True)
            self._heartbeat_sender_worker.start()
            print_debug(self._print_target, self.debug_level,
                f"{self.role} Heartbeat: Sender started", DEBUG_INFO)
   

    def send_heartbeat(self, hb_interval:int):
        """Function to send heartbeats to a local Runner, interval between heartbeast determined by 
        interval argument."""
        reconect_attempts = 0
        while self.network == 1:
            # Wait for the local Runner IP to be set, it this thread starts up too fast
            if not self.local_runner_ip and reconect_attempts < 5:
                print_debug(self._print_target, self.debug_level,
                            "Local Runner IP not set yet - Waiting for it to be set by other threads", DEBUG_WARNING)
                time.sleep(hb_interval)
                reconect_attempts += 1
                continue
            # If the local runner IP is not set after 5 attempts, shut down Remote
            elif reconect_attempts >= 5:
                print_debug(self._print_target, self.debug_level,
                            "Local Runner IP still not set within timeout window - Shutting Down...", DEBUG_WARNING)
                self.stop()
                return
            

            elaped_time = time.time() - self.last_network_communication
            if elaped_time < hb_interval:
                time.sleep(hb_interval)
                continue
            else:
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as hb_socket:
                        hb_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                        hb_socket.settimeout(5)
                        hb_socket.connect((self.local_runner_ip, self.hb_port))
                        heartbeat_msg = {
                            "type": "heartbeat",
                            "role": self.role,
                            "name": self.name,
                            "timestamp": time.time()
                        }
                        hb_socket.sendall(json.dumps(heartbeat_msg).encode())
                        self.last_network_communication = time.time() 
                        data = hb_socket.recv(1024) # Wait for an ack back from local
                        if not data:
                            print_debug(self._print_target, self.debug_level,
                                        "Heartbeat connection failed or no data recieved", DEBUG_WARNING)
                            hb_socket.close()
                            break
                        msg = json.loads(data.decode())
                        if msg.get("type") == "hb_ack":
                            print_debug(self._print_target, self.debug_level,
                                        f"Heartbeat ack from {msg.get('name')}", DEBUG_INFO)
                            self.last_heartbeat_from_local = msg.get("timestamp", time.time())
                            hb_socket.close()
                        else:
                            print_debug(self._print_target, self.debug_level,
                                        "Message recieved was not Heartbeat ack", DEBUG_WARNING)
                            hb_socket.close()
                        time.sleep(hb_interval)
                except Exception as e:
                    print_debug(self._print_target, self.debug_level,
                                f"Error in sending heartbeat: {e}", DEBUG_WARNING)
                    return
                    

    def heartbeat_timeout_check(self, hb_timeout:int, hb_check_interval:int=5):
        """Function to periodically check if 'hb_timeout' seconds have passed since
            last heartbeat recieved from the local Runner. If yes, assume local dead
            and shutdown this Runner."""
        while self.network == 1:

            # Remote side 
            if self.role == "remote":
                time_elapsed = time.time() - self.last_heartbeat_from_local  # Check time since last heartbeat from Local
                if time_elapsed > hb_timeout:
                    print_debug(self._print_target, self.debug_level,
                                f"Heartbeat timeout: {time_elapsed} seconds since last heartbeat from {self.local_runner_name}; Assuming Local DEAD - Shutting Down...", DEBUG_WARNING)
                    self.network = 0
                    self.stop()
                    return
                time.sleep(hb_check_interval)

            # Local side
            elif self.role == "local":
                current_time = time.time() # Check time since last heartbeat from remote Runners
                for rname in list(self.remote_runners.keys()):
                    info = self.remote_runners[rname]
                    time_elapsed = current_time - info["last_hb"]
                    if time_elapsed > hb_timeout:
                        print_debug(self._print_target, self.debug_level,
                                    f"{rname} timed out ({time_elapsed:.2f}s).  Attempting single restart…", DEBUG_WARNING)
                        if info["restart_attempted"] == False:
                            try:
                                info["restart_attempted"] = True
                                self.attempt_remote_restart(rname)
                                info["last_hb"] = time.time()
                            except Exception as e:
                                print_debug(self._print_target, self.debug_level,
                                            f"Failed to restart remote runner - Deleting from active remote runners: {e}", DEBUG_WARNING)
                                del self.remote_runners[rname]
                                continue
                        elif info["restart_attempted"] == True:
                            print_debug(self._print_target, self.debug_level,
                                        f"Restart of {rname} already attempted - assuming dead, deleting for active remote runners", DEBUG_WARNING)
                            del self.remote_runners[rname]
                            continue
                time.sleep(hb_check_interval)


    def generate_network_json_config(self):
        """Function to generate a JSON config file for network mode,
        containing the local IP address, name of the Runner and port Local is listening on."""
        if sys.version_info[:3] == (3, 12, 3):  # Used to tell our two systems apart
            config_dir = "/workspaces/meow_base/meow_base/.netconfs"
        else:
            user = os.environ.get("USER") or os.environ.get("USERNAME")  # Allows for all standard Ubuntu setups to run the code
            config_dir = f"/home/{user}/meow_base/meow_base/.netconfs"
        if not os.path.exists(config_dir):
            os.makedirs(config_dir)

        config = {
            "name": self.name,
            "ip": "192.168.94.138",
            "msg_port": self.msg_port
        }
        conf_file = os.path.join(config_dir, "network_config.json")
        with open(conf_file, "w") as f:
            json.dump(config, f)
        print_debug(self._print_target, self.debug_level,
            f"Network config file generated at: {conf_file}", DEBUG_INFO)
        return conf_file


    def transfer_network_config(self, client: paramiko.SSHClient, local_config_file: str):
        """Function to transfer the network config file generated by a local Runner to the remote system."""
        try:
            sftp = client.open_sftp()
            if sys.version_info[:3] == (3, 12, 3):  # Used to tell our two systems apart
                remote_dir = "/workspaces/meow_base/meow_base/.netconfs"
            else:
                user = os.environ.get("USER") or os.environ.get("USERNAME")
                remote_dir = f"/home/{user}/meow_base/meow_base/.netconfs"

            # Check if the requiered directory exists - if not create it
            try:
                sftp.stat(remote_dir)
            except IOError:
                sftp.mkdir(remote_dir)
                print_debug(self._print_target, self.debug_level,
                        f"Directory for network configs not found -  {remote_dir} created", DEBUG_INFO)
            
            remote_config_path = f"{remote_dir}/transfered_network_config.json"
            print(f"Local File path: {local_config_file}")
            print(f"Remote File path: {remote_config_path}")
            sftp.put(local_config_file, remote_config_path)
            print_debug(self._print_target, self.debug_level,
                    f"Transferred network config file to remote: {remote_config_path}", DEBUG_INFO)
            sftp.close()
        except Exception as e:
            print_debug(self._print_target, self.debug_level,
                    f"Failed to send network config file: {e}", DEBUG_WARNING)
        return


    def load_runner_filepath(self, runner_manifest_filepath:str):
        """Function to load the Runner file path from the manifest file"""
        if not os.path.exists(runner_manifest_filepath):
            raise FileNotFoundError(f"Could not find manifest file at {runner_manifest_filepath}")

        with open(runner_manifest_filepath, "r") as f:
            data = json.load(f)

        for available_runners in data.get("available_runners", []):
            if available_runners.get("filename") == self.runner_file_name:
                self.runner_file_path = available_runners.get("fullpath")
                return
        else:
            raise ValueError(f"Runner {self.runner_file_name} not found in JSON file")


    def transfer_local_located_runner_to_remote(self, client: paramiko.SSHClient, runner_manifest_filepath:Any=os.path.expanduser("/workspaces/meow_base/examples/runners/.runner_confs.json")):
        """Function to transfer a specified Runner to the remote system."""
        if sys.version_info[:3] == (3, 12, 3):
            runner_manifest_filepath = "/workspaces/meow_base/examples/runners/.runner_confs.json"
        else:
            user = os.environ.get("USER") or os.environ.get("USERNAME")
            runner_manifest_filepath = f"/home/{user}/meow_base/examples/runners/.runner_confs.json"
        self.load_runner_filepath(runner_manifest_filepath)
        try:
            sftp = client.open_sftp()

            # Check if the directory exists, if not error out
            try:
                sftp.stat(runner_manifest_filepath)
            except IOError:
                print_debug(self._print_target, self.debug_level,
                        f"Directory {runner_manifest_filepath} does not exist, please confirm it exists and attempt again", DEBUG_INFO)
                return

            runner_on_local = f"{self.runner_file_path}/{self.runner_file_name}"
            runner_on_remote = f"{self.runner_file_path}/{self.runner_file_name}"
            sftp.put(runner_on_local, runner_on_remote)
            print_debug(self._print_target, self.debug_level,
                    f"Transferred runner to remote: {self.runner_file_path}", DEBUG_INFO)
            sftp.close()
        except Exception as e:
            print_debug(self._print_target, self.debug_level,
                    f"Failed to send runner file: {e}", DEBUG_WARNING)
        return


    def attempt_remote_restart(self, runner_name:str):
        """Function to attempt a restart of a remote Runner."""
        if self.role != "local":
            print_debug(self._print_target, self.debug_level,
                "Restart command can only be sent from local runner", DEBUG_WARNING)
            return
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        host_name = self.remote_runners[runner_name]["ssh_hostname"]
        user = self.remote_runners[runner_name]["ssh_user"]
        port = self.remote_runners[runner_name]["ssh_port"]
        del self.remote_runners[runner_name]
        client.connect(hostname = host_name, username = user, port = port, key_filename = self.ssh_private_key_dir)
        print_debug(self._print_target, self.debug_level,
            f"SSH connection established agin to {runner_name}", DEBUG_INFO)
        
        if self.runner_file_name == None:
            if sys.version_info[:3] == (3, 12, 3):
                meow_base_path = "/workspaces/meow_base/examples/"
            else:
                user = os.environ.get("USER") or os.environ.get("USERNAME")
                meow_base_path = f"/home/{user}/meow_base/examples/"
            requested_runner = "skeleton_runner.py"
        else:
            meow_base_path = self.runner_file_path
            print_debug(self._print_target, self.debug_level,
                f"Requested runner file path: {meow_base_path}", DEBUG_INFO)
            requested_runner = self.runner_file_name
            print_debug(self._print_target, self.debug_level,
                f"Requested runner file name: {requested_runner}", DEBUG_INFO)

        if sys.version_info[:3] == (3, 12, 3):
            client.exec_command(f'cd {meow_base_path} && source /app/venv/bin/activate && nohup python3 {requested_runner} &')
        else:
            client.exec_command(f'cd {meow_base_path} && nohup python3 {requested_runner} &')
        client.close()


    def setup_ssh_connection_to_remote(self):
        """Function to setup an SSH connection to a remote system, and initialize a remote Runner."""
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        conf_host_name, conf_user, conf_port = self.deconstruct_ssh_alias()
        if conf_host_name == None or conf_user == None or conf_port == None:
            print_debug(self._print_target, self.debug_level,
                "SSH config alias not found - unable to connect to remote system", DEBUG_WARNING)
            return
        client.connect(hostname = conf_host_name, username = conf_user, port = conf_port, key_filename = self.ssh_private_key_dir)
        print_debug(self._print_target, self.debug_level,
            "SSH connection established", DEBUG_INFO)
    
        # Transfer the network config file to the remote machine
        try:
            local_config_to_send = self.generate_network_json_config()
            print(f"Local config file path: {local_config_to_send}")
            self.transfer_network_config(client, local_config_to_send)
        except Exception as e:
            print_debug(self._print_target, self.debug_level,
                f"Failed to transfer network config file: {e}", DEBUG_WARNING)
            return

        # Check if there exsits a Runner file the user wants to transfer to the remote system
        if not self.runner_file_name == None:
            try:
                self.transfer_local_located_runner_to_remote(client)
            except Exception as e:
                print_debug(self._print_target, self.debug_level,
                    f"Failed to transfer runner file: {e}", DEBUG_WARNING)
                return

        if self.runner_file_name == None:
            if sys.version_info[:3] == (3, 12, 3):
                meow_base_path = "/workspaces/meow_base/examples/"
            else:
                user = os.environ.get("USER") or os.environ.get("USERNAME")
                meow_base_path = f"/home/{user}/meow_base/examples/"
            requested_runner = "skeleton_runner.py"
        else:
            meow_base_path = self.runner_file_path
            print_debug(self._print_target, self.debug_level,
                f"Requested runner file path: {meow_base_path}", DEBUG_INFO)
            requested_runner = self.runner_file_name
            print_debug(self._print_target, self.debug_level,
                f"Requested runner file name: {requested_runner}", DEBUG_INFO)
        
        if sys.version_info[:3] == (3, 12, 3):
            cmd = f'cd {meow_base_path} && source /app/venv/bin/activate && nohup python3 {requested_runner} &'
        else:
            cmd = f'cd {meow_base_path} && nohup python3 {requested_runner} &'
        for i in range(self.runners_to_start):
            client.exec_command(cmd)
        client.close()
        return


    def load_transfered_network_config(self):
        """Function to look for the JSON config file in the .netconfs folder, 
        parse the file, and store the local Runner's Name, IP and Port."""
        if sys.version_info[:3] == (3, 12, 3):
            config_dir = "/workspaces/meow_base/meow_base/.netconfs"
        else:
            user = os.environ.get("USER") or os.environ.get("USERNAME")
            config_dir = f"/home/{user}/meow_base/meow_base/.netconfs"
        
        config_file = os.path.join(config_dir, "transfered_network_config.json")
        if os.path.exists(config_file):
            try:
                with open(config_file, "r") as f:
                    config = json.load(f)
                self.local_runner_name = config.get("name")
                self.local_runner_ip = config.get("ip")
                self.msg_port = config.get("msg_port")
                print_debug(self._print_target, self.debug_level,
                            f"Loaded local runner config: Name = {self.local_runner_name}, IP = {self.local_runner_ip}",
                            DEBUG_INFO)
            except Exception as e:
                print_debug(self._print_target, self.debug_level,
                            f"Error reading local runner config: {e}", DEBUG_WARNING)
                return
        else:
            print_debug(self._print_target, self.debug_level,
                        f"Config file not found at {config_file}", DEBUG_WARNING)
            return
 

    def send_and_read_message(self, r_ip, r_port, message):
        """Function to send a message over the socket connection when not using JSON."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(5)
                sock.connect((r_ip, r_port))
                message = f"{self.name}: {message}\n"
                sock.sendall(message.encode())
                if self.role == "remote":
                    self.last_network_communication = time.time()
                data = sock.recv(1024)
                if not data:
                    print_debug(self._print_target, self.debug_level,
                        "No data received from Runner", DEBUG_WARNING)
                    return
                print(f"Received: {data.decode()}")
                return
        except Exception as e:
            print_debug(self._print_target, self.debug_level,
                f"Failed to send message: {e}", DEBUG_WARNING)

    
    def send_and_recieve_json_msg(self, ip_addr, r_port, msg):
        """Function to send a JSON message over the socket connection and receive a response - 
        If the connection fails, it will also attempt to reconnect a few times before giving up."""
        reconnect_attempts = 5
        for attempt in range(1, reconnect_attempts + 1):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.settimeout(5)
                    sock.connect((ip_addr, r_port))
                    sock.sendall(json.dumps(msg).encode())
                    if self.role == "remote":
                        self.last_network_communication = time.time()
                    sock.shutdown(socket.SHUT_WR)
                    data = sock.recv(1024)
                    return (data.decode())
            except OSError as e:
                if attempt < reconnect_attempts:
                    time.sleep(0.3)
                    continue
            except Exception as e:
                print_debug(self._print_target, self.debug_level,
                    f"Failed to send or recieve JSON message: {e}", DEBUG_WARNING)
                return
   

    def get_attached_conductors(self, target=None) -> None:
        """Function to get attached conductors to the remote or local Runner"""
        # If no target is specified, get conductors from both local and remote Runners
        if target == None:
            msg = {
                    "type": "get_conductors",
                    "requested by": self.name
                }
            remote_msg = ""
            for rname, rinfo in self.remote_runners.items():
                conductors = self.send_and_recieve_json_msg(rinfo["ip"], rinfo["port"], msg)
                remote_msg += f"{conductors}\n"
                continue
            local_conductor_names = [conductor.__class__.__name__ for conductor in self.conductors]
            print(f"Attached Conductors to {self.name}: {', '.join(local_conductor_names)}\n{remote_msg}")
            return 

        if target == "local":
            if not self.conductors:
                print(f"No Conductors attached to {self.name}")
                return 
            else:
                conductor_names = [conductor.__class__.__name__ for conductor in self.conductors]
                print(f"Attached Conductors to {self.name}: {', '.join(conductor_names)}")
                return conductor_names
    
        if target == "remote":
            if not self.conductors:
                for rname, rinfo in self.remote_runners.items():
                    remote_ip = rinfo["ip"]
                    remote_port = rinfo["port"]
                    msg = f"No Conductors attached to {self.name}"
                    self.send_and_read_message(remote_ip, remote_port, msg)
                    continue
                return
            else:
                msg = {
                    "type": "get_conductors",
                    "requested by": self.name
                }
                for rname, rinfo in self.remote_runners.items():
                    try:
                        conductors = self.send_and_recieve_json_msg(rinfo["ip"], rinfo["port"], msg)
                        print(f"[{rname}] : {conductors}")
                        continue
                    except Exception as e:
                        print_debug(self._print_target, self.debug_level,
                            f"Failed to get conductors from {rname}: {e}", DEBUG_WARNING)
                        return
            return 
        raise ValueError(f"Unknown target “{target}” for get_attached_conductors()")
            

    def get_attached_handlers(self, target=None) -> None:
        """Function to get attached handlers from the remote or local Runner"""
        if target == None:
            msg = {
                "type": "get_handlers",
                "requested by": self.name
            }
            remote_msg = ""
            for rname, rinfo in self.remote_runners.items():
                handlers = self.send_and_recieve_json_msg(rinfo["ip"], rinfo["port"], msg)
                remote_msg += f"{handlers}\n"
                continue
            local_handler_names = [handler.__class__.__name__ for handler in self.handlers]
            print(f"Attached Handlers to {self.name}: {', '.join(local_handler_names)}\n{remote_msg}")
            return 

        if target == "local":
            if not self.handlers:
                print(f"No Handlers attached to {self.name}")
                return
            else:
                handler_names = [handler.__class__.__name__ for handler in self.handlers]
                print(f"Attached Handlers to {self.name}: {', '.join(handler_names)}")
                return handler_names
    
        if target == "remote":
            if not self.handlers:
                for rname, rinfo in self.remote_runners.items():
                    remote_ip = rinfo["ip"]
                    remote_port = rinfo["port"]
                    msg = f"No Handlers attached to {self.name}"
                    self.send_and_read_message(remote_ip, remote_port, msg)
                    continue
                return
            else:
                msg = {
                    "type": "get_handlers",
                    "requested by": self.name
                }
                for rname, rinfo in self.remote_runners.items():
                    try:
                        handlers = self.send_and_recieve_json_msg(rinfo["ip"], rinfo["port"], msg)
                        print(f"[{rname}] : {handlers}")
                        continue
                    except Exception as e:
                        print_debug(self._print_target, self.debug_level,
                            f"Failed to get handlers from {rname}: {e}", DEBUG_WARNING)
        

    def get_attached_monitors(self, target=None) -> None:
        """Function to get attached monitors from the remote or local Runners"""
        if target == None:
            msg = {
                "type": "get_monitors",
                "requested by": self.name
            }
            remote_msg = ""
            for rname, rinfo in self.remote_runners.items():
                monitors = self.send_and_recieve_json_msg(rinfo["ip"], rinfo["port"], msg)
                remote_msg += f"{monitors}\n"
                continue
            local_monitor_names = [f" ({monitor.__class__.__name__}): {monitor.name}" for monitor in self.monitors]
            print(f"Attached Monitors to {self.name}: {', '.join(local_monitor_names)}\n{remote_msg}")
            return

        if target == "local":
            if not self.monitors:
                print(f"No Monitors attached to {self.name}")
                return
            else:
                monitor_names = [f" ({monitor.__class__.__name__}): {monitor.name}" for monitor in self.monitors]
                print(f"Attached Monitors to {self.name}: {', '.join(monitor_names)}")
                return monitor_names
    
        if target == "remote":
            if not self.monitors:
                msg = f"No Monitors attached to {self.name}"
                self.send_and_read_message(self.remote_runner_ip,msg)
                return
            else:
                msg = {
                    "type": "get_monitors",
                    "requested by": self.name
                }
                for rname, rinfo in self.remote_runners.items():
                    try:
                        monitors = self.send_and_recieve_json_msg(rinfo["ip"], rinfo["port"], msg)
                        print(f"[{rname}] : {monitors}")
                        continue
                    except Exception as e:
                        print_debug(self._print_target, self.debug_level,
                            f"Failed to get monitors from {rname}: {e}", DEBUG_WARNING)
                        return

    
    def get_queue(self, target=None) -> None:
        """Function to retrieve the job queues from the local and/or remote systems based on the input of the fuction"""
        if target == None:
            msg = {
                "type": "get_queue",
                "requested by": self.name,
                "timestamp": time.time()
            }
            for rname, rinfo in self.remote_runners.items():
                try:
                    print_debug(self._print_target, self.debug_level,
                        f"Getting job queue from {rname}", DEBUG_INFO)
                    remote_q = self.send_and_recieve_json_msg(rinfo["ip"], rinfo["port"], msg)
                    print_debug(self._print_target, self.debug_level,
                        f"[{rname}] Job Queue: {remote_q}", DEBUG_INFO)
                    local_q = self.job_queue
                    print(f"{self.name} job queue: {local_q}\n{rname} job queue: {remote_q}")
                    continue
                except Exception as e:
                    print_debug(self._print_target, self.debug_level,
                        f"Failed to get Job Queue from {rname}: {e}", DEBUG_WARNING)
                    return
            return
        
        if target == "local":
            job_q = self.job_queue
            print(f"Local job queue: {job_q}")
            return job_q
            
        if target == "remote":
            for rname, rinfo in self.remote_runners.items():
                try:
                    msg = {
                        "type": "get_queue",
                        "requested by": self.name,
                        "timestamp": time.time()
                    }
                    remote_q = self.send_and_recieve_json_msg(rinfo["ip"], rinfo["port"], msg)
                    print(f"[{rname}] Job Queue: {remote_q}")
                    continue
                except Exception as e:
                    print_debug(self._print_target, self.debug_level,
                        f"Failed to get Job Queue from {rname}: {e}", DEBUG_WARNING)
                    return
        
    
    def add_monitor(self, monitor:BaseMonitor, target:str="local") -> None:
        """Function to add a monitor to the local or remote Runner"""
        # Check if the monitor provided is of type BaseMonitor
        check_type(monitor, BaseMonitor, hint="MeowRunner.add_monitor")
        if target == "local":
            return self._add_local_monitor(monitor)
        if target == "remote":
            patterns = monitor.get_patterns()
            recipes = monitor.get_recipes()

            #Pickle the objects and convert to base64 in order to send over JSON
            patterns_base64 = base64.b64encode(pickle.dumps(patterns)).decode()
            recipes_base64 = base64.b64encode(pickle.dumps(recipes)).decode()

            base_dir = getattr(monitor, "base_dir", None)
            if base_dir is None:
                raise ValueError("Input directory not declared for monitor")
            msg = {
                "type": "add_monitor",
                "requested by": self.name,
                "name": monitor.name,
                "base_dir": base_dir,
                "patterns": patterns_base64,
                "recipes": recipes_base64
                }
            for rname, rinfo in list(self.remote_runners.items()):
                try:
                    print_debug(self._print_target, self.debug_level,
                        f"Adding monitor to {rname}", DEBUG_INFO)
                    responds = self.send_and_recieve_json_msg(rinfo["ip"], rinfo["port"], msg)
                    print_debug(self._print_target, self.debug_level,
                        f"[{rname}] Response: {responds}", DEBUG_INFO)
                    continue
                except Exception as e:
                    print_debug(self._print_target, self.debug_level,
                        f"Failed to add monitor to {rname}: {e}", DEBUG_WARNING)
                    return
            return
        raise ValueError(f"Unknown target “{target}” for add_monitor()")

    
    def add_pattern(self, monitor_name:str, pattern:BasePattern, target:str="local") -> None:
        """Function to add a pattern to the local or remote monitor"""
        # Check if the pattern provided is of type BasePattern
        check_type(pattern, BasePattern, hint="MeowRunner.add_pattern")
        if target == "local":
            return self._add_local_pattern(monitor_name, pattern)
        if target == "remote":
            pattern_base64 = base64.b64encode(pickle.dumps(pattern)).decode()
            msg = {
                "type": "add_pattern",
                "requested by": self.name,
                "monitor": monitor_name,
                "pattern": pattern_base64
            }
            for rname, rinfo in list(self.remote_runners.items()):
                try:
                    print_debug(self._print_target, self.debug_level,
                        f"Adding pattern to {rname}", DEBUG_INFO)
                    responds = self.send_and_recieve_json_msg(rinfo["ip"], rinfo["port"], msg)
                    print_debug(self._print_target, self.debug_level,
                        f"[{rname}] Response: {responds}", DEBUG_INFO)
                    continue
                except Exception as e:
                    print_debug(self._print_target, self.debug_level,
                        f"Failed to add pattern to {rname}: {e}", DEBUG_WARNING)
                    return
            return
        raise ValueError(f"Unknown target “{target}” for add_pattern()")
    
    def delete_pattern(self, monitor_name:str, pattern:str, target:str="local") -> None:
        """Function to delete a pattern from the local or remote monitor"""
        if target == "local":
            return self._delete_local_pattern(monitor_name, pattern)
        if target == "remote":
            msg = {
                "type": "delete_pattern",
                "requested by": self.name,
                "monitor": monitor_name,
                "pattern_name": pattern
            }
            for rname, rinfo in list(self.remote_runners.items()):
                try:
                    print_debug(self._print_target, self.debug_level,
                        f"Deleting pattern from {monitor_name} attached to {rname}", DEBUG_INFO)
                    responds = self.send_and_recieve_json_msg(rinfo["ip"], rinfo["port"], msg)
                    print_debug(self._print_target, self.debug_level,
                        f"[{rname}] Response: {responds}", DEBUG_INFO)
                    continue
                except Exception as e:
                    print_debug(self._print_target, self.debug_level,
                        f"Failed to delete pattern from {rname}: {e}", DEBUG_WARNING)
                    return
            return
        raise ValueError(f"Unknown target “{target}” for delete_pattern()")
    

    def get_attached_patterns(self, monitor:str=None, target:str="local") -> None:
        """Function to get the attached patterns to a local or remote monitor"""
        if monitor == None:
            if len(self.monitors) != 1:
                raise ValueError("Must specify the monitors given name when more than one exists")
            target_monitor = self.monitors[0].name

        if target == "local":
            target_monitor = self.get_monitor_by_name(monitor)
            if target_monitor == None:
                raise ValueError(f"No monitor pressent called: {monitor}")
            patterns = list(target_monitor.get_patterns().keys())
            print(f"Attached Patterns to {target_monitor.name}: {', '.join(patterns)}")
            return patterns

        if target == "remote":
            msg = {
                "type": "get_patterns",
                "requested by": self.name,
                "monitor": monitor
            }
            for rname, rinfo in self.remote_runners.items():
                try:
                    print_debug(self._print_target, self.debug_level,
                        f"Getting patterns from {rname}", DEBUG_INFO)
                    responds = self.send_and_recieve_json_msg(rinfo["ip"], rinfo["port"], msg)
                    try:
                        patterns = json.loads(responds)
                        print(f"Attached Patterns to {monitor}: {', '.join(patterns)}")
                        continue
                    except Exception as e:
                        print_debug(self._print_target, self.debug_level,
                            f"Failed to decode response from {rname}: {e}", DEBUG_WARNING)
                        continue
                except Exception as e:
                    print_debug(self._print_target, self.debug_level,
                        f"Failed to get patterns from {rname}: {e}", DEBUG_WARNING)
                    return
            return
        raise ValueError(f"Unknown target “{target}” for get_attached_patterns()")