
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
import re
import ast
import time
import json

from multiprocessing import Pipe
from typing import Any, Union, Dict, List, Type, Tuple

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
            name:str="Unnamed Runner", role:str="local",

            # Added Network Options
            network:int=0, ssh_config_alias:Any=None, ssh_private_key_dir:Any=os.path.expanduser("~/.ssh/id_ed25519"), debug_port:int=10001, hb_port:int=10005 )->None:
        """MeowRunner constructor. This connects all provided monitors, 
        handlers and conductors according to what events and jobs they produce 
        or consume."""


        self.name = name
        self.network = network
        self.ssh_config_alias = ssh_config_alias

        # Another implementation to determine if Runner is Remote or Local (Does not force Runner to be called Remote to detect)
        self.role = role
       
        self.ssh_private_key_dir = ssh_private_key_dir


        # Debugging port for network mode
        self.debug_port = debug_port


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

        # Setup debugging
        self._print_target, self.debug_level = setup_debugging(print, logging)

        # Setup queues
        self.event_queue = []
        self.job_queue = []

        # Get the local IP address, so each runner instantly knows its own IP
        self.local_ip_addr = self._get_local_ip()

        # Used for Heartbeat and Handshake, but might delete, given better method probably exists
        self.remote_alive = False
        self.remote_runner_name = None



        # Contained variables that only the Remote Runner should set
        self.local_runner_name = None
        self.local_runner_ip = None


        # Contained variables that only the Local Runner should set
        self.remote_runner_name = None
        self.remote_runner_ip = None

        # Used for logging time of last heartbeat recived from Remote Runner, along with standard hb_port (Standard 10005)
        self.last_heartbeat_from_remote = time.time()
        self.last_heartbeat_from_local = time.time()
        self.hb_port = hb_port
        self.hb_sender_thread_active = False


        """ if network == 1:
            print_debug(self._print_target, self.debug_level,
                "Network mode enabled", DEBUG_INFO)
            print_debug(self._print_target, self.debug_level,
                f"Config Alias Name: {self.ssh_config_alias}", DEBUG_INFO)
            print_debug(self._print_target, self.debug_level,
                f"Port: {self.debug_port}", DEBUG_INFO)
            print_debug(self._print_target, self.debug_level,
                f"SSH Key Directory: {self.ssh_private_key_dir}\n", DEBUG_INFO) """



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


    # Used to listening for msgs from Remote Runner.
    def setup_listener_thread(self) -> None:
        """Function to setup a listener thread to listen for messages from 
        the remote machine."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.local_ip_addr, self.debug_port))
            s.listen(20)
            print_debug(self._print_target, self.debug_level,
                "Listener thread listening...", DEBUG_INFO)
            
            all_inputs = [s, self._stop_listener_pipe[0]]
            #print(f"DEBUG: All inputs: {all_inputs}")
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
                    # print_debug(self._print_target, self.debug_level,
                    #     "Listener thread started", DEBUG_INFO)
                    # print_debug(self._print_target, self.debug_level,
                    #     f"Listening on port {self.port}", DEBUG_INFO)


    # Used for enabling the Remote Runner to listen for incoming messages from the Local Runner
    def setup_remote_listener_thread(self) -> None:
        """Function similar to setup_listener_thread, customized for the remote runner."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.local_ip_addr, 10002)) # When testing on own machine must use two different ports, but Idea is to only use one.
            s.listen()
            print_debug(self._print_target, self.debug_level,
                "Remote listener thread listening...", DEBUG_INFO)
            
            all_inputs = [s, self._stop_remote_listener_pipe[0]]
            while True:
                ready, _, _ = select.select(all_inputs, [], [])
                if self._stop_remote_listener_pipe[0] in ready:
                    print_debug(self._print_target, self.debug_level,
                        "Remote listener thread stopped", DEBUG_INFO)
                    return
                if s in ready:
                    conn, addr = s.accept()
                    print_debug(self._print_target, self.debug_level,
                        f"Remote accepted connection from {addr}", DEBUG_INFO)
                    listener_thread = threading.Thread(
                        target=self.handle_listener_thread,
                        args=(conn, addr))
                    listener_thread.daemon = True
                    listener_thread.start()
                    print_debug(self._print_target, self.debug_level,
                        "Remote listener thread started", DEBUG_INFO)


    # Seperate listening thread to monitor heartbeats sent to hb_port(as standard, port is set to 10005)
    def setup_heartbeat_listener(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.local_ip_addr, self.hb_port))
            s.listen()
            print_debug(self._print_target, self.debug_level,
                        "Persistent Heartbeat listener thread listening...", DEBUG_INFO)

            while self.network == 1:
                conn, addr = s.accept()
                print_debug(self._print_target, self.debug_level,
                            f"CONSISTANT Heartbeat connection accepted from {addr}", DEBUG_INFO)
                with conn:
                    while self.network == 1:
                        try:
                            data = conn.recv(1024)
                            if not data:
                                print_debug(self._print_target, self.debug_level,
                                            "CONSISTANT heartbeat connection closed by remote", DEBUG_WARNING)
                                break
                            heartbeat = json.loads(data.decode())
                            self.last_heartbeat_from_remote = heartbeat.get("timestamp", time.time())
                            print_debug(self._print_target, self.debug_level,
                                        f"Received heartbeat from {heartbeat.get('name')}", DEBUG_INFO)
                            # When heartbeat is recieved, prepare and send an ack back, so remote knows local is recieving heartbeats.
                            ack_response = {
                                "type": "hb_ack",
                                "role": self.role,
                                "name": self.name,
                                "remote_timestamp": self.last_heartbeat_from_remote,
                                "timestamp": time.time()
                            }
                            conn.sendall(json.dumps(ack_response).encode())
                        except Exception as e:
                            print_debug(self._print_target, self.debug_level,
                                        f"Error in CONSISTANT heartbeat listener: {e}", DEBUG_WARNING)
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

        # if self.network == 1:

        #     # Send attached conductors, handlers & monitors to the remote machine
        #     self.send_attached_conductors()
        #     self.send_attached_handlers()

        #     # Send a message to the remote machine
        #     print_debug(self._print_target, self.debug_level,
        #         "Message sent to remote machine", DEBUG_INFO)
        


        if self.network == 1 and self.role == "local":
            print_debug(self._print_target, self.debug_level,
                "Setting up network connection...", DEBUG_INFO)
            if self._network_listener_worker is None:
                # Setting up listening thread
                self._network_listener_worker = threading.Thread(
                    target=self.setup_listener_thread,
                    args=[])
                self._network_listener_worker.daemon = True
                self._network_listener_worker.start()
                print_debug(self._print_target, self.debug_level, 
                    "Starting MeowRunner network listener...", DEBUG_INFO)
            else:
                msg = "Repeated calls to start MeowRunner network listener " \
                    "have no effect."
                print_debug(self._print_target, self.debug_level, 
                    msg, DEBUG_WARNING)
                raise RuntimeWarning(msg)

            self.heartbeat_thread_dealer()
            self.setup_ssh_connection_to_remote()

        

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

            self.load_transfered_network_config()
            self.send_handshake_to_local()
            self.heartbeat_thread_dealer(hb_interval=5, hb_timeout=15)


    def stop(self)->None:
        """Function to stop the runner by stopping all of the constituent 
        monitors, handlers and conductors, along with managing interaction 
        threads."""

        if self.role == "local" and self.network == 1 and self.remote_runner_ip:
            self._send_stop_cmd_to_remote()
            while self.remote_alive:
                print_debug(self._print_target, self.debug_level,
                    "Waiting for remote runner to stop...", DEBUG_INFO)
                time.sleep(1)
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
        

        if self.role == "local":
            pass


        if self.role == "remote" and self.network == 1 and self.local_runner_ip:
            self._confirm_remote_runner_shutdown()
            self.network = 0
            

            

        
        # Closes network threads.
        # Maybe not needed as threads close their connections on their own.
        if self._network_listener_worker is None and self.role == "local":
            msg = "Cannot stop remote network listener thread that is not started."
            print_debug(self._print_target, self.debug_level,
                msg, DEBUG_WARNING)
            raise RuntimeWarning(msg)
        else:
            if self.role == "local":
                self._stop_listener_pipe[1].send(1)
                self._network_listener_worker.join()
                print_debug(self._print_target, self.debug_level,
                    "Local Network listener thread stopped", DEBUG_INFO)
        
        
        if self._remote_network_listener_worker is None and self.role == "remote":
            msg = "Cannot stop network listener thread that is not started."
            print_debug(self._print_target, self.debug_level,
                msg, DEBUG_WARNING)
            raise RuntimeWarning(msg)
        else:
            if self.role == "remote":
                self._stop_remote_listener_pipe[1].send(1)
                print(f"MEEEEEE ISSSSS {self.role}")
                self._remote_network_listener_worker.join()
                print_debug(self._print_target, self.debug_level,
                    "Remote Network listener thread stopped", DEBUG_INFO)
        
        if self.role == "local":
            active_threads = threading.enumerate()
            print_debug(self._print_target, self.debug_level, f"Active threads: {[t.name for t in active_threads]}", DEBUG_INFO)

        if self.role == "remote":
            active_threads = threading.enumerate()
            print_debug(self._print_target, self.debug_level, f"Active threads: {[t.name for t in active_threads]}", DEBUG_INFO)

        
    # Used to instruct remote runner to also call .stop() if local runner called .stop()
    def _send_stop_cmd_to_remote(self) -> None:
        """Function to send a stop command to the remote machine."""

        if self.role != "local":
            print_debug(self._print_target, self.debug_level,
                "Stop command can only be sent from local runner", DEBUG_WARNING)
            return
        
        stop_msg = {
            "type": "runner_shutdown",
            "requested by": self.name,
            "reason": "Local runner called .stop()"
        }
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.remote_runner_ip, 10002))
                s.sendall(json.dumps(stop_msg).encode())
                print(f"DEBUG: Sent stop message to remote runner: {stop_msg}")
                print_debug(self._print_target, self.debug_level,
                    "Stop message sent to remote machine", DEBUG_INFO)
        except Exception as e:
            print_debug(self._print_target, self.debug_level,
                f"Failed to send stop command to remote: {e}", DEBUG_WARNING)



    def _confirm_remote_runner_shutdown(self) -> None:
        """Function to confirm the shutdown of the remote runner."""
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
                s.connect((self.local_runner_ip, 10001))
                s.sendall(json.dumps(confirm_shtdwn_msg).encode())
                print_debug(self._print_target, self.debug_level,
                    "Ack of shutdown sent to local machine", DEBUG_INFO)
        except Exception as e:
            print_debug(self._print_target, self.debug_level,
                f"Failed to send ack of shutdown command to local: {e}", DEBUG_WARNING)
        

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

    
    # Used in network listner
    def handle_listener_thread(self, conn, addr):
        with conn:
            #print(f'Connected by {addr}')
            while self.network == 1:
                try:
                    data = conn.recv(1024)
                    if not data:
                        break
                    msg = data.decode()
                    try:
                        msg_data = json.loads(msg)
                    except Exception:
                        msg_data = {}

                    # Msg sent from Remote Runner, so store name and IP
                    if msg_data.get("type") == "handshake":
                        if msg_data.get("role") == "remote":
                            self.remote_runner_name = msg_data.get("name")
                            self.remote_runner_ip = msg_data.get("ip")
                            print_debug(self._print_target, self.debug_level,
                                        f"Local runner received remote handshake: Name = {self.remote_runner_name}, IP = {self.remote_runner_ip}",
                                        DEBUG_INFO)
                            self.remote_alive = True
                        #conn.sendall("Handshake Acknowledged".encode())
                    
                    elif msg_data.get("type") == "runner_shutdown":
                        if self.role == "remote":
                            print_debug(self._print_target, self.debug_level,
                                        f"Remote runner shutdown requested by {msg_data.get('requested by')}: {msg_data.get('reason')}",
                                        DEBUG_INFO)
                            self.stop()
                            break


                    elif msg_data.get("type") == "runner_shutdown_ack":
                        if self.role == "local":
                            print_debug(self._print_target, self.debug_level,
                                    f"Local Runner shutdown acknowledged by {msg_data.get('runner stopped')}",DEBUG_INFO)
                            self.remote_alive = False
                            self.network = 0
                            break

                    else:
                        conn.sendall(data)
                        print(f"LISTENING: {msg}", flush=True)
                except Exception as e:
                    print_debug(self._print_target, self.debug_level,
                        f"Failed to process incoming message: {e}", DEBUG_WARNING)
                    break
            
        
    def _get_local_ip(self):
        """Function to get the local IP address"""
        local_ip = socket.gethostbyname(socket.gethostname())
        print_debug(self._print_target, self.debug_level,
            f"Machines local IP: {local_ip}", DEBUG_INFO)
        return local_ip
    


    def send_handshake_to_local(self, handshake_port:int=10001):
        """Function to send a handshake message to the local machine"""
        handshake_msg = {
            "type": "handshake",
            "role": self.role,
            "name": self.name,
            "ip": self.local_ip_addr
        }
        print_debug(self._print_target, self.debug_level,
            "Sending handshake", DEBUG_INFO)
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.local_runner_ip, handshake_port))
                s.sendall(json.dumps(handshake_msg).encode())
                # Option to read the response
                """ data = s.recv(1024)
                print_debug(self._print_target, self.debug_level,
                    f"Received: {data.decode()}", DEBUG_INFO) """
                #s.shutdown(socket.SHUT_RDWR)
        except Exception as e:
            print_debug(self._print_target, self.debug_level,
                f"Failed to send handshake: {e}", DEBUG_WARNING)



    def heartbeat_thread_dealer(self, hb_interval:int=5, hb_timeout:int=30):
        
        if self.network != 1:
            print_debug(self._print_target, self.debug_level,
                "Network mode not enabled - Hearbeat thread can't be started", DEBUG_WARNING)
            return

        if self.role == "local" and self.network == 1:
            threading.Thread(target=self.setup_heartbeat_listener, daemon=True).start()
            print_debug(self._print_target, self.debug_level,
                f"{self.role} heartbeat] Listener started", DEBUG_INFO)

        if self.role == "remote" and self.network == 1 and self.hb_sender_thread_active == False:
            self.hb_sender_thread_active = True
            threading.Thread(target=self.send_heartbeat, args=(hb_interval, hb_timeout), daemon=True).start()
            print_debug(self._print_target, self.debug_level,
                f"{self.role} heartbeat] Sender started", DEBUG_INFO)

        

    def send_heartbeat(self, hb_interval:int, hb_timeout:int):
        """Send heartbeats over a constant connection."""
        while self.network == 1:
            time_elapsed = time.time() - self.last_heartbeat_from_remote
            if not self.local_runner_ip:
                print_debug(self._print_target, self.debug_level,
                            "[REMOTE RUNNER HEARTBEAT FUNC] Local runner IP not set yet.", DEBUG_WARNING)
                time.sleep(hb_interval)
                continue

            if time_elapsed > hb_timeout:
                print_debug(self._print_target, self.debug_level,
                            f"Heartbeat not received for: {time_elapsed} seconds Assuming dead", DEBUG_WARNING)
                self.network = 0
                break
            try:
                hb_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                hb_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                hb_socket.settimeout(5)
                hb_socket.connect((self.local_runner_ip, self.hb_port))
                """ print_debug(self._print_target, self.debug_level,
                            "Persistent heartbeat sender connected", DEBUG_INFO) """
            except Exception as e:
                print_debug(self._print_target, self.debug_level,
                            f"Failed to establish persistent heartbeat connection: {e}", DEBUG_WARNING)
                time.sleep(hb_interval)
                continue

            while self.network == 1:
                heartbeat_msg = {
                    "type": "heartbeat",
                    "role": self.role,
                    "name": self.name,
                    "timestamp": time.time()
                }
                try:
                    hb_socket.sendall(json.dumps(heartbeat_msg).encode())
                    
                    # wait for ack
                    data = hb_socket.recv(1024)
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
                    else:
                        print_debug(self._print_target, self.debug_level,
                                    "Unexpected message received", DEBUG_WARNING)

                    # print_debug(self._print_target, self.debug_level,
                    #             "Heartbeat sent over CONSTANT connection", DEBUG_INFO)
                except Exception as e:
                    print_debug(self._print_target, self.debug_level,
                                f"Error sending heartbeat; closing constant connection: {e}", DEBUG_WARNING)
                    hb_socket.close()
                    break 
                time.sleep(hb_interval)
            try:
                hb_socket.close()
            except Exception:
                break


    # Called by the Local Runner to listen for incoming heartbeats from the Remote Runner
    # def heartbeat_listener(self, hb_timeout:int, hb_interval:int):
    #     """Function to listen for incoming heartbeats from the remote runner"""
    #     print("DO I START FIRST??")
    #     while self.network:
    #         time_since_last_hb = time.time() - self.last_heartbeat
    #         if time_since_last_hb > hb_timeout:
    #             print_debug(self._print_target, self.debug_level,
    #                 f"LAST HEARTBEAT GOTTEN AT {self.last_heartbeat}", DEBUG_WARNING)
    #             print_debug(self._print_target, self.debug_level,
    #                 f"Heartbeat not recived for: {time_since_last_hb} seconds", DEBUG_WARNING)
    #             time.sleep(hb_interval)


    # Function to auto generate the network config file, holding local IP and Name for now.
    def generate_network_json_config(self):
        """Function to generate a JSON config file for network mode"""
        config_dir = "/workspaces/meow_base/meow_base/.netconfs"
        if not os.path.exists(config_dir):
            os.makedirs(config_dir)

        config = {
            "name": self.name,
            "ip": self.local_ip_addr,
        }
        conf_file = os.path.join(config_dir, "network_config.json")

        with open(conf_file, "w") as f:
            json.dump(config, f)
        print_debug(self._print_target, self.debug_level,
            f"Network config file generated at: {conf_file}", DEBUG_INFO)
        
        return conf_file



    def transfer_network_config(self, client: paramiko.SSHClient, local_config_file: str):
        """Function to transfer the network config file to the remote machine"""
        try:
            # Again hardcoded directory, maybe change later
            sftp = client.open_sftp()
            remote_dir = "/workspaces/meow_base/meow_base/.netconfs"

            # Check if the directory exists, if not create it
            try:
                sftp.stat(remote_dir)
            except IOError:
                sftp.mkdir(remote_dir)
    

            remote_config_path = f"{remote_dir}/transfered_network_config.json"
            sftp.put(local_config_file, remote_config_path)
            print_debug(self._print_target, self.debug_level,
                    f"Transferred network config file to remote: {remote_config_path}", DEBUG_INFO)
            sftp.close()
        except Exception as e:
            print_debug(self._print_target, self.debug_level,
                    f"Failed to send network config file: {e}", DEBUG_WARNING)




    def setup_ssh_connection_to_remote(self, ssh_config_path:Any=os.path.expanduser("~/.ssh/config")):
        """Function to setup an SSH connection to a remote machine, and tell it to send debug msg's to given socket."""
        try:
            ssh_config = paramiko.SSHConfig.from_path(ssh_config_path)
            found_conf = ssh_config.lookup(self.ssh_config_alias)
            #print(f"Host Conf: {found_conf}")

            # Get the values if present in the config file
            conf_host_name = found_conf.get("hostname")
            conf_user = found_conf.get("user")
            conf_port = found_conf.get("port")
        except Exception as e:
            print_debug(self._print_target, self.debug_level,
                f"Failed to load SSH config: {e}", DEBUG_WARNING)
            return

        client = paramiko.SSHClient()
        client.load_system_host_keys()

        # This warns if the host is unknown
        """client.set_missing_host_key_policy(paramiko.WarningPolicy())"""

        # This adds it, so no warning (Maybe not best practice but works here)
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        client.connect(hostname=conf_host_name, username=conf_user, port=conf_port, key_filename=self.ssh_private_key_dir)
        print_debug(self._print_target, self.debug_level,
            "SSH connection established", DEBUG_INFO)
    

        # Transfer the network config file to the remote machine
        try:
            local_config_to_send = self.generate_network_json_config()
            self.transfer_network_config(client, local_config_to_send)
        except Exception as e:
            print_debug(self._print_target, self.debug_level,
                f"Failed to transfer network config file: {e}", DEBUG_WARNING)
            return



        meow_base_path = "/workspaces/meow_base"
        # meow_base_path = "/home/rlf574/meow_base"
        #stdin, stdout, stderr = 
        client.exec_command(f'cd {meow_base_path}/examples && source /app/venv/bin/activate && nohup python3 skeleton_runner.py > log.txt 2>&1 &')
    
        #print("Remote stdout:", stdout.read().decode())
        #print("Remote stderr:", stderr.read().decode())

        client.close()


    def load_transfered_network_config(self):
        """ Looks for the JSON config file in the .netconfs folder, parses the file, and stores the local runner's Name and IP."""

        # These dirs are currently hard coded. Maybe a good idea to make it more robust and constimizable later
        config_dir = "/workspaces/meow_base/meow_base/.netconfs"
        # Naming of the file is also hard coded atm, another idea could be to timestamp them and look for the latest one, (but maybe out of scope if we asume only ever one Remote Runner)
        config_file = os.path.join(config_dir, "transfered_network_config.json")
        
        if os.path.exists(config_file):
            try:
                with open(config_file, "r") as f:
                    config = json.load(f)
                self.local_runner_name = config.get("name")
                self.local_runner_ip = config.get("ip")
                print_debug(self._print_target, self.debug_level,
                            f"Loaded local runner config: Name = {self.local_runner_name}, IP = {self.local_runner_ip}",
                            DEBUG_INFO)
            except Exception as e:
                print_debug(self._print_target, self.debug_level,
                            f"Error reading local runner config: {e}", DEBUG_WARNING)
        else:
            print_debug(self._print_target, self.debug_level,
                        f"Config file not found at {config_file}", DEBUG_WARNING)




    # Maybe an idea for checking common file paths for storing ssh keys and look through them. Maybe not needed or out of scope
    # def validate_ssh_filepath(self):
    #     """Function to validate the SSH key file path"""
    #     if not os.path.exists(self.ssh_key_dir):
    #         print_debug(self._print_target, self.debug_level,
    #             f"SSH Key directory '{self.ssh_key_dir}' does not exist", DEBUG_WARNING)   


    # Function to allow for debug msgs and handshake and heartbeat msgs to be sent to the Remote or Local machine depending on each Runners "role"
    def send_message(self, ip_addr, message):
        """Function to send a message over the socket connection when 
        there is a status to report"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((ip_addr, self.debug_port))
                #print(f'IP: {self.ip_addr}, Port: {self.port}')
                #print(f'Sending message: {message}')


                """ print_debug(self._print_target, self.debug_level,
                    f"Sending message: {message}", DEBUG_INFO) """
                
                message = f"{self.name}: {message}\n"
                sock.sendall(message.encode())
                #sock.shutdown(socket.SHUT_RDWR)
        except Exception as e:
            print_debug(self._print_target, self.debug_level,
                f"Failed to send message: {e}", DEBUG_WARNING)

    
    def send_attached_conductors(self):
        """Function to send attached conductors to the remote machine"""

        if not self.conductors:
            msg = f"No Conductors attached to {self.name}"
            if self.role == "remote":
                self.send_message(self.local_runner_ip, msg)
            elif self.role == "local":
                self.send_message(self.remote_runner_ip,msg)
            return
        else:
            conductor_names = [conductor.__class__.__name__ for conductor in self.conductors]
            msg = f"Attached Conductors to {self.name}: {', '.join(conductor_names)}"
            if self.role == "remote":
                self.send_message(self.local_runner_ip, msg)
            elif self.role == "local":
                self.send_message(self.remote_runner_ip, msg)
            
    

    def send_attached_handlers(self):
        """Function to send attached handlers to the remote machine"""

        if not self.handlers:
            msg = f"No Handlers attached to {self.name}"
            self.send_message(msg)
            return
        else:
            handler_names = [handler.__class__.__name__ for handler in self.handlers]
            msg = f"Attached Handlers to {self.name}: {', '.join(handler_names)}"
            self.send_message(msg)


    def send_attached_monitors(self):
        """Function to send attached handlers to the remote machine"""

        if not self.monitors:
            msg = f"No Monitors attached to {self.name}"
            self.send_message(msg)
            return
        else:
            monitor_names = [monitor.__class__.__name__ for monitor in self.monitors]
            msg = f"Attached Monitors to {self.name}: {', '.join(monitor_names)}"
            self.send_message(msg)


    # Maybe usefull - !Not Done!
    def send_all_attached_components(self):
        """Function to send all attached components to the remote machine - (monitors, conductors, handlers)"""
        pass
