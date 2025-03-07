
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
            name:str="Unnamed Runner",

            # Added Network Options
            network:int=0, ssh_config_alias:Any=None, ssh_private_key_dir:Any=os.path.expanduser("~/.ssh/id_ed25519"), debug_port:int=10001 )->None:
        """MeowRunner constructor. This connects all provided monitors, 
        handlers and conductors according to what events and jobs they produce 
        or consume."""


        self.name = name
        self.network = network
        self.ssh_config_alias = ssh_config_alias
       
        self.ssh_private_key_dir = ssh_private_key_dir

        # These become redundant with a config file in .ssh
        #self.usr = usr
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

        # Setup debugging
        self._print_target, self.debug_level = setup_debugging(print, logging)

        # Setup queues
        self.event_queue = []
        self.job_queue = []

        self.local_ip_addr = self._get_local_ip()


        if network == 1:
            print_debug(self._print_target, self.debug_level,
                "Network mode enabled", DEBUG_INFO)
            print_debug(self._print_target, self.debug_level,
                f"Config Alias Name: {self.ssh_config_alias}", DEBUG_INFO)
            print_debug(self._print_target, self.debug_level,
                f"Port: {self.debug_port}", DEBUG_INFO)
            print_debug(self._print_target, self.debug_level,
                f"SSH Key Directory: {self.ssh_private_key_dir}", DEBUG_INFO)



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
        the remote machine."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            #print(f"DEBUG: Binding listener to {self.ip_addr}:{self.port}")
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.local_ip_addr, self.debug_port))
            s.listen()
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
                    """ print_debug(self._print_target, self.debug_level,
                        "Listener thread started", DEBUG_INFO)
                    print_debug(self._print_target, self.debug_level,
                        f"Listening on port {self.port}", DEBUG_INFO) """


    
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

        """ if self.network == 1:

            # Send attached conductors, handlers & monitors to the remote machine
            self.send_attached_conductors()
            self.send_attached_handlers()

            # Send a message to the remote machine
            print_debug(self._print_target, self.debug_level,
                "Message sent to remote machine", DEBUG_INFO) """
        


        if self.network == 1:
            print_debug(self._print_target, self.debug_level,
                "Setting up network connection...", DEBUG_INFO)
            if self._network_listener_worker is None:
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
            
            
            self.setup_ssh_connection_to_remote()
            print_debug(self._print_target, self.debug_level,
                "SSH connection established", DEBUG_INFO)
        


    def stop(self)->None:
        """Function to stop the runner by stopping all of the constituent 
        monitors, handlers and conductors, along with managing interaction 
        threads."""

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
        

        # Maybe not needed as threads close their connections on their own
        if self._network_listener_worker is None:
            msg = "Cannot stop network listener thread that is not started."
            print_debug(self._print_target, self.debug_level,
                msg, DEBUG_WARNING)
            raise RuntimeWarning(msg)
        else:
            self._stop_listener_pipe[1].send(1)
            self._network_listener_worker.join()
        print_debug(self._print_target, self.debug_level,
            "Network listener thread stopped", DEBUG_INFO)

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
            print(f'Connected by {addr}')
            while True:
                try:
                    data = conn.recv(1024)
                    if not data:
                        break
                    conn.sendall(data)
                    print(f"LISTENING: {data.decode()}", flush=True)
                except Exception as e:
                    print_debug(self._print_target, self.debug_level,
                        f"Failed to send message: {e}", DEBUG_WARNING)
                    break
        

    def _get_local_ip(self):
        """Function to get the local IP address"""
        local_ip = socket.gethostbyname(socket.gethostname())
        print(f"DEBUGggg: Local IP: {local_ip}")
        return local_ip


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
    

        meow_base_path = "/workspaces/meow_base"
        #meow_base_path = "/home/rlf574/meow_base"
        local_ip_addr = self._get_local_ip()
        stdin, stdout, stderr = client.exec_command(f'export HOST_IP={local_ip_addr} && cd {meow_base_path}/examples && source /app/venv/bin/activate && python3 skeleton_runner.py')
    
        #print("Remote stdout:", stdout.read().decode())
        #print("Remote stderr:", stderr.read().decode())

        # Bad temp fix... maybe
        self.remote_alive = True

        client.close()




    # Maybe an idea for checking common file paths for storing ssh keys and look through them. Maybe not needed or out of scope
    def validate_ssh_filepath(self):
        """Function to validate the SSH key file path"""
        if not os.path.exists(self.ssh_key_dir):
            print_debug(self._print_target, self.debug_level,
                f"SSH Key directory '{self.ssh_key_dir}' does not exist", DEBUG_WARNING)   


    def send_message(self, message):
        """Function to send a message over the socket connection when 
        there is a status to report"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.ip_addr, self.debug_port))
                #print(f'IP: {self.ip_addr}, Port: {self.port}')
                #print(f'Sending message: {message}')


                """ print_debug(self._print_target, self.debug_level,
                    f"Sending message: {message}", DEBUG_INFO) """
                
                message = f"{self.name}: {message}\n"
                sock.sendall(message.encode())
                sock.shutdown(socket.SHUT_RDWR)
        except Exception as e:
            print_debug(self._print_target, self.debug_level,
                f"Failed to send message: {e}", DEBUG_WARNING)

    
    def send_attached_conductors(self):
        """Function to send attached conductors to the remote machine"""

        if not self.conductors:
            msg = f"No Conductors attached to {self.name}"
            self.send_message(msg)
            return
        else:
            conductor_names = [conductor.__class__.__name__ for conductor in self.conductors]
            msg = f"Attached Conductors to {self.name}: {', '.join(conductor_names)}"
            self.send_message(msg)
    

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


    """ def check_remote_runner_alive(self):
        ""Function to check if the remote runner is still alive""
        if self.remote_alive:
            return True
        else:
            # shutdown local runner
            self.stop() """
        