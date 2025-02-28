
"""
This file contains the defintion for the MeowRunner, the main construct used 
for actually orchestration MEOW analysis. It is intended as a modular system, 
with monitors, handlers, and conductors being swappable at initialisation.

Author(s): David Marchant
"""
import os
import sys
import threading

import socket # Networking functionality

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
            print:Any=sys.stdout, logging:int=0)->None:
        """MeowRunner constructor. This connects all provided monitors, 
        handlers and conductors according to what events and jobs they produce 
        or consume."""

        self._is_valid_job_queue_dir(job_queue_dir)
        self._is_valid_job_output_dir(job_output_dir)
        
        # self.ip
        # self.port
        
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

        # Setup debugging
        self._print_target, self.debug_level = setup_debugging(print, logging)

        # Setup queues
        self.event_queue = []
        self.job_queue = []

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

    def setup_ssh_connection(self, ip_addr, port):
        """Function to setup an SSH connection to a remote machine."""
        # TODO(tt): make checks for structure of ip_addr and port.        
        
        self.ip_addr = ip_addr
        self.port = port
        return ip_addr, port 
    
    def send_message(self, message):
        """Function to send a message over the socket connection when 
        there is a status to report"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.ip_addr, self.port))
                print(f'IP: {self.ip_addr}, Port: {self.port}')
                print(f'Sending message: {message}')
                sock.sendall(message.encode())
        except Exception as e:
            print(f'Failed to send message: {e}')