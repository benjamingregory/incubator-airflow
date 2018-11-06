# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging
import os
import requests
import time
import json
import pickle
import msgpack

from io import BytesIO
from jinja2 import Template

from airflow import configuration as conf
from airflow.configuration import AirflowConfigException
from airflow.utils.file import mkdirs

from airflow.exceptions import AirflowException
from airflow.utils.log.file_task_handler import FileTaskHandler
from fluent import sender
from fluent import event
from fluent import handler

class FluentDTaskHandler(FileTaskHandler):
    """
    FluentDTaskHandler is a python log handler that inherits from
    the FileTaskHandler. It receives task instance context and sends
    task instance logs to the FluentSender through the exposed TCP port.
    To display logs on the Web UI, it reads logs from the task instance's
    host machine.
    """

    def __init__(self, base_log_folder, filename_template, hostname, fluent_tag, fluent_port):
        """
        :param base_log_folder: Base log folder to place logs to read from for Flask
        :param filename_template: template filename string
        """
        super(FileTaskHandler, self).__init__()
        self._logger = None
        self.ti_to_json = None
        self.hostname = hostname
        self.fluent_tag = fluent_tag
        self.fluent_port = fluent_port
        self.handler = None

        # This is from the file task handler to replicate reading logs in Web UI
        self.local_base = base_log_folder
        self.filename_template = filename_template
        self.filename_jinja_template = None

        if "{{" in self.filename_template:
            self.filename_jinja_template = Template(self.filename_template)

        # child = os.getpid()
        # parent = os.getppid()
        # print(f"This is a fluentD log handler instance on child pid {child}")
        # print(f"This is a fluentD log handler instance on parent pid {parent}")

    def set_context(self, ti):
        """
        Provide task_instance context. Initialize FluentSender on port 24224.
        Parse ti information into usable JSON.
        :param ti: task instance object
        """
        # child = os.getpid()
        # parent = os.getppid()
        # print(f"Im setting the context in child pid {child} and parent pid {parent}")
        self._logger = sender.FluentSender(self.fluent_tag, host=self.hostname, port=self.fluent_port)
        self.ti_to_json = self._process_json(ti)

        local_loc = self._init_file(ti)
        self.handler = logging.FileHandler(local_loc)
        self.handler.setFormatter(self.formatter)
        self.handler.setLevel(self.level)

    def emit(self, record):
        """
        From the Log Record object, collect the message and add it to the parsed
        task instance JSON from set_context.
        :param record: log record object
        """
        msg = record.getMessage()

        if msg and self.ti_to_json is not None:
            self.ti_to_json['message'] = msg

        # File handler to satisfy logging checks and move logs to Web UI
        if self.handler is not None:
            self.handler.emit(record)

        # Send the data to the FluentSender
        if all([self._logger, self.ti_to_json]):
            if not self._logger.emit('task_instance', self.ti_to_json):
                print(logger.last_error)
                logger.clear_last_error()

    def flush(self):
        if self.handler is not None:
            self.handler.flush()

    def close(self):
        if self._logger is not None and self.handler is not None:
            self._logger.close()
            self.handler.close()

    # If something crashes, this should save all of the pending logs from being lost
    # def overflow_handler(self, pendings):
    #     unpacker = msgpack.Unpacker(BytesIO(pendings))
    #     for unpacked in unpacker:
    #         print(unpacked)

    def _process_json(self, ti):
        """
        Turn task instance object into JSON object
        """
        # print(pickle.dumps(ti))
        ti_info =  {'dag_id': str(ti.dag_id),
                    'task_id': str(ti.task_id),
                    'task': str(ti.task)}
        return ti_info

    # Inherit from FileTaskHandler to allow reading logs in Web UI

    def _render_filename(self, ti, try_number):
        return super()._render_filename(ti, try_number)

    def _init_file(self, ti):
        return super()._init_file(ti)

    def _read(self, ti, try_number):
        return super()._read(ti, try_number)

    def read(self, task_instance, try_number=None):
        return super().read(task_instance, try_number)
