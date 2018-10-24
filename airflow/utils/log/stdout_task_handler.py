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

import io
import json
import logging
import os
import requests
import sys

from jinja2 import Template

from airflow import configuration as conf
from airflow.configuration import AirflowConfigException
from airflow.utils.file import mkdirs

# LogRecord labels to include in the output
RECORD_LABELS = ['asctime', 'levelname', 'filename', 'lineno', 'message']

class modifiedStdout():
    """
    Class to keep track of the stdout context when running in child process.
    """
    def write(self, string):
        sys.__stdout__.write(string)

class JsonFormatter(logging.Formatter):
    """
    JsonFormatter is a custom formatter that inherits from logging.Formatter.
    It will overwrite the format method with its own logic.
    Given an object of processed task instance information, it will
    grab relevant log state information from the LogRecord object and
    merge the two objects before returning the combined json.
    """
    def __init__(self, processedTask=None):
        super(JsonFormatter, self).__init__()
        self.processedTask = processedTask
    def format(self, record):
        recordObject = {label: getattr(record, label) for label in RECORD_LABELS}

        # Merge LogRecord Object and self.processedTask Object
        recordObject = {**recordObject, **self.processedTask}
        return json.dumps(recordObject, indent=4)

class StdoutTaskHandler(logging.Handler):
    """
    StdoutTaskHAndler is a python log handler that receives task
    instance context and sends task instance logs directly to the
    standard out of the running process.
    """
    def __init__(self):
        """
        :param taskInstance: unique identifier for task instance information (dag_id, task_id, execution_date, and try_number)
        :param writer: object to modify the stdout to allow child processes to write to stdout
        """
        super(StdoutTaskHandler, self).__init__()
        self.handler = None
        self.taskInstance = None
        self.writer = None

    def set_context(self, ti):
        """
        Provide task_instance context. Set the sys.stdout to the writer object to save stdout context.
        Parse task instance information into the task instance attribute.
        :param ti: task instance object
        """
        # Save the context of stdout so child process can call
        self.writer = modifiedStdout()
        sys.stdout = self.writer

        # Parse important task information out of task instance object
        self.taskInstance = self._process_taskInstance(ti)

        # Set context for handler, including formatter and logging level
        self.handler = logging.StreamHandler(stream=sys.stdout)
        self.handler.setFormatter(JsonFormatter(self.taskInstance))
        self.handler.setLevel(self.level)

    def emit(self, record):

        # It is the Formatter's responsibility to set 'asctime' and 'message'
        self.formatter.format(record)

        if self.handler is not None:
            self.handler.emit(record)

    def flush(self):

        if self.handler is not None:
            self.handler.flush()

    def close(self):
        if self.handler is not None:
            self.handler.close()
            sys.stdout = sys.__stdout__

    def _process_taskInstance(self, ti):
        """
        Transform task instance class into an object with unique task identifiers
        """
        ti_info =  {'dag_id': str(ti.dag_id),
                    'task_id': str(ti.task_id),
                    'execution_date': str(ti.execution_date),
                    'try_number': str(ti.try_number)}
        return ti_info
