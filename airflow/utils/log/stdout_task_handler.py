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

import elasticsearch
import io
import json
import logging
import os
import requests
import sys

from elasticsearch_dsl import Search
from jinja2 import Template
from airflow.utils import timezone


from airflow import configuration as conf
from airflow.configuration import AirflowConfigException
from airflow.utils.file import mkdirs

from airflow.utils.log.es_task_handler import ElasticsearchTaskHandler


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
        recordObject = {**recordObject, **self.processedTask}
        return json.dumps(recordObject)

class StdoutTaskHandler(ElasticsearchTaskHandler):
    """
    StdoutTaskHAndler is a python log handler that receives task
    instance context and sends task instance logs directly to the
    standard out of the running process.
    """
    def __init__(self, log_id_template, filename_template, end_of_log_mark, host):
        """
        :param taskInstance: unique identifier for task instance information (dag_id, task_id, execution_date, and try_number)
        :param writer: object to modify the stdout to allow child processes to write to stdout
        """
        super(StdoutTaskHandler, self).__init__(log_id_template, filename_template, end_of_log_mark, host)
        self.handler = None
        self.taskInstance = None
        self.writer = None
        self.closed = False

        self.client = elasticsearch.Elasticsearch(['elasticsearch:9200'])

    def set_context(self, ti):
        """
        Provide task_instance context. Set the sys.stdout to the writer object to save stdout context.
        Parse task instance information into the task instance attribute.
        :param ti: task instance object
        """
        self.writer = modifiedStdout()
        sys.stdout = self.writer

        self.taskInstance = self._process_taskInstance(ti)

        self.handler = logging.StreamHandler(stream=sys.stdout)
        self.handler.setFormatter(JsonFormatter(self.taskInstance))
        self.handler.setLevel(self.level)

    def emit(self, record):
        """
        It is the Formatter's responsibility to set 'asctime' and 'message'.
        Without calling `format` on the LogRecord, these fields will be undefined.
        """
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

    # Overwrite file_task_handler's read function, because elastic task handler's _read returns a tuple
    def read(self, task_instance, try_number=None):
        if try_number is None:
            next_try = task_instance.next_try_number
            try_numbers = list(range(1, next_try))
        elif try_number < 1:
            logs = [
                'Error fetching the logs. Try number {} is invalid.'.format(try_number),
            ]
            return logs
        else:
            try_numbers = [try_number]

        logs = [''] * len(try_numbers)
        for i, try_number in enumerate(try_numbers):
            logs[i] += self._read(task_instance, try_number)[0]
        return logs
