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
from airflow.utils.helpers import parse_template_string


RECORD_LABELS = ['asctime', 'levelname', 'filename', 'lineno', 'message']

class modifiedStdout():
    """
    Class to keep track of the stdout context when running in child process.
    """
    def __init__(self):
        self.closed = False
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

        self.client = elasticsearch.Elasticsearch(['elasticsearch:9200'])
        self.log_id_template, self.log_id_jinja_template = \
            parse_template_string(log_id_template)
        self.mark_end_on_close = True
        self.end_of_log_mark = end_of_log_mark

        self.logMetadata = None
        self.closed = False


    def set_context(self, ti):
        """
        Provide task_instance context. Set the sys.stdout to the writer object to save stdout context.
        Parse task instance information into the task instance attribute.
        :param ti: task instance object
        """
        # super(StdoutTaskHandler, self).set_context(ti)

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

        # Prevent uploading log to remote storage multiple times
        if self.closed:
            return

        if not self.mark_end_on_close:
            self.closed = True
            return

        # Just in case the context of handler was not set
        if self.handler is None:
            self.closed = True
            return

        if self.handler.stream is None or self.handler.stream.closed:
            self.handler.stream = self.handler._open()

        self.handler.stream.write(self.end_of_log_mark)

        if self.handler is not None:
            self.writer.closed = True
            self.handler.close()
            sys.stdout = sys.__stdout__

        self.closed = True

    def _process_taskInstance(self, ti):
        """
        Transform task instance class into an object with unique task identifiers
        """
        ti_info =  {'dag_id': str(ti.dag_id),
                    'task_id': str(ti.task_id),
                    'execution_date': str(ti.execution_date),
                    'try_number': str(ti.try_number)}
        return ti_info

    def read(self, task_instance, try_number=None):
        """
        Read logs of a given task instance from elasticsearch.
        :param task_instance: task instance object
        :param try_number: task instance try_number to read logs from. If None,
                           it will start at 1.
        """
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
        self.logMetadata = [{}] * len(try_numbers)
        for i, try_number in enumerate(try_numbers):
            log, metadata = self._read(task_instance, try_number, self.logMetadata[i])
            logs[i] += log
            self.logMetadata[i] = metadata

        return logs
