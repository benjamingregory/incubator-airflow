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

from jinja2 import Template

from airflow import configuration as conf
from airflow.configuration import AirflowConfigException
from airflow.utils.file import mkdirs

from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.log.file_task_handler import FileTaskHandler
from fluent import sender
from fluent import event
from fluent import handler

class FluentDTaskHandler(logging.Handler):
    def __init__(self):
        super(FluentDTaskHandler, self).__init__()
        self.closed = False
        self._name = 'FluentDHandler'
        self._logger = None
        self.ti_to_json = None
        self.handler = None

    def set_context(self, ti):
        self._logger = sender.FluentSender('app', host='fluentd', port=24224)
        self.handler = logging.NullHandler()
        self.ti_to_json = self._process_json(ti)

    def emit(self, record):

        msg = record.getMessage()

        if msg and self.ti_to_json is not None:
            self.ti_to_json['message'] = msg

        if self.handler is not None:
            self.handler.emit(record)

        if all([self._logger, self.ti_to_json]):
            if not self._logger.emit('app', self.ti_to_json):
                print(logger.last_error)
                logger.clear_last_error()

    def _process_json(self, ti):
        # print(pickle.dumps(ti))
        ti_info =  {'dag_id': str(ti.dag_id),
                    'task_id': str(ti.task_id),
                    'task': str(ti.task)}
        # print(ti_json)
        # return ti_json
        # return {'test': 'test_process_json'}
        return ti_info

    def close(self):
        if self.closed or not self._logger or not self.handler:
            return
        self._logger.close()
        self.handler.close()
        self.closed = True
