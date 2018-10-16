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

class FluentDTaskHandler(logging.Handler, LoggingMixin):
    def __init__(self):
        self.closed = False
        self.handler = None
        self._name = 'FluentDHandler'
        self._logger = sender.FluentSender('app', host='localhost', port=24224)
        self._logger.emit('app', {
            'from': 'test_init',
            'to': 'test'
        })
        # self._get_sender()
        # self._logger = logging.getLogger('fluent.test')
        # self._handler = handler.FluentHandler('app', host='localhost', port=24224)
        # self._logger.info({'from': 'userA', 'to': 'userB'})

    def emit(self, record):
        if self.logger is not None:
            self.logger.emit('app', record)

    def emit_with_time(self, record):
        if self.logger is not None:
            event_time = int(time.time())
            self.logger.emit_with_time('app', event_time, record)

    def set_context(self, ti):
        super(FluentDTaskHandler, self).set_context(ti)
        self.handler = logging.getLogger('fluent.test')
        self.handler.setFormatter(self.formatter)
        self.handler.setLevel(self.level)

    def close(self):
        if self.closed:
            return
        self.sender.close()
