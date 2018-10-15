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

from airflow import configuration
from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.log.file_task_handler import FileTaskHandler
from fluent import sender
from fluent import event

class FluentDTaskHandler(logging.Handler):
    def __init__(self):
        self.closed = False
        self.sender = sender.setup('airflow', host='localhost', port=24224)
        event.Event('tagged', {
            'from': 'test',
            'to': 'elasticsearch'
        })

    def write(self):
        event.Event('tagged', {
            'from': 'test_write',
            'to': 'elasticsearch'
        })

    def close(self):
        if self.closed:
            return
        self.sender.close()
