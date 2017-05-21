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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import unittest

import airflow.utils.logging
from airflow import configuration
from airflow.exceptions import AirflowException
from airflow.utils.operator_resources import (
        ResourceSet, Resource, ScalarResource, TextResource, CPU, GPU, Disk, RAM
)


class LogUtilsTest(unittest.TestCase):

    def test_gcs_url_parse(self):
        """
        Test GCS url parsing
        """
        logging.info(
            'About to create a GCSLog object without a connection. This will '
            'log an error but testing will proceed.')
        glog = airflow.utils.logging.GCSLog()

        self.assertEqual(
            glog.parse_gcs_url('gs://bucket/path/to/blob'),
            ('bucket', 'path/to/blob'))

        # invalid URI
        self.assertRaises(
            AirflowException,
            glog.parse_gcs_url,
            'gs:/bucket/path/to/blob')

        # trailing slash
        self.assertEqual(
            glog.parse_gcs_url('gs://bucket/path/to/blob/'),
            ('bucket', 'path/to/blob'))

        # bucket only
        self.assertEqual(
            glog.parse_gcs_url('gs://bucket/'),
            ('bucket', ''))

class ResourceSetTest(unittest.TestCase):

    def test_all_resources_specified(self):
        resources = ResourceSet(cpu=1, ram=2, disk=3, gpu=4)
        self.assertEqual(resources.cpu.value, 1)
        self.assertEqual(resources.ram.value, 2)
        self.assertEqual(resources.disk.value, 3)
        self.assertEqual(resources.gpu.value, 4)

    def test_some_resources_specified(self):
        resources = ResourceSet(cpu=0, disk=1)
        self.assertEqual(resources.cpu.value, 0)
        self.assertEqual(resources.ram.value, configuration.getfloat('operators', 'default_ram'))
        self.assertEqual(resources.disk.value, 1)
        self.assertEqual(resources.gpu.value, configuration.getfloat('operators', 'default_gpu'))

    def test_no_resources_specified(self):
        resources = ResourceSet()
        self.assertEqual(resources.cpu.value,
                         configuration.getint('operators', 'default_cpu'))
        self.assertEqual(resources.ram.value,
                         configuration.getint('operators', 'default_ram'))
        self.assertEqual(resources.disk.value,
                         configuration.getint('operators', 'default_disk'))
        self.assertEqual(resources.gpu.value,
                         configuration.getint('operators', 'default_gpu'))

    def test_kwargs_text_resources(self):
        resources = ResourceSet(text='Text Resource')
        self.assertEqual(resources.text.value, 'Text Resource')

    def test_negative_resource_qty(self):
        with self.assertRaises(AirflowException):
            ResourceSet(cpus=-1)

    def test_varargs_must_be_resources(self):
        with self.assertRaises(AirflowException):
            ResourceSet(1)

    def test_varargs_must_be_concrete(self):
        with self.assertRaises(AirflowException):
            ResourceSet(ScalarResource(1))

    def test_concrete_varargs_become_props(self):
        resources = ResourceSet(CPU(3), RAM(2), Disk(4), GPU(5))
        self.assertEqual(resources.cpu.value, 3)
        self.assertEqual(resources.ram.value, 2)
        self.assertEqual(resources.disk.value, 4)
        self.assertEqual(resources.gpu.value, 5)

    def test_varargs_must_be_unique_resources(self):
        with self.assertRaises(AirflowException):
            resources = ResourceSet(CPU(1), CPU(3))

    def test_kwargs_and_args_are_exclusive(self):
        with self.assertRaises(AirflowException):
            resources = ResourceSet(CPU(1), cpu=2)

    def test_resource_set_can_be_copied(self):
        resource1 = ResourceSet(cpu=2, ram=2)
        resource2 = ResourceSet(resource1)
        self.assertEqual(resource1.cpu.value, resource2.cpu.value)
        self.assertEqual(resource1.ram.value, resource2.ram.value)

    def test_resource_set_can_be_constructed_from_dict(self):
        resources = {
                'cpu': 5,
                'ram': 6
        }
        resource_set = ResourceSet(resources)
        self.assertEqual(resource_set.cpu.value, 5)
        self.assertEqual(resource_set.ram.value, 6)

    def test_resource_set_from_dict_len(self):
        resources = {
                'cpu': 5,
                'ram': 6,
                'disk': 512
        }
        resource_set = ResourceSet(resources)
        self.assertEqual(len(resource_set), 4)

    def test_resource_set_is_iterable(self):
        resources = ResourceSet()
        self.assertEqual(len(resources), 4)
