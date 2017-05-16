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
from collections.abc import Set
from numbers import Number

from airflow import configuration
from airflow.exceptions import AirflowException

# Constants for resources (megabytes are the base unit)
MB = 1
GB = 1024 * MB
TB = 1024 * GB
PB = 1024 * TB
EB = 1024 * PB

registry = {}

def register_resource(target_class):
    registry[target_class.__name__.lower()] = target_class

class ResourceMeta(type):
    """
    Register Resource subclasses
    """
    def __new__(meta, name, bases, class_dict):
        cls = type.__new__(meta, name, bases, class_dict)
        register_resource(cls)
        return cls


class Resource(object, metaclass=ResourceMeta):
    def __init__(self, value):
        self.value = value
        self.validate()

    def __repr__(self):
        return str(self.value)

    def validate(self):
        """
        abstract method for subclasses to implement for validating
        value
        """
        pass

class ScalarResource(Resource):
    """
    A numeric resource type (e.g. cpu, disk, ram)
    """
    def validate(self):
        if self.value < 0:
            raise AirflowException(
                'Received resource quantity {} for scalar resource but resource quantity '
                'must be non-negative.'.format(self.value))


class TextResource(Resource):
    """
    A text resource type (e.g. network, vpc, node_name)
    """
    def __init__(self, value):
       super(TextResource, self).__init__(str(value))

# Define Common Resource Types
class CPU(ScalarResource): pass

class RAM(ScalarResource): pass

class GPU(ScalarResource): pass

class Disk(ScalarResource): pass

class ResourceSet(Set):
    """
    A Set of Resources. Allows for specifying custom resources via args, kwargs, dicts, or another ResourceSet
    A ResourceSet is immutable
    """
    def __init__(self, *args, **kwargs):
        self._resources = {}
        for arg in list(args):
            if isinstance(arg, self.__class__):
                self.__dict__ = arg.__dict__.copy()
                break
            elif isinstance(arg, dict):
                self.add_from_dict(arg)
                pass
            elif not isinstance(arg, (CPU, GPU, RAM, Disk)):
                raise AirflowException('only args of type CPU, RAM, GPU, and Disk are allowed as positional arguments')
            self.add_resource(arg.__class__.__name__.lower(), arg)
        self.add_from_dict(kwargs)
        self.apply_defaults()

    def apply_defaults(self):
        if not 'cpu' in self._resources:
            self.add_resource('cpu', CPU(configuration.getfloat('operators', 'default_cpu')))
        if not 'ram' in self._resources:
            self.add_resource('ram', RAM(configuration.getfloat('operators', 'default_ram')))
        if not 'disk' in self._resources:
            self.add_resource('disk', Disk(configuration.getfloat('operators', 'default_disk')))
        if not 'gpu' in self._resources:
            self.add_resource('gpu', GPU(configuration.getfloat('operators', 'default_gpu')))

    def add_resource(self, key, value):
        if key in self._resources:
            raise AirflowException('passed the same resource type twice')
        self._resources[key] = value

    def add_from_dict(self, resources_dict):
        for key, value in resources_dict.items():
            if isinstance(value, Resource):
                self.add_resource(key, value)
            else:
                lowerCaseKey = key.lower()
                if lowerCaseKey in registry:
                    inferred_cls = registry[lowerCaseKey]
                    resource = inferred_cls(value)
                    self.add_resource(key, resource)
                else:
                    # infer type of resource as Scalar or Text
                    if isinstance(value, Resource):
                        self.add_resource(key, value)
                    elif isinstance(value, str):
                        resource = TextResource(value)
                        self.add_resource(key, resource)
                    elif isinstance(value, Number):
                        resource = ScalarResource(value)
                        self.add_resource(key, resource)

    def __repr__(self):
        return str(self._resources)

    def __getattr__(self, attr):
        return self._resources.get(attr)

    def __iter__(self):
        for key, val in self._resources.items():
            yield (key, val)

    def __len__(self):
        return len(self._resources.items())

    def __contains__(self, value):
        return value in self._resources
