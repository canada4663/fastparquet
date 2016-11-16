"""Utils for working with the parquet thrift models."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os

import thriftpy


THRIFT_FILE = os.path.join(os.path.dirname(__file__), "parquet.thrift")
parquet_thrift = thriftpy.load(THRIFT_FILE, module_name=str("parquet_thrift"))


def schema_tree(schema, i=0):
    root = schema[i]
    root.children = {}
    while len(root.children) < root.num_children:
        i += 1
        s = schema[i]
        root.children[s.name] = s
        if s.num_children is not None:
            i = schema_tree(schema, i)
    return i + 1


class SchemaHelper(object):
    """Utility providing convenience methods for schema_elements."""

    def __init__(self, schema_elements):
        """Initialize with the specified schema_elements."""
        self.schema_elements = schema_elements
        self.root = schema_elements[0]
        self.schema_elements_by_name = dict(
            [(se.name, se) for se in schema_elements])
        schema_tree(schema_elements)

    def schema_element(self, name):
        """Get the schema element with the given name or path"""
        root = self.root
        if isinstance(name, str):
            name = [name]
        for part in name:
            root = root.children[part]
        return root

    def is_required(self, name):
        """Return true if the schema element with the given name is required."""
        required = True
        if isinstance(name, str):
            name = [name]
        parts = []
        for part in name:
            parts.append(part)
            s = self.schema_element(parts)
            if s.repetition_type != parquet_thrift.FieldRepetitionType.REQUIRED:
                required = False
                break
        return required

    def max_repetition_level(self, path):
        """Get the max repetition level for the given schema path."""
        max_level = 0
        for part in path:
            element = self.schema_element(part)
            if element.repetition_type == parquet_thrift.FieldRepetitionType.REQUIRED:
                max_level += 1
        return max_level

    def max_definition_level(self, path):
        """Get the max definition level for the given schema path."""
        max_level = 0
        for part in path:
            element = self.schema_element(part)
            if element.repetition_type != parquet_thrift.FieldRepetitionType.REQUIRED:
                max_level += 1
        return max_level
