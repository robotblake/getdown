import copy
import os
import re
import struct
import sys

from thrift.transport import TTransport
from thrift.protocol import TCompactProtocol

from .parquet.ttypes import FileMetaData

def underscore(word):
    word = re.sub(r"([A-Z\d]+)([A-Z][a-z])", r"\1_\2", word)
    word = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", word)
    word = word.replace("-", "_")
    return word.lower()

def lower(word):
    return word.lower()

def _read_footer(fileobj, offset):
    fileobj.seek(offset)
    protocol = _create_protocol(fileobj)
    metadata = FileMetaData()
    metadata.read(protocol)
    return metadata

def _write_footer(fileobj, offset, metadata):
    fileobj.seek(offset)
    protocol = _create_protocol(fileobj)
    metadata.write(protocol)
    return fileobj.tell() - offset

def _create_protocol(fileobj):
    transport = TTransport.TFileObjectTransport(fileobj)
    protocol = TCompactProtocol.TCompactProtocol(transport)
    return protocol

def _transform(fileobj, transform):
    file_size = os.fstat(fileobj.fileno()).st_size

    if file_size < 12:
        raise RuntimeError("file is too small")

    fileobj.seek(file_size - 8)
    footer_size = struct.unpack('<i', fileobj.read(4))[0]
    magic_number = fileobj.read(4)

    if magic_number != 'PAR1':
        raise RuntimeError("magic number is invalid")

    if file_size < (12 + footer_size):
        raise RuntimeError("file is too small")

    footer_offset = file_size - 8 - footer_size
    metadata = _read_footer(fileobj, footer_offset)

    for schema_element in metadata.schema:
        old_name = schema_element.name
        new_name = transform(old_name)
        schema_element.name = new_name

    for row_group in metadata.row_groups:
        for column in row_group.columns:
            old_path = column.meta_data.path_in_schema
            new_path = [transform(p) for p in old_path]
            column.meta_data.path_in_schema = new_path

    new_footer_size = _write_footer(fileobj, footer_offset, metadata)
    fileobj.write(struct.pack('<i', new_footer_size))
    fileobj.write('PAR1')

TRANSFORMS = {
    'lowercase': lambda word: word.lower(),
    'underscore': underscore,
}

def usage():
    print("usage: python -m getdown <lowercase|underscore> <myfile.parquet>")
    sys.exit(1)

if len(sys.argv) < 3:
    usage()

if sys.argv[1] not in TRANSFORMS:
    usage()

with open(sys.argv[2], 'r+b') as fileobj:
    _transform(fileobj, TRANSFORMS[sys.argv[1]])
