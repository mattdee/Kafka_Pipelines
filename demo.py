#!/usr/bin/python3.6

import struct
import sys

def input_stream():
    """
        Consume STDIN and yield each record that is received from MemSQL
    """
    while True:
        byte_len = sys.stdin.buffer.read(8)
        if len(byte_len) == 8:
            byte_len = struct.unpack("L", byte_len)[0]
            result = sys.stdin.buffer.read(byte_len)
            yield result
        else:
            assert len(byte_len) == 0, byte_len
            return


def log(message):
    """
        Log an informational message to stderr which will show up in MemSQL in
        the event of transform failure.
    """
    sys.stderr.buffer.write(message + b"\n")


def emit(message):
    """
        Emit a record back to MemSQL by writing it to STDOUT.  The record
        should be formatted as JSON, Avro, or CSV as it will be parsed by
        LOAD DATA.
    """
    sys.stdout.buffer.write(message + b"\n")

log(b"Begin transform")

# We start the transform here by reading from the input_stream() iterator.
for data in input_stream():
    # Since this is an identity transform we just emit what we receive.
    emit(data)

log(b"End transform")