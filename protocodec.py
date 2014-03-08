import struct
from proto import simple_pb2


def serialize_message(msg):
    pb_buf = msg.SerializeToString()
    pb_buf_len = struct.pack('!I', len(pb_buf))
    msg_buf = 'PB' + pb_buf_len + pb_buf
    return msg_buf


def parse_message(buf):
    msg = simple_pb2.SimpleRequest()
    try:
        msg.ParseFromString(buf)
        return msg
    except simple_pb2.message.DecodeError:
        return None
