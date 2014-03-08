import struct
from proto import simple_pb2


def serialize_message(msg):
    meta_info = simple_pb2.MetaInfo()
    meta_info.msg_name = msg.DESCRIPTOR.full_name

    meta_buf = meta_info.SerializeToString()
    msg_buf = msg.SerializeToString()
    meta_buf_len = len(meta_buf)
    msg_buf_len = len(msg_buf)

    pb_buf_len = struct.pack('!III', meta_buf_len + msg_buf_len + 8,
                             meta_buf_len, msg_buf_len)
    msg_buf = 'PB' + pb_buf_len + meta_buf + msg_buf
    return msg_buf


def parse_message(buf):
    if len(buf) < 8:
        return None

    (meta_len, pb_msg_len) = struct.unpack('!II', buf[:8])
    if len(buf) < 8 + meta_len + pb_msg_len:
        return None

    meta_msg_buf = buf[8:8 + meta_len]
    meta_info = simple_pb2.MetaInfo()
    try:
        meta_info.ParseFromString(meta_msg_buf)
    except simple_pb2.message.DecodeError:
        print 'parsing msg meta info failed'
        return None

    msg_cls = getattr(simple_pb2, meta_info.msg_name)
    if msg_cls is None:
        return None

    msg = msg_cls()
    try:
        msg.ParseFromString(buf)
        return msg
    except simple_pb2.message.DecodeError:
        return None
