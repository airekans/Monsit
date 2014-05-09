# Generated by the protocol buffer compiler.  DO NOT EDIT!

from google.protobuf import descriptor
from google.protobuf import message
from google.protobuf import reflection
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)



DESCRIPTOR = descriptor.FileDescriptor(
  name='rpc_meta.proto',
  package='rpc',
  serialized_pb='\n\x0erpc_meta.proto\x12\x03rpc\"k\n\x08MetaInfo\x12\x0f\n\x07\x66low_id\x18\x01 \x02(\x05\x12\x14\n\x0cservice_name\x18\x02 \x02(\t\x12\x13\n\x0bmethod_name\x18\x03 \x02(\t\x12\x10\n\x08msg_name\x18\x04 \x02(\t\x12\x11\n\thas_error\x18\x1e \x01(\x08\"2\n\rErrorResponse\x12\x10\n\x08\x65rr_code\x18\x01 \x02(\x05\x12\x0f\n\x07\x65rr_msg\x18\x02 \x02(\t*2\n\tErrorCode\x12\x0b\n\x07SUCCESS\x10\x00\x12\x18\n\x14SERVER_SERVICE_ERROR\x10\x01')

_ERRORCODE = descriptor.EnumDescriptor(
  name='ErrorCode',
  full_name='rpc.ErrorCode',
  filename=None,
  file=DESCRIPTOR,
  values=[
    descriptor.EnumValueDescriptor(
      name='SUCCESS', index=0, number=0,
      options=None,
      type=None),
    descriptor.EnumValueDescriptor(
      name='SERVER_SERVICE_ERROR', index=1, number=1,
      options=None,
      type=None),
  ],
  containing_type=None,
  options=None,
  serialized_start=184,
  serialized_end=234,
)


SUCCESS = 0
SERVER_SERVICE_ERROR = 1



_METAINFO = descriptor.Descriptor(
  name='MetaInfo',
  full_name='rpc.MetaInfo',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='flow_id', full_name='rpc.MetaInfo.flow_id', index=0,
      number=1, type=5, cpp_type=1, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='service_name', full_name='rpc.MetaInfo.service_name', index=1,
      number=2, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='method_name', full_name='rpc.MetaInfo.method_name', index=2,
      number=3, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='msg_name', full_name='rpc.MetaInfo.msg_name', index=3,
      number=4, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='has_error', full_name='rpc.MetaInfo.has_error', index=4,
      number=30, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=23,
  serialized_end=130,
)


_ERRORRESPONSE = descriptor.Descriptor(
  name='ErrorResponse',
  full_name='rpc.ErrorResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='err_code', full_name='rpc.ErrorResponse.err_code', index=0,
      number=1, type=5, cpp_type=1, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='err_msg', full_name='rpc.ErrorResponse.err_msg', index=1,
      number=2, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=132,
  serialized_end=182,
)

DESCRIPTOR.message_types_by_name['MetaInfo'] = _METAINFO
DESCRIPTOR.message_types_by_name['ErrorResponse'] = _ERRORRESPONSE

class MetaInfo(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _METAINFO
  
  # @@protoc_insertion_point(class_scope:rpc.MetaInfo)

class ErrorResponse(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _ERRORRESPONSE
  
  # @@protoc_insertion_point(class_scope:rpc.ErrorResponse)

# @@protoc_insertion_point(module_scope)
