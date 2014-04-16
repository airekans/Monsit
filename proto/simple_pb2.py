# Generated by the protocol buffer compiler.  DO NOT EDIT!

from google.protobuf import descriptor
from google.protobuf import message
from google.protobuf import reflection
from google.protobuf import service
from google.protobuf import service_reflection
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)



DESCRIPTOR = descriptor.FileDescriptor(
  name='simple.proto',
  package='',
  serialized_pb='\n\x0csimple.proto\"G\n\x08MetaInfo\x12\x14\n\x0cservice_name\x18\x01 \x02(\t\x12\x13\n\x0bmethod_name\x18\x02 \x02(\t\x12\x10\n\x08msg_name\x18\x03 \x02(\t\"\x91\x01\n\x07\x43PUInfo\x12\x0c\n\x04name\x18\x01 \x02(\t\x12\x12\n\nuser_count\x18\x02 \x02(\x05\x12\x12\n\nnice_count\x18\x03 \x02(\x05\x12\x11\n\tsys_count\x18\x04 \x02(\x05\x12\x12\n\nidle_count\x18\x05 \x02(\x05\x12\x14\n\x0ciowait_count\x18\x06 \x02(\x05\x12\x13\n\x0btotal_count\x18\x07 \x02(\x05\"I\n\x07NetInfo\x12\x0c\n\x04name\x18\x01 \x02(\t\x12\n\n\x02ip\x18\x02 \x02(\t\x12\x11\n\trecv_byte\x18\x03 \x02(\x03\x12\x11\n\tsend_byte\x18\x04 \x02(\x03\"n\n\rSimpleRequest\x12\x11\n\thost_name\x18\x01 \x02(\t\x12\x10\n\x08\x64\x61tetime\x18\x02 \x02(\x03\x12\x1b\n\tcpu_infos\x18\x03 \x03(\x0b\x32\x08.CPUInfo\x12\x1b\n\tnet_infos\x18\x04 \x03(\x0b\x32\x08.NetInfo\"2\n\x0eSimpleResponse\x12\x13\n\x0breturn_code\x18\x01 \x02(\x05\x12\x0b\n\x03msg\x18\x02 \x02(\t\"$\n\x0fRegisterRequest\x12\x11\n\thost_name\x18\x01 \x02(\t\"4\n\x10RegisterResponse\x12\x13\n\x0breturn_code\x18\x01 \x02(\x05\x12\x0b\n\x03msg\x18\x02 \x02(\t2k\n\rMonsitService\x12/\n\x08Register\x12\x10.RegisterRequest\x1a\x11.RegisterResponse\x12)\n\x06Report\x12\x0e.SimpleRequest\x1a\x0f.SimpleResponseB\x03\x90\x01\x01')




_METAINFO = descriptor.Descriptor(
  name='MetaInfo',
  full_name='MetaInfo',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='service_name', full_name='MetaInfo.service_name', index=0,
      number=1, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='method_name', full_name='MetaInfo.method_name', index=1,
      number=2, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='msg_name', full_name='MetaInfo.msg_name', index=2,
      number=3, type=9, cpp_type=9, label=2,
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
  serialized_start=16,
  serialized_end=87,
)


_CPUINFO = descriptor.Descriptor(
  name='CPUInfo',
  full_name='CPUInfo',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='name', full_name='CPUInfo.name', index=0,
      number=1, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='user_count', full_name='CPUInfo.user_count', index=1,
      number=2, type=5, cpp_type=1, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='nice_count', full_name='CPUInfo.nice_count', index=2,
      number=3, type=5, cpp_type=1, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='sys_count', full_name='CPUInfo.sys_count', index=3,
      number=4, type=5, cpp_type=1, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='idle_count', full_name='CPUInfo.idle_count', index=4,
      number=5, type=5, cpp_type=1, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='iowait_count', full_name='CPUInfo.iowait_count', index=5,
      number=6, type=5, cpp_type=1, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='total_count', full_name='CPUInfo.total_count', index=6,
      number=7, type=5, cpp_type=1, label=2,
      has_default_value=False, default_value=0,
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
  serialized_start=90,
  serialized_end=235,
)


_NETINFO = descriptor.Descriptor(
  name='NetInfo',
  full_name='NetInfo',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='name', full_name='NetInfo.name', index=0,
      number=1, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='ip', full_name='NetInfo.ip', index=1,
      number=2, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='recv_byte', full_name='NetInfo.recv_byte', index=2,
      number=3, type=3, cpp_type=2, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='send_byte', full_name='NetInfo.send_byte', index=3,
      number=4, type=3, cpp_type=2, label=2,
      has_default_value=False, default_value=0,
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
  serialized_start=237,
  serialized_end=310,
)


_SIMPLEREQUEST = descriptor.Descriptor(
  name='SimpleRequest',
  full_name='SimpleRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='host_name', full_name='SimpleRequest.host_name', index=0,
      number=1, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='datetime', full_name='SimpleRequest.datetime', index=1,
      number=2, type=3, cpp_type=2, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='cpu_infos', full_name='SimpleRequest.cpu_infos', index=2,
      number=3, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='net_infos', full_name='SimpleRequest.net_infos', index=3,
      number=4, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
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
  serialized_start=312,
  serialized_end=422,
)


_SIMPLERESPONSE = descriptor.Descriptor(
  name='SimpleResponse',
  full_name='SimpleResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='return_code', full_name='SimpleResponse.return_code', index=0,
      number=1, type=5, cpp_type=1, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='msg', full_name='SimpleResponse.msg', index=1,
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
  serialized_start=424,
  serialized_end=474,
)


_REGISTERREQUEST = descriptor.Descriptor(
  name='RegisterRequest',
  full_name='RegisterRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='host_name', full_name='RegisterRequest.host_name', index=0,
      number=1, type=9, cpp_type=9, label=2,
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
  serialized_start=476,
  serialized_end=512,
)


_REGISTERRESPONSE = descriptor.Descriptor(
  name='RegisterResponse',
  full_name='RegisterResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='return_code', full_name='RegisterResponse.return_code', index=0,
      number=1, type=5, cpp_type=1, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='msg', full_name='RegisterResponse.msg', index=1,
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
  serialized_start=514,
  serialized_end=566,
)

_SIMPLEREQUEST.fields_by_name['cpu_infos'].message_type = _CPUINFO
_SIMPLEREQUEST.fields_by_name['net_infos'].message_type = _NETINFO
DESCRIPTOR.message_types_by_name['MetaInfo'] = _METAINFO
DESCRIPTOR.message_types_by_name['CPUInfo'] = _CPUINFO
DESCRIPTOR.message_types_by_name['NetInfo'] = _NETINFO
DESCRIPTOR.message_types_by_name['SimpleRequest'] = _SIMPLEREQUEST
DESCRIPTOR.message_types_by_name['SimpleResponse'] = _SIMPLERESPONSE
DESCRIPTOR.message_types_by_name['RegisterRequest'] = _REGISTERREQUEST
DESCRIPTOR.message_types_by_name['RegisterResponse'] = _REGISTERRESPONSE

class MetaInfo(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _METAINFO
  
  # @@protoc_insertion_point(class_scope:MetaInfo)

class CPUInfo(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _CPUINFO
  
  # @@protoc_insertion_point(class_scope:CPUInfo)

class NetInfo(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _NETINFO
  
  # @@protoc_insertion_point(class_scope:NetInfo)

class SimpleRequest(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _SIMPLEREQUEST
  
  # @@protoc_insertion_point(class_scope:SimpleRequest)

class SimpleResponse(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _SIMPLERESPONSE
  
  # @@protoc_insertion_point(class_scope:SimpleResponse)

class RegisterRequest(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _REGISTERREQUEST
  
  # @@protoc_insertion_point(class_scope:RegisterRequest)

class RegisterResponse(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _REGISTERRESPONSE
  
  # @@protoc_insertion_point(class_scope:RegisterResponse)


_MONSITSERVICE = descriptor.ServiceDescriptor(
  name='MonsitService',
  full_name='MonsitService',
  file=DESCRIPTOR,
  index=0,
  options=None,
  serialized_start=568,
  serialized_end=675,
  methods=[
  descriptor.MethodDescriptor(
    name='Register',
    full_name='MonsitService.Register',
    index=0,
    containing_service=None,
    input_type=_REGISTERREQUEST,
    output_type=_REGISTERRESPONSE,
    options=None,
  ),
  descriptor.MethodDescriptor(
    name='Report',
    full_name='MonsitService.Report',
    index=1,
    containing_service=None,
    input_type=_SIMPLEREQUEST,
    output_type=_SIMPLERESPONSE,
    options=None,
  ),
])

class MonsitService(service.Service):
  __metaclass__ = service_reflection.GeneratedServiceType
  DESCRIPTOR = _MONSITSERVICE
class MonsitService_Stub(MonsitService):
  __metaclass__ = service_reflection.GeneratedServiceStubType
  DESCRIPTOR = _MONSITSERVICE

# @@protoc_insertion_point(module_scope)
