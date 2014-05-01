# Generated by the protocol buffer compiler.  DO NOT EDIT!

from google.protobuf import descriptor
from google.protobuf import message
from google.protobuf import reflection
from google.protobuf import service
from google.protobuf import service_reflection
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)



DESCRIPTOR = descriptor.FileDescriptor(
  name='monsit.proto',
  package='',
  serialized_pb='\n\x0cmonsit.proto\"\x91\x01\n\x07\x43PUInfo\x12\x0c\n\x04name\x18\x01 \x02(\t\x12\x12\n\nuser_count\x18\x02 \x02(\x05\x12\x12\n\nnice_count\x18\x03 \x02(\x05\x12\x11\n\tsys_count\x18\x04 \x02(\x05\x12\x12\n\nidle_count\x18\x05 \x02(\x05\x12\x14\n\x0ciowait_count\x18\x06 \x02(\x05\x12\x13\n\x0btotal_count\x18\x07 \x02(\x05\"I\n\x07NetInfo\x12\x0c\n\x04name\x18\x01 \x02(\t\x12\n\n\x02ip\x18\x02 \x02(\t\x12\x11\n\trecv_byte\x18\x03 \x02(\x03\x12\x11\n\tsend_byte\x18\x04 \x02(\x03\"Q\n\x0eVirtualMemInfo\x12\r\n\x05total\x18\x01 \x02(\x03\x12\x11\n\tavailable\x18\x02 \x02(\x03\x12\x0c\n\x04used\x18\x03 \x02(\x03\x12\x0f\n\x07percent\x18\x04 \x02(\x05\"I\n\x0bSwapMemInfo\x12\r\n\x05total\x18\x01 \x02(\x03\x12\x0c\n\x04\x66ree\x18\x02 \x02(\x03\x12\x0c\n\x04used\x18\x03 \x02(\x03\x12\x0f\n\x07percent\x18\x04 \x02(\x05\"O\n\x07MemInfo\x12$\n\x0bvirtual_mem\x18\x01 \x02(\x0b\x32\x0f.VirtualMemInfo\x12\x1e\n\x08swap_mem\x18\x02 \x02(\x0b\x32\x0c.SwapMemInfo\"\x8a\x01\n\rReportRequest\x12\x11\n\thost_name\x18\x01 \x02(\t\x12\x10\n\x08\x64\x61tetime\x18\x02 \x02(\x03\x12\x1b\n\tcpu_infos\x18\x03 \x03(\x0b\x32\x08.CPUInfo\x12\x1b\n\tnet_infos\x18\x04 \x03(\x0b\x32\x08.NetInfo\x12\x1a\n\x08mem_info\x18\x05 \x02(\x0b\x32\x08.MemInfo\"2\n\x0eReportResponse\x12\x13\n\x0breturn_code\x18\x01 \x02(\x05\x12\x0b\n\x03msg\x18\x02 \x02(\t\"$\n\x0fRegisterRequest\x12\x11\n\thost_name\x18\x01 \x02(\t\"4\n\x10RegisterResponse\x12\x13\n\x0breturn_code\x18\x01 \x02(\x05\x12\x0b\n\x03msg\x18\x02 \x02(\t2k\n\rMonsitService\x12/\n\x08Register\x12\x10.RegisterRequest\x1a\x11.RegisterResponse\x12)\n\x06Report\x12\x0e.ReportRequest\x1a\x0f.ReportResponseB\x03\x90\x01\x01')




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
  serialized_start=17,
  serialized_end=162,
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
  serialized_start=164,
  serialized_end=237,
)


_VIRTUALMEMINFO = descriptor.Descriptor(
  name='VirtualMemInfo',
  full_name='VirtualMemInfo',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='total', full_name='VirtualMemInfo.total', index=0,
      number=1, type=3, cpp_type=2, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='available', full_name='VirtualMemInfo.available', index=1,
      number=2, type=3, cpp_type=2, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='used', full_name='VirtualMemInfo.used', index=2,
      number=3, type=3, cpp_type=2, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='percent', full_name='VirtualMemInfo.percent', index=3,
      number=4, type=5, cpp_type=1, label=2,
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
  serialized_start=239,
  serialized_end=320,
)


_SWAPMEMINFO = descriptor.Descriptor(
  name='SwapMemInfo',
  full_name='SwapMemInfo',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='total', full_name='SwapMemInfo.total', index=0,
      number=1, type=3, cpp_type=2, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='free', full_name='SwapMemInfo.free', index=1,
      number=2, type=3, cpp_type=2, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='used', full_name='SwapMemInfo.used', index=2,
      number=3, type=3, cpp_type=2, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='percent', full_name='SwapMemInfo.percent', index=3,
      number=4, type=5, cpp_type=1, label=2,
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
  serialized_start=322,
  serialized_end=395,
)


_MEMINFO = descriptor.Descriptor(
  name='MemInfo',
  full_name='MemInfo',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='virtual_mem', full_name='MemInfo.virtual_mem', index=0,
      number=1, type=11, cpp_type=10, label=2,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='swap_mem', full_name='MemInfo.swap_mem', index=1,
      number=2, type=11, cpp_type=10, label=2,
      has_default_value=False, default_value=None,
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
  serialized_start=397,
  serialized_end=476,
)


_REPORTREQUEST = descriptor.Descriptor(
  name='ReportRequest',
  full_name='ReportRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='host_name', full_name='ReportRequest.host_name', index=0,
      number=1, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='datetime', full_name='ReportRequest.datetime', index=1,
      number=2, type=3, cpp_type=2, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='cpu_infos', full_name='ReportRequest.cpu_infos', index=2,
      number=3, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='net_infos', full_name='ReportRequest.net_infos', index=3,
      number=4, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='mem_info', full_name='ReportRequest.mem_info', index=4,
      number=5, type=11, cpp_type=10, label=2,
      has_default_value=False, default_value=None,
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
  serialized_start=479,
  serialized_end=617,
)


_REPORTRESPONSE = descriptor.Descriptor(
  name='ReportResponse',
  full_name='ReportResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='return_code', full_name='ReportResponse.return_code', index=0,
      number=1, type=5, cpp_type=1, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='msg', full_name='ReportResponse.msg', index=1,
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
  serialized_start=619,
  serialized_end=669,
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
  serialized_start=671,
  serialized_end=707,
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
  serialized_start=709,
  serialized_end=761,
)

_MEMINFO.fields_by_name['virtual_mem'].message_type = _VIRTUALMEMINFO
_MEMINFO.fields_by_name['swap_mem'].message_type = _SWAPMEMINFO
_REPORTREQUEST.fields_by_name['cpu_infos'].message_type = _CPUINFO
_REPORTREQUEST.fields_by_name['net_infos'].message_type = _NETINFO
_REPORTREQUEST.fields_by_name['mem_info'].message_type = _MEMINFO
DESCRIPTOR.message_types_by_name['CPUInfo'] = _CPUINFO
DESCRIPTOR.message_types_by_name['NetInfo'] = _NETINFO
DESCRIPTOR.message_types_by_name['VirtualMemInfo'] = _VIRTUALMEMINFO
DESCRIPTOR.message_types_by_name['SwapMemInfo'] = _SWAPMEMINFO
DESCRIPTOR.message_types_by_name['MemInfo'] = _MEMINFO
DESCRIPTOR.message_types_by_name['ReportRequest'] = _REPORTREQUEST
DESCRIPTOR.message_types_by_name['ReportResponse'] = _REPORTRESPONSE
DESCRIPTOR.message_types_by_name['RegisterRequest'] = _REGISTERREQUEST
DESCRIPTOR.message_types_by_name['RegisterResponse'] = _REGISTERRESPONSE

class CPUInfo(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _CPUINFO
  
  # @@protoc_insertion_point(class_scope:CPUInfo)

class NetInfo(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _NETINFO
  
  # @@protoc_insertion_point(class_scope:NetInfo)

class VirtualMemInfo(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _VIRTUALMEMINFO
  
  # @@protoc_insertion_point(class_scope:VirtualMemInfo)

class SwapMemInfo(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _SWAPMEMINFO
  
  # @@protoc_insertion_point(class_scope:SwapMemInfo)

class MemInfo(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _MEMINFO
  
  # @@protoc_insertion_point(class_scope:MemInfo)

class ReportRequest(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _REPORTREQUEST
  
  # @@protoc_insertion_point(class_scope:ReportRequest)

class ReportResponse(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _REPORTRESPONSE
  
  # @@protoc_insertion_point(class_scope:ReportResponse)

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
  serialized_start=763,
  serialized_end=870,
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
    input_type=_REPORTREQUEST,
    output_type=_REPORTRESPONSE,
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