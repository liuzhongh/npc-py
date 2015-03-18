# -*- coding: UTF-8 -*-
#-*- author: Liuzh -*-
import io
import logging
import os
import socket
import struct

from avro import io as avro_io
from avro import schema

# ------------------------------------------------------------------------------
# Constants
from avro.schema import Error, AvroException


def LoadResource(name):
  dir_path = os.path.dirname(__file__)
  rsrc_path = os.path.join(dir_path, name)
  with open(rsrc_path, 'r') as f:
    return f.read()


# Handshake schema is pulled in during build
HANDSHAKE_REQUEST_SCHEMA_JSON = LoadResource('NHandshakeRequest.avsc')
HANDSHAKE_RESPONSE_SCHEMA_JSON = LoadResource('NHandshakeResponse.avsc')

HANDSHAKE_REQUEST_SCHEMA = schema.Parse(HANDSHAKE_REQUEST_SCHEMA_JSON)
HANDSHAKE_RESPONSE_SCHEMA = schema.Parse(HANDSHAKE_RESPONSE_SCHEMA_JSON)

HANDSHAKE_REQUESTOR_WRITER = avro_io.DatumWriter(HANDSHAKE_REQUEST_SCHEMA)
HANDSHAKE_REQUESTOR_READER = avro_io.DatumReader(HANDSHAKE_RESPONSE_SCHEMA)
HANDSHAKE_RESPONDER_WRITER = avro_io.DatumWriter(HANDSHAKE_RESPONSE_SCHEMA)
HANDSHAKE_RESPONDER_READER = avro_io.DatumReader(HANDSHAKE_REQUEST_SCHEMA)

META_SCHEMA = schema.Parse('{"type": "map", "values": "bytes"}')
META_WRITER = avro_io.DatumWriter(META_SCHEMA)
META_READER = avro_io.DatumReader(META_SCHEMA)

SYSTEM_ERROR_SCHEMA = schema.Parse('["string"]')

AVRO_RPC_MIME = 'avro/binary'

# protocol cache

# Map: remote name -> remote MD5 hash
_REMOTE_HASHES = {}

# Decoder/encoder for a 32 bits big-endian integer.
UINT32_BE = avro_io.STRUCT_INT

# Default size of the buffers use to frame messages:
BUFFER_SIZE = 8192

# ------------------------------------------------------------------------------
# Exceptions


class AvroRemoteException(schema.AvroException):
  """
  Raised when an error message is sent by an Avro requestor or responder.
  """
  def __init__(self, fail_msg=None):
    schema.AvroException.__init__(self, fail_msg)

class ConnectionClosedException(schema.AvroException):
  pass

class NettyClient():
    def __init__(self, host, port, timeout=socket._GLOBAL_DEFAULT_TIMEOUT):
        self._host = host
        self._port = port
        # self._conn = socket.create_connection((host, port))
        self._remote_name = property(lambda self: self._sock.getsockname())

    def getConnection(self):
        self._conn = socket.create_connection((self._host, self._port))
        return self._conn

    @property
    def remote_name(self):
        return self._remote_name

    def close(self):
        self._conn.shutdown(socket.SHUT_RDWR)
        self._conn.close()
        self._conn = None

class ClientRequestor():

    def getClient(self, client, service_name, method_name, protocol, args={}, id=1, timeout=50000, protocol_type=1):
        if service_name is None:
            raise AvroException("Instance name can't be null!")
        if method_name is None:
            raise AvroException("Method name can't be null!")
        if protocol is None:
            raise AvroException("Protocol can't be null!")

        self._conn = client.getConnection()
        self._local_protocol = protocol

        # build handshake and call request
        buffer_writer = io.BytesIO()
        buffer_encoder = avro_io.BinaryEncoder(buffer_writer)
        self._WriteHandshakeRequest(buffer_encoder, service_name, method_name, id, timeout, protocol_type)
        self._WriteCallRequest(method_name, args, buffer_encoder)

        # send the handshake and call request; block until call response
        call_request = buffer_writer.getvalue()

        self.WriteMessage(call_request)
        result = self.ReadMessage()

        # process the handshake and call response
        buffer_decoder = avro_io.BinaryDecoder(io.BytesIO(result))
        call_response_exists = self._ReadHandshakeResponse(buffer_decoder)
        if call_response_exists:
          return self._ReadCallResponse(method_name, buffer_decoder)

        return None

    def _ReadHandshakeResponse(self, decoder):
        """Reads and processes the handshake response message.

        Args:
          decoder: Decoder to read messages from.
        Returns:
          call-response exists (boolean) ???
        Raises:
          schema.AvroException on ???
        """
        logging.info('Processing handshake response: %s', decoder)
        handshake_response = HANDSHAKE_REQUESTOR_READER.read(decoder)
        logging.info('Processing handshake response: %s', handshake_response)

        return True

    def _ReadCallResponse(self, message_name, decoder):
        """Reads and processes a method call response.

        The format of a call response is:
          - response metadata, a map with values of type bytes
          - a one-byte error flag boolean, followed by either:
            - if the error flag is false,
              the message response, serialized per the message's response schema.
            - if the error flag is true,
              the error, serialized per the message's error union schema.

        Args:
          message_name:
          decoder:
        Returns:
          ???
        Raises:
          schema.AvroException on ???
        """
        # response metadata
        # response_metadata = META_READER.read(decoder)

        # local response schema
        local_message_schema = self._local_protocol.message_map.get(message_name)
        if local_message_schema is None:
          raise schema.AvroException('Unknown local message: %s' % message_name)

        # error flag
        if not decoder.read_boolean():
          reader_schema = local_message_schema.response
          return self._ReadResponse(reader_schema, reader_schema, decoder)
        else:
          reader_schema = local_message_schema.errors
          raise self._ReadError(reader_schema, reader_schema, decoder)

    def _ReadResponse(self, writer_schema, reader_schema, decoder):
        datum_reader = avro_io.DatumReader(writer_schema, reader_schema)
        result = datum_reader.read(decoder)
        return result

    def _ReadError(self, writer_schema, reader_schema, decoder):
        datum_reader = avro_io.DatumReader(writer_schema, reader_schema)
        error = datum_reader.read(decoder)
        if error is not None:
            error = error.split(": ")[1]
        return AvroRemoteException(error)

    def _WriteHandshakeRequest(self, encoder, service_name, method_name, id, timeout, protocol_type):
        """Emits the handshake request.

        Args:
          encoder: Encoder to write the handshake request into.
        """

        request_datum = {
          'id': id,
          'timeout': timeout,
          'protocolType': protocol_type,
          'targetInstanceName': service_name,
          'methodName': method_name,
        }

        logging.info('Sending handshake request: %s', request_datum)
        HANDSHAKE_REQUESTOR_WRITER.write(request_datum, encoder)

    def _WriteCallRequest(self, message_name, request_datum, encoder):
        """
        The format of a call request is:
          * request metadata, a map with values of type bytes
          * the message name, an Avro string, followed by
          * the message parameters. Parameters are serialized according to
            the message's request declaration.
        """
        # request metadata (not yet implemented)
        # request_metadata = {}
        # META_WRITER.write(request_metadata, encoder)

        # Identify message to send:
        message = self._local_protocol.message_map.get(message_name)
        if message is None:
          raise schema.AvroException('Unknown message: %s' % message_name)

        # message parameters
        self._WriteRequest(message.request, request_datum, encoder)

    def _WriteRequest(self, request_schema, request_datum, encoder):
        logging.info('writing request: %s', request_datum)
        datum_writer = avro_io.DatumWriter(request_schema)
        datum_writer.write(request_datum, encoder)

    def ReadMessage(self):
      heads = self._conn.recv(8)
      c = UINT32_BE.unpack(heads[4:])[0]

      logging.info('Response Head serial: %s, size: %s', str(UINT32_BE.unpack(heads[:4])[0]), str(c))

      msg = io.BytesIO()
      for i in range(0, c):
          buff = io.BytesIO()
          size = self.__read_length()
          while buff.tell() < size:
              chunk = self._conn.recv(size - buff.tell())
              if chunk == b'':
                  raise ConnectionClosedException("socket read 0 bytes")
              buff.write(chunk)
          msg.write(buff.getvalue())
      return msg.getvalue()

    def __read_length(self):
        length = self._conn.recv(4)
        if length == b'':
            raise ConnectionClosedException("socket read 0 bytes")

        return UINT32_BE.unpack(length)[0]

    def WriteMessage(self, message):

        headFmt = '!2I'

        bio = io.BytesIO()
        req_body_buffer = FramedWriter(bio)
        req_body_buffer.Write(message)
        req_body = bio.getvalue()

        self._conn.send(struct.pack(headFmt, 1, 1))
        self._conn.send(req_body)


# ------------------------------------------------------------------------------
# Framed message


class FramedWriter(object):
    """Wrapper around a file-like object to write framed data."""

    def __init__(self, writer):
        self._writer = writer

    def Write(self, message):
        """Writes a message.

        Message is chunked into sequences of frames terminated by an empty frame.

        Args:
          message: Message to write, as bytes.
        """
        while len(message) > 0:
            chunk_size = max(BUFFER_SIZE, len(message))
            chunk = message[:chunk_size]
            self._WriteBuffer(chunk)
            message = message[chunk_size:]

        # A message is always terminated by a zero-length buffer.
        self._WriteUnsignedInt32(0)

    def _WriteBuffer(self, chunk):
        self._WriteUnsignedInt32(len(chunk))
        self._writer.write(chunk)

    def _WriteUnsignedInt32(self, uint32):
        self._writer.write(UINT32_BE.pack(uint32))