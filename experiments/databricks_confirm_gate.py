"""Owned one-shot unary gRPC transport gate; forwards original protobuf bytes.

Only a caller-proven owned Confirm is held. Its bounded deadline always releases
forwarding, including on controller failure. Application metadata/status and raw
payloads are retained separately from the ordinary certified recording proxy.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import threading
import time

SUBMIT = '/com.fivetran.query_cache.SQL/SubmitEnrichedSQL'
CONFIRM = '/com.fivetran.query_cache.Execution/ConfirmExecution'


class UnaryGate:
    def __init__(self, upstream, directory, ownership, timeout=45):
        import grpc
        self.grpc=grpc;self.directory=Path(directory);self.directory.mkdir()
        self.ownership=ownership;self.timeout=timeout
        self.armed=False;self.used=False;self.lock=threading.Lock()
        self.response_armed=False;self.response_used=False
        self.response_arrived=threading.Event();self.response_release=threading.Event()
        self.arrived=threading.Event();self.release=threading.Event()
        self.channel=grpc.insecure_channel(upstream)
        self.server=grpc.server(ThreadPoolExecutor(max_workers=8))
        gate=self
        class Handler(grpc.GenericRpcHandler):
            def service(self,details):
                return grpc.unary_unary_rpc_method_handler(
                    lambda request,context:gate.forward(details.method,request,context),
                    request_deserializer=lambda value:value,response_serializer=lambda value:value)
        self.server.add_generic_rpc_handlers((Handler(),))
        port=self.server.add_insecure_port('127.0.0.1:0')
        if not port:raise RuntimeError('gate failed to bind owned loopback port')
        self.endpoint=f'127.0.0.1:{port}';self.server.start()

    def event(self,name,**values):
        (self.directory/(name+'.json')).write_text(json.dumps({'at':datetime.now(timezone.utc).isoformat(),
                   'monotonic':time.monotonic(),**values},indent=2))

    def forward(self,method,request,context):
        hold=False
        if method==CONFIRM and self.ownership(method,request,None):
            with self.lock:
                if self.armed and not self.used:self.used=True;hold=True
        if hold:
            (self.directory/'confirm.request.pb').write_bytes(request)
            self.event('received',request_sha256=hashlib.sha256(request).hexdigest())
            self.arrived.set()
            released=self.release.wait(self.timeout)
            self.event('forwarded',controller_released=released,
                       request_sha256=hashlib.sha256(request).hexdigest())
        try:
            result,call=self.channel.unary_unary(method).with_call(
                request,metadata=tuple(context.invocation_metadata()),timeout=min(context.time_remaining() or 120,120))
            owned=self.ownership(method,request,result)
            hold_response=False
            if method==SUBMIT and owned:
                with self.lock:
                    if self.response_armed and not self.response_used:
                        self.response_used=True;hold_response=True
            if hold_response:
                (self.directory/'submit.request.pb').write_bytes(request)
                (self.directory/'submit.response.pb').write_bytes(result)
                self.event('response-received',request_sha256=hashlib.sha256(request).hexdigest(),
                           response_sha256=hashlib.sha256(result).hexdigest())
                self.response_arrived.set()
                released=self.response_release.wait(self.timeout)
                self.event('response-forwarded',controller_released=released,
                           response_sha256=hashlib.sha256(result).hexdigest())
            context.send_initial_metadata(call.initial_metadata());context.set_trailing_metadata(call.trailing_metadata())
            if hold:
                (self.directory/'confirm.response.pb').write_bytes(result)
                self.event('responded',response_sha256=hashlib.sha256(result).hexdigest(),status='OK')
            return result
        except self.grpc.RpcError as error:
            context.send_initial_metadata(error.initial_metadata() or ())
            context.set_code(error.code());context.set_details(error.details())
            context.set_trailing_metadata(error.trailing_metadata() or ())
            if hold:self.event('responded',status=error.code().name,details=error.details())
            return b''

    def close(self):
        self.release.set();self.response_release.set();self.server.stop(5).wait(10);self.channel.close()
