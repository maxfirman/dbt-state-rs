"""Real local gRPC transport tests; no warehouse/State credentials."""
from concurrent.futures import ThreadPoolExecutor
import tempfile
import threading
import unittest
import grpc
from databricks_confirm_gate import CONFIRM,UnaryGate

class GateTransport(unittest.TestCase):
    def test_only_owned_confirm_is_held_once_and_bytes_are_preserved(self):
        server=grpc.server(ThreadPoolExecutor(max_workers=4));received=[]
        def forward(value,context):
            received.append(value);context.send_initial_metadata((('x-app','yes'),))
            context.set_trailing_metadata((('x-tail','yes'),));return value+b' response'
        server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler('com.fivetran.query_cache.Execution',
            {'ConfirmExecution':grpc.unary_unary_rpc_method_handler(forward)}),))
        port=server.add_insecure_port('127.0.0.1:0');server.start()
        with tempfile.TemporaryDirectory() as root:
            gate=UnaryGate(f'127.0.0.1:{port}',root+'/gate',lambda method,request,response:request==b'owned',timeout=2)
            channel=grpc.insecure_channel(gate.endpoint);rpc=channel.unary_unary(CONFIRM);gate.armed=True
            self.assertEqual(rpc(b'other'),b'other response');self.assertFalse(gate.used)
            result=[]
            thread=threading.Thread(target=lambda:result.append(rpc.with_call(b'owned')));thread.start()
            self.assertTrue(gate.arrived.wait(2));self.assertEqual(received,[b'other'])
            gate.release.set();thread.join(3);self.assertFalse(thread.is_alive())
            self.assertEqual(result[0][0],b'owned response')
            self.assertIn(('x-app','yes'),result[0][1].initial_metadata())
            self.assertIn(('x-tail','yes'),result[0][1].trailing_metadata())
            self.assertEqual(rpc(b'owned'),b'owned response')
            gate.close();channel.close()
        server.stop(0).wait()

    def test_controller_timeout_releases_unmodified_confirmation(self):
        server=grpc.server(ThreadPoolExecutor(max_workers=2))
        server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler('com.fivetran.query_cache.Execution',
            {'ConfirmExecution':grpc.unary_unary_rpc_method_handler(lambda value,context:value)}),))
        port=server.add_insecure_port('127.0.0.1:0');server.start()
        with tempfile.TemporaryDirectory() as root:
            gate=UnaryGate(f'127.0.0.1:{port}',root+'/gate',lambda *args:True,timeout=.1)
            gate.armed=True;channel=grpc.insecure_channel(gate.endpoint)
            self.assertEqual(channel.unary_unary(CONFIRM)(b'owned'),b'owned')
            import json
            from pathlib import Path
            self.assertFalse(json.loads(Path(root+'/gate/forwarded.json').read_text())['controller_released'])
            gate.close();channel.close()
        server.stop(0).wait()

    def test_upstream_error_status_and_application_metadata_are_preserved(self):
        server=grpc.server(ThreadPoolExecutor(max_workers=2))
        def rejected(value,context):
            context.send_initial_metadata((('x-error-initial','owned'),))
            context.set_trailing_metadata((('x-rejection','owned'),))
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,'exact hosted detail')
        server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler('com.fivetran.query_cache.Execution',
            {'ConfirmExecution':grpc.unary_unary_rpc_method_handler(rejected)}),))
        port=server.add_insecure_port('127.0.0.1:0');server.start()
        with tempfile.TemporaryDirectory() as root:
            gate=UnaryGate(f'127.0.0.1:{port}',root+'/gate',lambda *args:False)
            channel=grpc.insecure_channel(gate.endpoint)
            with self.assertRaises(grpc.RpcError) as observed:channel.unary_unary(CONFIRM)(b'owned')
            self.assertEqual(observed.exception.code(),grpc.StatusCode.INVALID_ARGUMENT)
            self.assertEqual(observed.exception.details(),'exact hosted detail')
            self.assertIn(('x-error-initial','owned'),observed.exception.initial_metadata())
            self.assertIn(('x-rejection','owned'),observed.exception.trailing_metadata())
            gate.close();channel.close()
        server.stop(0).wait()

    def test_owned_submit_response_is_held_without_changing_bytes_before_confirm_stage(self):
        from databricks_confirm_gate import SUBMIT
        server=grpc.server(ThreadPoolExecutor(max_workers=4));seen=[]
        def forward(value,context):
            seen.append(value);context.send_initial_metadata((('x-app','ready'),))
            return value+b' response'
        server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler('com.fivetran.query_cache.SQL',
            {'SubmitEnrichedSQL':grpc.unary_unary_rpc_method_handler(forward)}),))
        port=server.add_insecure_port('127.0.0.1:0');server.start()
        with tempfile.TemporaryDirectory() as root:
            gate=UnaryGate(f'127.0.0.1:{port}',root+'/gate',lambda method,request,response:request==b'owned',timeout=2)
            gate.response_armed=True;channel=grpc.insecure_channel(gate.endpoint);rpc=channel.unary_unary(SUBMIT)
            self.assertEqual(rpc(b'other'),b'other response');self.assertFalse(gate.response_used)
            results=[];thread=threading.Thread(target=lambda:results.append(rpc.with_call(b'owned')));thread.start()
            self.assertTrue(gate.response_arrived.wait(2));self.assertEqual(seen,[b'other',b'owned'])
            self.assertEqual(results,[])
            gate.response_release.set();thread.join(3);self.assertFalse(thread.is_alive())
            self.assertEqual(results[0][0],b'owned response')
            self.assertIn(('x-app','ready'),results[0][1].initial_metadata())
            self.assertEqual(rpc(b'owned'),b'owned response')
            self.assertFalse(gate.used)
            gate.close();channel.close()
        server.stop(0).wait()

if __name__=='__main__':unittest.main()
