package kaggle

import (
	"context"
	"strings"
	"testing"
)

func TestFiniteSSEReplayRedactionBoundsAndReset(t *testing.T) {
	m := monitorFixture(t)
	c := m.config
	c.PythonExecutable = pythonForTest(t)
	fixture := `
def fixture():
    def response(events):
        data = b''.join(b'data: ' + json.dumps(dict(data=x)).encode() + b'\n\n' for x in events)
        return type('Response', (), {'headers': {'Content-Type':'text/event-stream'}, 'raw':io.BytesIO(data)})()
    first = stream_result(response(['one\n', 'two\n']), {'log_limit':1}, 'RUNNING', 'SYNTHETIC_TOKEN')
    assert base64.b64decode(first['text_b64']) == b'one\n'
    req = dict(log_offset=first['offset'], log_prefix=first['prefix'], log_limit=1)
    second = stream_result(response(['one\n','two\n','three\n']), req, 'RUNNING', 'SYNTHETIC_TOKEN')
    assert base64.b64decode(second['text_b64']) == b'two\n'
    req = dict(log_offset=second['offset'], log_prefix=second['prefix'], log_limit=1)
    idle = stream_result(response(['one\n','two\n']), req, 'RUNNING', 'SYNTHETIC_TOKEN')
    assert idle['offset'] == second['offset'] and idle['prefix'] == second['prefix'] and idle['availability'] == 'live'
    reset = stream_result(response(['short\n']), req, 'RUNNING', 'SYNTHETIC_TOKEN')
    assert reset['status'] == 'reset'
    try:
        stream_result(response(['changed\n','two\n']), req, 'RUNNING', 'SYNTHETIC_TOKEN')
    except core.IdentityMismatch:
        pass
    else:
        raise AssertionError('changed prefix accepted')
    redacted = stream_result(response(['SYNTHETIC_', 'TOKEN token=private\n']), {}, 'RUNNING', 'SYNTHETIC_TOKEN')
    assert base64.b64decode(redacted['text_b64']) == b'[REDACTED] [REDACTED]\n'
    bad = type('Bad', (), {'headers':{'Content-Type':'text/event-stream'}, 'raw':io.BytesIO(b'data: {bad}\n\n')})()
    try:
        stream_result(bad, {}, 'RUNNING', 'SYNTHETIC_TOKEN')
    except ValueError:
        pass
    else:
        raise AssertionError('malformed SSE accepted')
    large = response(['x'*70000+'\n'])
    try:
        stream_result(large, {}, 'RUNNING', 'SYNTHETIC_TOKEN')
    except ValueError:
        pass
    else:
        raise AssertionError('oversized event accepted')
    bounded = stream_result(response(['x'*40000+'\n', 'y'*40000+'\n']), {}, 'RUNNING', 'SYNTHETIC_TOKEN')
    assert bounded['truncated'] and bounded['offset'] == 40001 and len(base64.b64decode(bounded['text_b64'])) == 40001
    continuation = stream_result(response(['x'*40000+'\n','y'*40000+'\n']), dict(log_offset=bounded['offset'],log_prefix=bounded['prefix']), 'RUNNING', 'SYNTHETIC_TOKEN')
    assert base64.b64decode(continuation['text_b64']) == b'y'*40000+b'\n'
    sentinel = type('End', (), {'headers':{'Content-Type':'text/event-stream'},'raw':io.BytesIO(b'data: END_OF_LOG\n\n')})()
    observed = stream_result(sentinel, {}, 'RUNNING', 'SYNTHETIC_TOKEN')
    assert observed['availability'] == 'live' and observed['offset'] == 0
    class Idle:
        def read(self, size):
            raise TimeoutError()
    idle = type('IdleResponse', (), {'headers':{'Content-Type':'text/event-stream'}, 'raw':Idle()})()
    observed = stream_result(idle, {}, 'RUNNING', 'SYNTHETIC_TOKEN')
    assert observed['availability'] == 'live' and observed['offset'] == 0 and not observed['text_b64']
    terminal_idle = stream_result(idle, {}, 'COMPLETE', 'SYNTHETIC_TOKEN')
    assert terminal_idle['availability'] == 'delayed' and not terminal_idle['text_b64']
    terminal_empty = stream_result(response([]), {}, 'COMPLETE', 'SYNTHETIC_TOKEN')
    assert terminal_empty['availability'] == 'after_completion' and not terminal_empty['text_b64']
    completed = type('Blob' , (), {'headers':{'Content-Type':'application/json'}, 'raw':io.BytesIO(json.dumps([dict(data='one\n'),dict(data='two\n')]).encode())})()
    result = stream_result(completed, {'log_limit':1}, 'COMPLETE', 'SYNTHETIC_TOKEN')
    assert result['availability'] == 'after_completion' and base64.b64decode(result['text_b64']) == b'one\n'
    import types
    class Request:
        def __init__(self, method, url, headers):
            self.method, self.url, self.headers = method, url, headers
    class Adapter:
        def send(self, request, **kwargs):
            assert request.method == 'GET'
            assert request.url == 'https://api.kaggle.com/v1/kernels/logs/stream/owner/slug'
            assert kwargs['proxies'] == {} and kwargs['verify'] is True and kwargs['stream'] is True
            return type('Reply', (), {'status_code':200})()
    class Session:
        headers = {'Content-Type':'application/json'}
        def prepare_request(self, request):
            assert 'Content-Type' not in request.headers
            return request
        def get_adapter(self, url):
            return Adapter()
    sys.modules['requests'] = types.SimpleNamespace(Session=Session, Request=Request)
    transport = LogStreamGuard(Session(), 'owner', 'slug')
    transport.open()
    try:
        transport.open()
    except ValueError:
        pass
    else:
        raise AssertionError('duplicate GET permitted')
    class RedirectAdapter:
        def send(self, request, **kwargs):
            return type('Redirect', (), {'status_code':302, 'close':lambda self:None})()
    session = Session()
    session.get_adapter = lambda url:RedirectAdapter()
    try:
        LogStreamGuard(session, 'owner', 'slug').open()
    except ValueError:
        pass
    else:
        raise AssertionError('redirect permitted')
    class SDKRequest:
        pass
    class Client:
        def __init__(self, **kwargs):
            api = types.SimpleNamespace(get_kernel=lambda r:None, get_kernel_session_status=lambda r:None)
            self.kernels = types.SimpleNamespace(kernels_api_client=api)
            self.security = types.SimpleNamespace(oauth_client=types.SimpleNamespace(introspect_token=lambda r:None))
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
    sys.modules['kagglesdk.kaggle_client'] = types.SimpleNamespace(KaggleClient=Client)
    sys.modules['kagglesdk.kaggle_env'] = types.SimpleNamespace(KaggleEnv=types.SimpleNamespace(PROD='prod'))
    sys.modules['kagglesdk.security.types.oauth_service'] = types.SimpleNamespace(IntrospectTokenRequest=SDKRequest)
    sys.modules['kagglesdk.kernels.types.kernels_api_service'] = types.SimpleNamespace(ApiGetAcceleratorQuotaStatisticsRequest=SDKRequest, ApiGetKernelRequest=SDKRequest, ApiGetKernelSessionStatusRequest=SDKRequest)
    class ReadGuard:
        session = None
        def __init__(self, client, mode):
            assert mode == 'read_only'
        def call(self, operation, method, request):
            self.last = dict(active=True) if operation == 'auth' else dict(status='RUNNING')
            return types.SimpleNamespace(active=True, username='owner')
    checked = []
    def check_kernel(raw, request, kernel_id):
        checked.append(kernel_id)
        if len(checked) == 2:
            raise core.IdentityMismatch('changed after stream')
    core.Guard, core.check_kernel = ReadGuard, check_kernel
    result_response = response(['sensitive payload\n'])
    result_response.close = lambda:None
    globals()['LogStreamGuard'] = lambda *args:types.SimpleNamespace(open=lambda:result_response)
    denied = operate(dict(owner='owner', execution=dict(owner='owner',slug='slug',kernel_id='42')), 'SYNTHETIC_TOKEN', 'logs')
    assert denied['status'] == 'invalid' and not denied['text_b64'] and checked == ['42','42']
    return first
main=fixture
`
	source := strings.Replace(monitorProgram(), "\nif __name__ == \"__main__\":", fixture+"\nif __name__ == \"__main__\":", 1)
	result, err := runMonitorSource(context.Background(), c, "logs", nil, monitorRequest{}, source)
	if err != nil || !result.Replay || result.Availability != "live" {
		t.Fatal(result, err)
	}
}
