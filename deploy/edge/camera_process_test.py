#!/usr/bin/env python3
r"""Explicit isolated camera-process validation, not assembled #202 acceptance.

Prerequisites: topology.py/camera_lan.py from topology commit f099c68, Python 3,
iproute2, setpriv, FFmpeg/FFprobe, and the compiled internal/e2e test binary in
the same disposable image. Build with GOMAXPROCS=2 and `go test -p 2 -c`; COPY
that binary and this script into the image. No host filesystem mounts are needed.
The opt-in TestEdgeCameraProcess is the only Go test executed by this validator.

For an image containing these files at /test and the binary at /test/e2e.test:

    docker run --rm --network=none --privileged --cpus=2 --memory=512m \
      --pids-limit=256 IMAGE python3 /test/topology.py --timeout 90 -- \
      python3 /test/camera_process_test.py --test-binary /test/e2e.test

The root CP coordinator launches UID 65534 camera/edge children through topology
manifest prefixes. topology.py obtains the camera's actual DHCP lease and owns
namespace/process cleanup even if this coordinator fails. Every credential
travels through a private stdin pipe. The embedded credential is a known synthetic
fixture value, not a device/CP secret. Captured RTP/H264 reaches ffprobe/ffmpeg
through stdin, without credential-bearing URI arguments. Encoders/decoders use
one thread, and process/socket waits and captured media have explicit bounds.

Checks cover native multicast, both protocol credentials, synthetic activation
response loss, held ONVIF Set changing decoded H264, immutable existing RTSP
sessions, disconnect, same-IP listener rebind/identity and stop/EOF/SIGTERM.
Activation uses the existing synthetic TP-Link wire-shape exchange; it does not
decrypt firmware RSA ciphertext or test arbitrary password generation. CP policy,
Worker/Producer orchestration and hardware acceptance remain separate work.
"""

import argparse
import base64
import hashlib
import json
import os
import pathlib
import select
import signal
import socket
import subprocess
import sys
import time
import urllib.parse
import xml.etree.ElementTree as ET
from http.client import HTTPConnection, RemoteDisconnected


def emit(value):
    print(json.dumps(value), flush=True)


def receive(process):
    assert select.select([process.stdout], [], [], 25)[0], 'bounded response timeout'
    line = process.stdout.readline()
    assert line and len(line) < 65536, 'missing bounded response'
    return json.loads(line)


def request(process, value):
    process.stdin.write(json.dumps(value) + '\n')
    process.stdin.flush()
    return receive(process)


def coordinator(test_binary):
    topology = json.loads(pathlib.Path(os.environ['SHALE_TOPOLOGY_FILE']).read_text())
    before = dict(token='main', codec='h264', width=160, height=90, fps=10, bitrate=64000, gop=10)
    after = dict(token='main', codec='h264', width=320, height=180, fps=20, bitrate=256000, gop=20)
    start = {'version': 1, 'id': 1, 'op': 'start', 'ip': topology['roles']['camera']['address'],
             'interface': 'eth0', 'profile': before, 'modes': [before, after], 'username': 'admin',
             'password': 'fixture-private-password-sentinel', 'active': False}
    credentials = {'username': start['username'], 'password': start['password']}
    env = {**os.environ, 'SHALE_EDGE_CAMERA_PROCESS': '1', 'GOMAXPROCS': '2'}

    def launch():
        argv = [*topology['roles']['camera']['exec_prefix'], test_binary,
                '-test.run', '^TestEdgeCameraProcess$', '-test.count=1']
        return subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, text=True, env=env)
    helper = launch()
    ready = request(helper, start)
    assert ready['event'] == 'ready' and (not ready['status']['active'])
    initial = ready['status']
    assert start['password'] not in json.dumps(ready)
    process_status = dict(row.split(':', 1) for row in pathlib.Path(f'/proc/{helper.pid}/status').read_text().splitlines() if ':' in row)
    assert set(process_status['Uid'].split()) == {'65534'}
    assert all((int(process_status[key].strip(), 16) == 0 for key in ('CapInh', 'CapPrm', 'CapEff', 'CapBnd', 'CapAmb')))
    client = subprocess.Popen([*topology['roles']['edge']['exec_prefix'], sys.executable,
                               str(pathlib.Path(__file__).resolve()), '--edge-client'],
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, text=True)
    info = request(client, {**credentials, 'endpoints': initial['endpoints']})
    assert info['ready']
    assert request(client, {'op': 'discover'})['epr'] == initial['identity']['DiscoveryID']
    assert request(client, {'op': 'inspect'})['status'] == 401
    sequence = 2

    def control(op, **values):
        nonlocal sequence
        result = request(helper, {'version': 1, 'id': sequence, 'op': op, **values})
        sequence += 1
        assert start['password'] not in json.dumps(result)
        assert result['event'] != 'error', result.get('code')
        return result.get('status')
    control('lose_activation_response')
    assert request(client, {'op': 'activate'})['lost_response']
    state = control('status')
    assert state['active'] and state['activations'] == 1
    assert request(client, {'op': 'inspect'})['status'] == 200
    unauthorized = request(client, {'op': 'wrong_auth'})
    assert unauthorized['onvif'] == unauthorized['rtsp'] == 401
    old = request(client, {'op': 'open', 'slot': 'old'})
    assert (old['width'], old['height']) == (160, 90) and old['decoded']
    control('hold_configuration')
    assert request(client, {'op': 'set'})['lost_response']
    state = control('status')
    assert state['accepted'] and (not state['settled']) and (state['profile']['width'] == 160)
    state = control('release_configuration')
    assert state['settled'] and state['profile']['width'] == 320 and (state['configuration_writes'] == 1)
    old = request(client, {'op': 'sample', 'slot': 'old'})
    assert (old['width'], old['height']) == (160, 90) and old['decoded']
    new = request(client, {'op': 'open', 'slot': 'new'})
    assert (new['width'], new['height']) == (320, 180) and new['decoded']
    control('disconnect')
    assert request(client, {'op': 'closed'})['closed']
    rebound = control('rebind', ip=start['ip'], interface='eth0')
    assert rebound['identity'] == initial['identity'] and rebound['endpoints'] != initial['endpoints']
    assert request(client, {'op': 'endpoints', 'endpoints': rebound['endpoints']})['ready']
    assert request(client, {'op': 'discover'})['endpoint'] == rebound['endpoints']['ONVIF']
    assert request(client, {'op': 'inspect'})['status'] == 200
    control('stop')
    helper.stdin.close()
    assert helper.wait(timeout=20) == 0
    diagnostics = helper.stderr.read()
    assert start['password'] not in diagnostics
    for endpoint in rebound['endpoints'].values():
        assert not request(client, {'op': 'reachable', 'endpoint': endpoint})['reachable']
    request(client, {'op': 'stop'})
    assert client.wait(timeout=5) == 0
    for ending in ('eof', 'signal'):
        helper = launch()
        start['active'] = True
        assert request(helper, start)['event'] == 'ready'
        if ending == 'eof':
            helper.stdin.close()
        else:
            helper.send_signal(signal.SIGTERM)
        assert helper.wait(timeout=20) == 0
        assert start['password'] not in helper.stderr.read()
        assert helper.stdout.read() == '', 'test framework output escaped JSON pipe'
    emit({'uid': 65534, 'actual_multicast': True, 'prepared_credential_auth_both_protocols': True,
          'synthetic_activation_response_loss': True, 'held_set_changes_actual_h264': True,
          'existing_rtsp_session_immutable': True, 'rebind_preserves_identity': True,
          'stop_eof_sigterm_cleanup': True, 'credential_free_public_output': True})


class RTSP:

    def __init__(self, endpoint, user, password):
        u = urllib.parse.urlparse(endpoint)
        self.conn = socket.create_connection((u.hostname, u.port), timeout=5)
        self.file = self.conn.makefile('rb')
        self.endpoint, self.auth = (endpoint, base64.b64encode((user + ':' + password).encode()).decode())
        self.seq = 0

    def send(self, op):
        self.seq += 1
        uri = self.endpoint + ('/trackID=0' if op == 'SETUP' else '')
        extra = 'Transport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n' if op == 'SETUP' else ''
        self.conn.sendall(f'{op} {uri} RTSP/1.0\r\nCSeq: {self.seq}\r\nAuthorization: Basic {self.auth}\r\n{extra}\r\n'.encode())
        status = int(self.file.readline().split()[1])
        headers = {}
        while True:
            line = self.file.readline()
            if line == b'\r\n':
                break
            k, v = line.decode().split(':', 1)
            headers[k.lower()] = v.strip()
        body = self.file.read(int(headers.get('content-length', '0')))
        return (status, body)

    def sample(self):
        raw = bytearray()
        deadline = time.monotonic() + 4
        idr = False
        markers = 0
        while time.monotonic() < deadline and (markers < 15 or not idr):
            header = self.file.read(4)
            assert len(header) == 4 and header[0] == 36
            packet = self.file.read(int.from_bytes(header[2:], 'big'))
            payload = packet[12:]
            kind = payload[0] & 31
            if kind == 28:
                if payload[1] & 128:
                    raw.extend(b'\x00\x00\x00\x01' + bytes([payload[0] & 224 | payload[1] & 31]))
                raw.extend(payload[2:])
                idr = idr or payload[1] & 31 == 5
            elif 1 <= kind <= 23:
                raw.extend(b'\x00\x00\x00\x01' + payload)
                idr = idr or kind == 5
            markers += bool(packet[1] & 128)
            assert len(raw) < 2 << 20
        probe = subprocess.run(['ffprobe', '-v', 'error', '-threads', '1', '-f', 'h264', '-i', 'pipe:0', '-show_entries', 'stream=width,height', '-of', 'json'], input=raw, capture_output=True, timeout=10)
        assert probe.returncode == 0
        dimensions = json.loads(probe.stdout)['streams'][0]
        decode = subprocess.run(['ffmpeg', '-v', 'error', '-threads', '1', '-f', 'h264', '-i', 'pipe:0', '-frames:v', '1', '-pix_fmt', 'gray', '-threads', '1', '-f', 'rawvideo', 'pipe:1'], input=raw, capture_output=True, timeout=10)
        assert decode.returncode == 0 and len(decode.stdout) == dimensions['width'] * dimensions['height']
        return {**dimensions, 'decoded': True}


def edge():
    state = json.loads(sys.stdin.readline())
    user, password = (state['username'], state['password'])
    endpoints = state['endpoints']
    streams = {}
    topo = json.loads(pathlib.Path(os.environ['SHALE_TOPOLOGY_FILE']).read_text())
    lan = topo['roles']['edge']['camera_address']

    def soap(op, contents='', wrong=False):
        nonce = b'fixture-private-nonce'
        created = '2026-11-08T00:00:00Z'
        digest = base64.b64encode(hashlib.sha1(nonce + created.encode() + (password + 'x' if wrong else password).encode()).digest()).decode()
        security = f'<Security><UsernameToken><Username>{user}</Username><Password>{digest}</Password><Nonce>{base64.b64encode(nonce).decode()}</Nonce><Created>{created}</Created></UsernameToken></Security>'
        body = f'<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope"><s:Header>{security}</s:Header><s:Body><t:{op} xmlns:t="http://www.onvif.org/ver20/media/wsdl">{contents}</t:{op}></s:Body></s:Envelope>'.encode()
        return http(body, 'application/soap+xml')

    def http(body, content):
        u = urllib.parse.urlparse(endpoints['ONVIF'])
        conn = HTTPConnection(u.hostname, u.port, timeout=15)
        conn.request('POST', u.path, body, {'Content-Type': content})
        try:
            response = conn.getresponse()
            return (response.status, response.read())
        finally:
            conn.close()
    emit({'ready': True})
    for line in sys.stdin:
        cmd = json.loads(line)
        op = cmd['op']
        if op == 'discover':
            conn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            conn.bind((lan, 0))
            conn.settimeout(3)
            conn.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(lan))
            conn.sendto(b'<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope"><s:Header><MessageID>urn:uuid:fixture-probe</MessageID></s:Header><s:Body><Probe/></s:Body></s:Envelope>', ('239.255.255.250', 3702))
            data, peer = conn.recvfrom(65536)
            root = ET.fromstring(data)
            value = lambda tag: next((e.text for e in root.iter() if e.tag.endswith('}' + tag) or e.tag == tag))
            emit({'epr': value('Address'), 'endpoint': value('XAddrs'), 'source': peer[0]})
            conn.close()
        elif op == 'inspect':
            emit({'status': soap('GetDeviceInformation')[0]})
        elif op == 'activate':
            try:
                http(json.dumps({'set_password': {'ciphertext': base64.b64encode(bytes(128)).decode(), 'username': 'admin', 'pwd_sec_type': '1'}}).encode(), 'application/json')
                emit({'lost_response': False})
            except RemoteDisconnected:
                emit({'lost_response': True})
        elif op == 'wrong_auth':
            rtsp = RTSP(endpoints['RTSP'], user, password + 'x')
            status = rtsp.send('DESCRIBE')[0]
            rtsp.conn.close()
            emit({'onvif': soap('GetDeviceInformation', wrong=True)[0], 'rtsp': status})
        elif op == 'open':
            rtsp = RTSP(endpoints['RTSP'], user, password)
            for method in ['OPTIONS', 'DESCRIBE', 'SETUP', 'PLAY']:
                assert rtsp.send(method)[0] == 200
            streams[cmd['slot']] = rtsp
            emit(rtsp.sample())
        elif op == 'sample':
            emit(streams[cmd['slot']].sample())
        elif op == 'set':
            configuration = '<Configuration GovLength="20"><Encoding>H264</Encoding><Resolution><Width>320</Width><Height>180</Height></Resolution><RateControl><FrameRateLimit>20</FrameRateLimit><BitrateLimit>256</BitrateLimit></RateControl></Configuration>'
            try:
                soap('SetVideoEncoderConfiguration', configuration)
                emit({'lost_response': False})
            except RemoteDisconnected:
                emit({'lost_response': True})
        elif op == 'closed':
            for stream in streams.values():
                while stream.file.read(65536):
                    pass
                stream.conn.close()
            streams.clear()
            emit({'closed': True})
        elif op == 'endpoints':
            endpoints = cmd['endpoints']
            emit({'ready': True})
        elif op == 'reachable':
            u = urllib.parse.urlparse(cmd['endpoint'])
            try:
                conn = socket.create_connection((u.hostname, u.port), timeout=1)
                conn.close()
                reachable = True
            except OSError:
                reachable = False
            emit({'reachable': reachable})
        elif op == 'stop':
            emit({'stopped': True})
            return

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--test-binary', default='/test/e2e.test', help='compiled internal/e2e test binary inside the image')
    parser.add_argument('--edge-client', action='store_true', help=argparse.SUPPRESS)
    arguments = parser.parse_args()
    if arguments.edge_client:
        edge()
    else:
        coordinator(arguments.test_binary)
