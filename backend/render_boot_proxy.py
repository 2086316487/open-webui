from __future__ import annotations

import asyncio
import os
import shlex
import signal
import sys
from typing import Mapping

from aiohttp import ClientSession, ClientTimeout, WSMsgType, web


PUBLIC_HOST = os.getenv('HOST', '0.0.0.0')
PUBLIC_PORT = int(os.getenv('PORT', '8080'))
UPSTREAM_HOST = os.getenv('OPEN_WEBUI_INTERNAL_HOST', '127.0.0.1')
UPSTREAM_PORT = int(os.getenv('OPEN_WEBUI_INTERNAL_PORT', '18080'))
UPSTREAM_HTTP = os.getenv('OPEN_WEBUI_UPSTREAM_URL', f'http://{UPSTREAM_HOST}:{UPSTREAM_PORT}')
UPSTREAM_WS = UPSTREAM_HTTP.replace('http://', 'ws://', 1).replace('https://', 'wss://', 1)

HOP_BY_HOP_HEADERS = {
    'connection',
    'keep-alive',
    'proxy-authenticate',
    'proxy-authorization',
    'te',
    'trailer',
    'transfer-encoding',
    'upgrade',
}


def log(message: str) -> None:
    print(f'[render_boot_proxy] {message}', flush=True)


def filtered_headers(headers: Mapping[str, str]) -> dict[str, str]:
    return {k: v for k, v in headers.items() if k.lower() not in HOP_BY_HOP_HEADERS}


def upstream_url(request: web.Request, base_url: str) -> str:
    return f'{base_url}{request.rel_url}'


def child_command() -> list[str]:
    configured = os.getenv('RENDER_BOOT_PROXY_CHILD_CMD')
    if configured:
        return shlex.split(configured)
    return ['bash', 'start.sh']


async def start_child(app: web.Application) -> None:
    child_env = os.environ.copy()
    child_env['HOST'] = UPSTREAM_HOST
    child_env['PORT'] = str(UPSTREAM_PORT)
    child_env['RENDER_BOOT_PROXY'] = 'false'
    child_env.setdefault('UVICORN_WORKERS', '1')

    cmd = child_command()
    command_display = ' '.join(shlex.quote(part) for part in cmd)
    log(f'starting child: {command_display}')
    log(f'listening on {PUBLIC_HOST}:{PUBLIC_PORT}, forwarding to {UPSTREAM_HTTP}')
    process = await asyncio.create_subprocess_exec(*cmd, env=child_env)
    app['child_process'] = process
    app['upstream_ready'] = False
    app['monitor_task'] = asyncio.create_task(monitor_child(process))
    app['probe_task'] = asyncio.create_task(probe_upstream(app))


async def monitor_child(process: asyncio.subprocess.Process) -> None:
    return_code = await process.wait()
    log(f'child exited with code {return_code}')
    os._exit(return_code or 1)


async def probe_upstream(app: web.Application) -> None:
    session: ClientSession = app['session']
    while True:
        try:
            async with session.get(f'{UPSTREAM_HTTP}/health') as response:
                if response.status < 500:
                    if not app['upstream_ready']:
                        log('upstream is ready')
                    app['upstream_ready'] = True
                    await asyncio.sleep(10)
                    continue
        except Exception:
            pass

        app['upstream_ready'] = False
        await asyncio.sleep(2)


async def create_session(app: web.Application) -> None:
    app['session'] = ClientSession(timeout=ClientTimeout(total=None, sock_connect=30))


async def cleanup(app: web.Application) -> None:
    for task_name in ('probe_task', 'monitor_task'):
        task = app.get(task_name)
        if task:
            task.cancel()

    process = app.get('child_process')
    if process and process.returncode is None:
        process.send_signal(signal.SIGTERM)
        try:
            await asyncio.wait_for(process.wait(), timeout=15)
        except asyncio.TimeoutError:
            process.kill()

    session = app.get('session')
    if session:
        await session.close()


async def health(request: web.Request) -> web.Response:
    return web.json_response(
        {
            'status': True,
            'upstream_ready': bool(request.app.get('upstream_ready', False)),
        }
    )


async def proxy_websocket(request: web.Request) -> web.WebSocketResponse:
    server_ws = web.WebSocketResponse()
    await server_ws.prepare(request)

    session: ClientSession = request.app['session']
    headers = filtered_headers(request.headers)
    headers['x-forwarded-host'] = request.host
    headers['x-forwarded-proto'] = request.scheme

    try:
        async with session.ws_connect(upstream_url(request, UPSTREAM_WS), headers=headers) as client_ws:

            async def client_to_upstream() -> None:
                async for message in server_ws:
                    if message.type == WSMsgType.TEXT:
                        await client_ws.send_str(message.data)
                    elif message.type == WSMsgType.BINARY:
                        await client_ws.send_bytes(message.data)
                    elif message.type == WSMsgType.CLOSE:
                        await client_ws.close()

            async def upstream_to_client() -> None:
                async for message in client_ws:
                    if message.type == WSMsgType.TEXT:
                        await server_ws.send_str(message.data)
                    elif message.type == WSMsgType.BINARY:
                        await server_ws.send_bytes(message.data)
                    elif message.type == WSMsgType.CLOSE:
                        await server_ws.close()

            await asyncio.gather(client_to_upstream(), upstream_to_client())
    except Exception as exc:
        log(f'websocket proxy error: {exc}')

    return server_ws


async def proxy_http(request: web.Request) -> web.StreamResponse:
    if request.path == '/health':
        return await health(request)

    session: ClientSession = request.app['session']
    if request.headers.get('upgrade', '').lower() == 'websocket':
        return await proxy_websocket(request)

    headers = filtered_headers(request.headers)
    headers['x-forwarded-host'] = request.host
    headers['x-forwarded-proto'] = request.scheme
    body = await request.read()

    try:
        async with session.request(
            request.method,
            upstream_url(request, UPSTREAM_HTTP),
            data=body or None,
            headers=headers,
            allow_redirects=False,
        ) as upstream:
            response = web.StreamResponse(
                status=upstream.status,
                reason=upstream.reason,
                headers=filtered_headers(upstream.headers),
            )
            await response.prepare(request)
            async for chunk in upstream.content.iter_chunked(64 * 1024):
                await response.write(chunk)
            await response.write_eof()
            return response
    except Exception:
        return web.Response(
            status=503,
            text='Open WebUI is still starting. Refresh this page in a minute.',
            content_type='text/plain',
        )


def main() -> None:
    app = web.Application(client_max_size=1024**3)
    app.on_startup.append(create_session)
    app.on_startup.append(start_child)
    app.on_cleanup.append(cleanup)
    app.router.add_route('*', '/{path_info:.*}', proxy_http)
    web.run_app(app, host=PUBLIC_HOST, port=PUBLIC_PORT, print=None, access_log=None)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
