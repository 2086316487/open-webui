from __future__ import annotations

import asyncio
import os
import shlex
import signal
import sys
from contextlib import suppress


PUBLIC_HOST = os.getenv('HOST', '0.0.0.0')
PUBLIC_PORT = int(os.getenv('PORT', '8080'))
UPSTREAM_HOST = os.getenv('OPEN_WEBUI_INTERNAL_HOST', '127.0.0.1')
UPSTREAM_PORT = int(os.getenv('OPEN_WEBUI_INTERNAL_PORT', '18080'))
RESTART_DELAY = int(os.getenv('RENDER_BOOT_PROXY_RESTART_DELAY', '5'))
CONNECT_TIMEOUT = float(os.getenv('RENDER_BOOT_PROXY_CONNECT_TIMEOUT', '0.25'))
READ_LIMIT = int(os.getenv('RENDER_BOOT_PROXY_READ_LIMIT', str(64 * 1024)))


def log(message: str) -> None:
    print(f'[render_boot_proxy] {message}', flush=True)


def child_command() -> list[str]:
    configured = os.getenv('RENDER_BOOT_PROXY_CHILD_CMD')
    if configured:
        return shlex.split(configured)
    return ['bash', 'start.sh']


def child_environment() -> dict[str, str]:
    env = os.environ.copy()
    env['HOST'] = UPSTREAM_HOST
    env['PORT'] = str(UPSTREAM_PORT)
    env['RENDER_BOOT_PROXY'] = 'false'
    env.setdefault('UVICORN_WORKERS', '1')
    env.setdefault('PYTHONDONTWRITEBYTECODE', '1')
    env.setdefault('MALLOC_ARENA_MAX', '1')
    env.setdefault('OMP_NUM_THREADS', '1')
    env.setdefault('OPENBLAS_NUM_THREADS', '1')
    env.setdefault('MKL_NUM_THREADS', '1')
    env.setdefault('NUMEXPR_NUM_THREADS', '1')
    env.setdefault('TOKENIZERS_PARALLELISM', 'false')
    return env


def http_response(status: str, body: bytes, content_type: str = 'text/plain') -> bytes:
    return (
        f'HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n'
    ).encode() + body


async def child_supervisor(state: dict[str, object]) -> None:
    cmd = child_command()
    command_display = ' '.join(shlex.quote(part) for part in cmd)

    while not state.get('stopping'):
        log(f'starting child: {command_display}')
        process = await asyncio.create_subprocess_exec(*cmd, env=child_environment())
        state['child_process'] = process
        return_code = await process.wait()
        log(f'child exited with code {return_code}')

        if state.get('stopping'):
            break

        log(f'restarting child in {RESTART_DELAY}s')
        await asyncio.sleep(RESTART_DELAY)


async def pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while True:
            data = await reader.read(READ_LIMIT)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    finally:
        writer.close()
        with suppress(Exception):
            await writer.wait_closed()


async def read_initial_request(reader: asyncio.StreamReader) -> bytes:
    try:
        return await asyncio.wait_for(reader.read(4096), timeout=1)
    except asyncio.TimeoutError:
        return b''


async def handle_unavailable(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    request_head = await read_initial_request(reader)
    if request_head.startswith((b'GET /health ', b'HEAD /health ')):
        body = b'{"status":true,"upstream_ready":false}\n'
        writer.write(http_response('200 OK', body, 'application/json'))
    else:
        body = b'Open WebUI is still starting. Refresh this page in a minute.\n'
        writer.write(http_response('503 Service Unavailable', body))

    await writer.drain()
    writer.close()
    with suppress(Exception):
        await writer.wait_closed()


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        upstream_reader, upstream_writer = await asyncio.wait_for(
            asyncio.open_connection(UPSTREAM_HOST, UPSTREAM_PORT),
            timeout=CONNECT_TIMEOUT,
        )
    except Exception:
        await handle_unavailable(reader, writer)
        return

    await asyncio.gather(
        pipe(reader, upstream_writer),
        pipe(upstream_reader, writer),
    )


async def shutdown(state: dict[str, object], supervisor: asyncio.Task[None]) -> None:
    state['stopping'] = True
    supervisor.cancel()

    process = state.get('child_process')
    if process and getattr(process, 'returncode', None) is None:
        process.send_signal(signal.SIGTERM)
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(process.wait(), timeout=15)
        if process.returncode is None:
            process.kill()

    with suppress(asyncio.CancelledError):
        await supervisor


async def main_async() -> None:
    state: dict[str, object] = {}
    server = await asyncio.start_server(handle_client, PUBLIC_HOST, PUBLIC_PORT)
    log(f'listening on {PUBLIC_HOST}:{PUBLIC_PORT}, forwarding to {UPSTREAM_HOST}:{UPSTREAM_PORT}')
    supervisor = asyncio.create_task(child_supervisor(state))

    try:
        async with server:
            await server.serve_forever()
    finally:
        await shutdown(state, supervisor)


def main() -> None:
    asyncio.run(main_async())


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
