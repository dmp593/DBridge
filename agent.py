import os
import argparse
import asyncio
import logging
import typing
import uuid
import ssl


T = typing.TypeVar("T")


def parse(to: typing.Type[T], value: typing.Any, or_default: T | None = None) -> T | None:
    if isinstance(value, to):
        return value
    try:
        return to(value)
    except (ValueError, TypeError):
        return or_default


def parse_bool(value: typing.Any, or_default: typing.Any | None = None) -> typing.Any | None:
    if isinstance(value, int):
        return value != 0

    if isinstance(value, str):
        match value.lower().strip():
            case "yes" | "y" | "t" | "true" | "s" | "sim" | "yea" | "yeah" | "on" | 1 | "1":
                return True
            case "no" | "n" | "f" | "false" | "ñ" | "não" | "nao" | "nop" | "nope" | "off" | 0 | "0" | "":
                return False
            case _:
                return or_default


def validate_min_threads(value):
    try:
        f_value = int(value)
        if f_value <= 0:
            raise argparse.ArgumentTypeError("must be a positive integer")
        return f_value
    except ValueError:
        raise argparse.ArgumentTypeError("Invalid integer value.")


def validate_retry_delay_seconds(value):
    try:
        f_value = float(value)
        if f_value <= 0 or f_value > 300:
            raise argparse.ArgumentTypeError("must be > 0 and <= 300")
        return f_value
    except ValueError:
        raise argparse.ArgumentTypeError("Invalid float value.")


def validate_log_level(value: str):
    value = value.upper()
    log_levels = logging.getLevelNamesMapping()

    if value not in log_levels:
        raise argparse.ArgumentTypeError(f"options are {", ".join(log_levels)}")

    return value


def parse_args():
    parser = argparse.ArgumentParser(description="Agent to connect to proxy and forward data.")

    default_proxy_host = os.getenv("PROXY_HOST")
    default_proxy_port = parse(to=int, value=os.getenv("PROXY_PORT"), or_default=7000)

    default_health_port = parse(to=int, value=os.getenv("HEALTH_PORT"), or_default=4000)

    default_service_host = os.getenv("SERVICE_HOST", "localhost")
    default_service_port = parse(to=int, value=os.getenv("SERVICE_PORT"), or_default=None)

    default_min_threads = parse(to=int, value=os.getenv("MIN_THREADS", os.cpu_count()), or_default=1)
    default_retry_delay_seconds = parse(to=float, value=os.getenv("RETRY_DELAY_SECONDS"), or_default=1.0)

    default_log_level = os.getenv("LOG_LEVEL", "INFO")

    default_use_ssl = parse_bool(value=os.getenv("USE_SSL"), or_default=False)
    default_ssl_cert = os.getenv("SSL_CERT", None)

    parser.add_argument("-x", "--proxy-host", default=default_proxy_host, required=default_proxy_host is None,
                        help="Proxy server host (required if not set in environment)")

    parser.add_argument("-p", "--proxy-port", type=int, default=default_proxy_port,
                        help=f"Proxy server port (default: {default_proxy_port})")

    parser.add_argument("-e", "--health-port", type=int, default=default_health_port,
                        help=f"Health prob port (default: {default_health_port})")

    parser.add_argument("-d", "--service-host", default=default_service_host,
                        help=f"Service host (default: {default_service_host})")

    parser.add_argument("-b", "--service-port", type=int, default=default_service_port, required=default_service_port is None,
                        help="Service port (required if not set in environment)")

    parser.add_argument("-n", "--min-threads", type=validate_min_threads, default=default_min_threads,
                        help=f"Minimum number of agent threads to start initially. The system will scale based on load (default: {default_min_threads})")

    parser.add_argument("-r", "--retry-delay-seconds", type=validate_retry_delay_seconds, default=default_retry_delay_seconds,
                        help=f"Delay before retrying connection (default: {default_retry_delay_seconds:.2f}s)")

    parser.add_argument("-v", "--log-level", type=validate_log_level, default=default_log_level,
                        help=f"Log Level (default: {default_log_level})")

    parser.add_argument("-s", "--ssl", action="store_true", default=default_use_ssl,
                        help="Enable SSL for connecting to the proxy (default: no)")

    parser.add_argument("-c", "--cert", type=str, default=default_ssl_cert,
                        help=f"Path to the CA certificate file (optional)")

    return parser.parse_args()


async def forward(agent_token: uuid.UUID, source_peername: tuple[str, int], source: asyncio.StreamReader, destination: asyncio.StreamWriter):
    try:
        while True:
            data = await source.read(4096)

            if not data:
                break

            destination.write(data)
            await destination.drain()
    except asyncio.IncompleteReadError:
        logging.error(
            "(%s) 😢 { source: %s:%d <-> destination: %s:%d } Unexpected EOF",
            agent_token, *source_peername, *destination.transport.get_extra_info('peername')
        )

    except ConnectionResetError:
        logging.error(
            "(%s) 😩 { source: %s:%d <-> destination: %s:%d } Connection reset",
            agent_token, *source_peername, *destination.transport.get_extra_info('peername')
        )

    except Exception:
        logging.error(
            "(%s) 👽 { source: %s:%d <-> destination: %s:%d } Something crazy happened",
            agent_token, *source_peername, *destination.transport.get_extra_info('peername')
        )

    finally:
        destination.close()
        await destination.wait_closed()


async def handle_health(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    writer.write("💅 slaying and thriving, PERIOD.".encode())
    await writer.drain()

    writer.close()
    await writer.wait_closed()


async def run_agent(token: uuid.UUID, proxy_host, proxy_port, svc_host, svc_port, use_ssl, cert, retry_delay_seconds, queue: asyncio.Queue):
    proxy_writer = None
    svc_writer = None

    try:
        ssl_context = ssl.create_default_context(cafile=cert) if use_ssl else None
        proxy_reader, proxy_writer = await asyncio.open_connection(proxy_host, proxy_port, ssl=ssl_context)

        logging.info("(%s) 🏃 ready!", token)

        proxy_writer.write(token.bytes)  # Send my identification
        await proxy_writer.drain()

        data = None

        while True:
            data = await proxy_reader.read(7)
            if data != b"ping": break

            proxy_writer.write(b"pong")
            await proxy_writer.drain()

            logging.debug("(%s) 🏓 ping-pong", token)

        if data != b"connect":
            logging.error("(%s) 🥜 something went nuts", token)

            proxy_writer.close()

            await asyncio.gather(
                proxy_writer.wait_closed(),
                asyncio.sleep(retry_delay_seconds),
                queue.put(1)  # Signal to spawn a new agent
            )

            return

        logging.debug("(%s) 💿 start forwarding", token)

        _, svc = await asyncio.gather(
            queue.put(1),  # Signal to spawn a new agent,
            asyncio.open_connection(svc_host, svc_port),
            return_exceptions=True
        )

        svc_reader, svc_writer = svc

        proxy_writer.write(b"ready")
        await proxy_writer.drain()

        svc_peername = svc_writer.transport.get_extra_info('peername')
        proxy_peername = proxy_writer.transport.get_extra_info('peername')

        try:
            await asyncio.gather(
                forward(token, proxy_peername, proxy_reader, svc_writer),  # Proxy → Agent → Service
                forward(token, svc_peername, svc_reader, proxy_writer)  # Service → Agent → Proxy
            )

            logging.debug("(%s) 🏁 finished forwarding", token)

        except Exception:
            logging.error(
                "(%s) 🛸 { proxy %s:%d <-> service %s:%d } Something crazy happened.",
                token, *proxy_peername, *svc_peername
            )

            svc_writer.close()
            proxy_writer.close()

            await asyncio.gather(
                svc_writer.wait_closed(),
                proxy_writer.wait_closed(),
                return_exceptions=True
            )

    except (OSError, asyncio.IncompleteReadError):
        logging.error("(%s) 😭 connection error, retrying in %.2f sec(s)...", token, retry_delay_seconds)
        await asyncio.sleep(retry_delay_seconds)
        await queue.put(1)  # Signal to spawn a new agent

    except Exception as ex:
        logging.error("(%s) ❌ exception: %s", token, ex)

    finally:
        logging.info("(%s) 🪦 R.I.P", token)

        tasks = []

        if proxy_writer:
            proxy_writer.close()
            tasks.append(proxy_writer.wait_closed())

        if svc_writer:
            svc_writer.close()
            tasks.append(svc_writer.wait_closed())

        await asyncio.gather(*tasks, return_exceptions=False)


async def spawn_agents(queue: asyncio.Queue, *args):
    while True:
        await queue.get()  # Wait for a signal to spawn an agent
        token = uuid.uuid4()
        logging.debug("(%s) 🤰 laboring a new agent", token)
        asyncio.create_task(run_agent(token, *args, queue))


async def shutdown(loop, health_server):
    logging.debug("️( -_•)︻デ═一💥 killing...")

    await asyncio.sleep(0.5)  # Allow logs to flush

    if health_server:
        health_server.close()
        await health_server.wait_closed()

    tasks = {t for t in asyncio.all_tasks() if t is not asyncio.current_task()}

    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)
    loop.call_soon(loop.stop)



async def main():
    args = parse_args()
    logging.basicConfig(level=args.log_level, format="%(message)s")

    loop = asyncio.get_running_loop()
    queue = asyncio.Queue()

    health_server = None

    try:
        # Start the initial set of agents
        await asyncio.gather(
            *[queue.put(1) for _ in range(args.min_threads)],
            return_exceptions=True
        )

        asyncio.create_task(
            spawn_agents(
                queue,
                args.proxy_host,
                args.proxy_port,
                args.service_host,
                args.service_port,
                args.ssl,
                args.cert,
                args.retry_delay_seconds,
            )
        )

        health_server = await asyncio.start_server(
            handle_health, 'localhost', args.health_port
        )

        await asyncio.Event().wait()  # Keep the event loop running

    except asyncio.CancelledError:
        logging.debug("🛑 Agent was cancelled.")
        await shutdown(loop, health_server)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.debug("⌨️ Interrupted by user.")
