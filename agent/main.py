import os
import argparse
import asyncio
import logging
import typing
import uuid
import ssl

logging.basicConfig(level=logging.INFO, format="%(message)s")

T = typing.TypeVar("T")


def parse(to: typing.Type[T], value: typing.Any, or_default: T | None = None) -> T | None:
    if isinstance(value, to):
        return value
    try:
        return to(value)
    except (ValueError, TypeError):
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


def parse_args():
    parser = argparse.ArgumentParser(description="Agent to connect to proxy and forward data.")

    default_proxy_host = os.getenv("PROXY_HOST")
    default_proxy_port = parse(to=int, value=os.getenv("PROXY_PORT"), or_default=7000)

    default_database_host = os.getenv("DATABASE_HOST", "localhost")
    default_database_port = parse(to=int, value=os.getenv("DATABASE_PORT"), or_default=None)

    default_min_threads = parse(to=int, value=os.getenv("MIN_THREADS", os.cpu_count()), or_default=None)
    default_retry_delay_seconds = parse(to=float, value=os.getenv("RETRY_DELAY_SECONDS"), or_default=1.0)

    parser.add_argument("-x", "--proxy-host", default=default_proxy_host, required=default_proxy_host is None,
                        help="Proxy server host (required if not set in environment)")

    parser.add_argument("-p", "--proxy-port", type=int, default=default_proxy_port,
                        help=f"Proxy server port (default: {default_proxy_port})")

    parser.add_argument("-d", "--db-host", default=default_database_host,
                        help=f"Database host (default: {default_database_host})")

    parser.add_argument("-b", "--db-port", type=int, default=default_database_port, required=default_database_port is None,
                        help="Database port (required if not set in environment)")

    parser.add_argument("-n", "--min-threads", type=validate_min_threads, default=default_min_threads,
                        help=f"Minimum number of agent threads to start initially. The system will scale based on load (default: {default_min_threads})")

    parser.add_argument("-r", "--retry-delay-seconds", type=validate_retry_delay_seconds, default=default_retry_delay_seconds,
                        help=f"Delay before retrying connection (default: {default_retry_delay_seconds:.2f}s)")

    parser.add_argument("-s", "--ssl", action="store_true", help="Enable SSL for connecting to the proxy")

    parser.add_argument("-c", "--cert", type=str, help="Path to the CA certificate file (optional)")

    return parser.parse_args()


async def forward(source: asyncio.StreamReader, destination: asyncio.StreamWriter):
    try:
        while True:
            data = await source.read(4096)
            if not data:
                break
            destination.write(data)
            await destination.drain()

    except asyncio.IncompleteReadError:
        logging.info("😢 unexpected EOF")

    except ConnectionResetError:
        logging.info("😩 connection reset")

    except Exception:
        logging.info("👽 Something crazy happened.")

    finally:
        destination.close()
        await destination.wait_closed()


async def run_agent(proxy_host, proxy_port, db_host, db_port, use_ssl, cert, retry_delay_seconds, queue: asyncio.Queue):
    token = uuid.uuid4()

    try:
        ssl_context = ssl.create_default_context(cafile=cert) if use_ssl else None
        proxy_reader, proxy_writer = await asyncio.open_connection(proxy_host, proxy_port, ssl=ssl_context)

        logging.info("(%s) 🏃 Ready!", token)

        proxy_writer.write(token.bytes)  # Send my identification
        await proxy_writer.drain()

        if await proxy_reader.read(7) != b"connect":
            logging.info("(%s) 🥜 Something went nuts.", token)

            proxy_writer.close()
            await proxy_writer.wait_closed()

            await asyncio.sleep(retry_delay_seconds)
            await queue.put(1)  # Signal to spawn a new agent
            return

        await queue.put(1)  # Signal to spawn a new agent
        db_reader, db_writer = await asyncio.open_connection(db_host, db_port)

        proxy_writer.write(b"ready")
        await proxy_writer.drain()

        try:
            await asyncio.gather(
                forward(proxy_reader, db_writer),  # Proxy → Agent → DB
                forward(db_reader, proxy_writer)   # DB → Agent → Proxy
            )

        except Exception:
            logging.info("🛸 Something crazy happened.")

            db_writer.close()
            await db_writer.wait_closed()

            proxy_writer.close()
            await proxy_writer.wait_closed()

    except (OSError, asyncio.IncompleteReadError):
        logging.info("(%s) 😭 Connection error, retrying in %.2f second(s)...", token, retry_delay_seconds)
        await asyncio.sleep(retry_delay_seconds)
        await queue.put(1)  # Signal to spawn a new agent

    except Exception as ex:
        logging.info("(%s) ❌ Exception: %s", token, ex)

    finally:
        logging.info("(%s) 🗑 Discarding ..." % token)


async def shutdown(loop):
    logging.info("️( -_•)︻デ═一💥 killing...")

    await asyncio.sleep(0.5)  # Allow logs to flush

    tasks = {t for t in asyncio.all_tasks() if t is not asyncio.current_task()}

    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)
    loop.call_soon(loop.stop)


async def spawn_agents(queue: asyncio.Queue, *args):
    while True:
        await queue.get()  # Wait for a signal to spawn an agent
        asyncio.create_task(run_agent(*args, queue))


async def main():
    args = parse_args()
    loop = asyncio.get_running_loop()
    queue = asyncio.Queue()

    try:
        for _ in range(args.min_threads):  # Start the initial set of agents
            await queue.put(1)

        asyncio.create_task(
            spawn_agents(
                queue,
                args.proxy_host,
                args.proxy_port,
                args.db_host,
                args.db_port,
                args.ssl,
                args.cert,
                args.retry_delay_seconds,
            )
        )

        await asyncio.Event().wait()  # Keep the event loop running

    except asyncio.CancelledError:
        logging.info("🛑 Agent was cancelled.")
        await shutdown(loop)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("🔫 Interrupted by user.")
