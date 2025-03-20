import os
import argparse
import asyncio
import logging
import typing
import uuid


logging.basicConfig(level=logging.INFO, format="%(message)s")


T = typing.TypeVar("T")


def parse(to: typing.Type[T], value: typing.Any, or_default: T | None = None) -> T | None:
    if isinstance(value, to):
        return value

    try:
        return to(value)
    except (ValueError, TypeError):
        return or_default


def positive_float(value):
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

    default_retry_delay_seconds = parse(to=float, value=os.getenv("RETRY_DELAY_SECONDS"), or_default=1.0)

    parser.add_argument("-x", "--proxy-host", default=default_proxy_host, required=default_proxy_host is None,
                        help="Proxy server host (required if not set in environment)")

    parser.add_argument("-q", "--proxy-port", type=int, default=default_proxy_port,
                        help=f"Proxy server port (default: {default_proxy_port})")

    parser.add_argument("-d", "--db-host", default=default_database_host,
                        help=f"Database host (default: {default_database_host})")

    parser.add_argument("-b", "--db-port", type=int, default=default_database_port, required=default_database_port is None,
                        help="Database port (required if not set in environment)")

    parser.add_argument("-r", "--retry-delay-seconds", type=positive_float, default=default_retry_delay_seconds,
                        help=f"Delay before retrying connection (default: {default_retry_delay_seconds:.2f}s)")

    return parser.parse_args()

async def forward(source: asyncio.StreamReader, destination: asyncio.StreamWriter):
    while True:
        data = await source.read(4096)

        if not data:
            break

        destination.write(data)
        await destination.drain()

    destination.close()
    await destination.wait_closed()


async def run_agent(proxy_host: str, proxy_port: int, db_host: str, db_port: int, retry_delay_seconds: float = 1.0):
    try:
        proxy_reader, proxy_writer = await asyncio.open_connection(proxy_host, proxy_port)
        token = uuid.uuid4()

        logging.info("三三ᕕ{ •̃_•̃ }ᕗ    ➤➤➤    %s" % token)

        proxy_writer.write(token.bytes)  # Send my identification
        await proxy_writer.drain()

        if await proxy_reader.read(7) != b"connect":
            logging.info("🤨 Unexpected response from proxy.")

            asyncio.create_task(  # Spawn a new connection
                run_agent(proxy_host, proxy_port, db_host, db_port, retry_delay_seconds)
            )

            proxy_writer.close()
            return await proxy_writer.wait_closed()

        db_reader, db_writer = await asyncio.open_connection(db_host, db_port)

        asyncio.create_task(  # Spawn a new connection
            run_agent(proxy_host, proxy_port, db_host, db_port, retry_delay_seconds)
        )

        proxy_writer.write(b"ready")
        await proxy_writer.drain()

        await asyncio.gather(
            forward(proxy_reader, db_writer),  # Proxy → Agent → DB
            forward(db_reader, proxy_writer)   # DB → Agent → Proxy
        )

    except (OSError, asyncio.IncompleteReadError) as e:
        logging.info("{╥﹏╥} Connection error, retrying in %.2f second(s)...", retry_delay_seconds)

        await asyncio.sleep(retry_delay_seconds)

        asyncio.create_task(  # Spawn a new connection
            run_agent(proxy_host, proxy_port, db_host, db_port, retry_delay_seconds)
        )


async def shutdown(loop):
    logging.info(f"{{𓌻‸𓌻}} ᴜɢʜ...")

    await asyncio.sleep(0.5)  # Allow logs to flush

    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]

    for task in tasks:
        task.cancel()

    # Wait until all tasks are finished or canceled
    await asyncio.gather(*tasks, return_exceptions=True)

    loop.call_soon(loop.stop)


async def main():
    args = parse_args()
    loop = asyncio.get_running_loop()

    try:
        asyncio.create_task(
            run_agent(args.proxy_host, args.proxy_port, args.db_host, args.db_port, args.retry_delay_seconds)
        )

        await asyncio.Event().wait()

    except asyncio.CancelledError:
        await shutdown(loop)


if __name__ == "__main__":
    asyncio.run(main())
