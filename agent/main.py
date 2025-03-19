import os
import asyncio
import logging
import uuid


logging.basicConfig(level=logging.INFO, format="%(message)s")

PROXY_HOST = os.getenv("PROXY_HOST", "localhost")
PROXY_PORT_FORWARDS = int(os.getenv("PROXY_PORT_FORWARDS", 4000))
PROXY_PORT_AGENTS = int(os.getenv("PROXY_PORT_AGENTS", 8000))

DATABASE_HOST = os.getenv("DATABASE_HOST", "localhost")
DATABASE_PORT = int(os.getenv("DATABASE_PORT", 3306))

notification_connection_create = b"connection.create"


async def forward(source: asyncio.StreamReader, destination: asyncio.StreamWriter):
    while True:
        data = await source.read(4096)

        if not data:
            break

        destination.write(data)
        await destination.drain()

    destination.close()
    await destination.wait_closed()


async def handle_proxy_connection(token: str):
    try:
        proxy_reader, proxy_writer = await asyncio.open_connection(PROXY_HOST, PROXY_PORT_FORWARDS)
        proxy_writer.write(token.encode())  # send my identification token
        await proxy_writer.drain()

        db_reader, db_writer = await asyncio.open_connection(DATABASE_HOST, DATABASE_PORT)

        await asyncio.gather(
            asyncio.create_task(
                forward(proxy_reader, db_writer)  # Proxy → Agent → DB
            ),
            asyncio.create_task(
                forward(db_reader, proxy_writer)  # DB → Agent → Proxy
            )
        )

    except OSError:
        pass


async def run_agent(token: str):
    reader, writer = await asyncio.open_connection(PROXY_HOST, PROXY_PORT_AGENTS)
    logging.info("三三ᕕ{ •̃_•̃ }ᕗ    ➤➤➤    %s" % token)

    writer.write(token.encode())  # send my identification token
    await writer.drain()

    # open a proxy connection
    asyncio.create_task(handle_proxy_connection(token))

    while True:
        data = await reader.read(1024)

        if not data:
            break

        if data == notification_connection_create:
            asyncio.create_task(handle_proxy_connection(token))  # Spawn a new proxy connection


async def shutdown(loop, shutdown_event: asyncio.Event):
    logging.info(f"{{𓌻‸𓌻}} ᴜɢʜ...")

    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]

    for task in tasks:
        task.cancel()

    # Wait until all tasks are finished or canceled
    await asyncio.gather(*tasks, return_exceptions=True)

    shutdown_event.set()
    loop.call_soon(loop.stop)


async def main():
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    try:
        while True:
            try:
                token = str(uuid.uuid4())
                await run_agent(token)

            except OSError:
                logging.info("{╥﹏╥}")
                await asyncio.sleep(1)

    except asyncio.CancelledError:
        await shutdown(loop, shutdown_event)


if __name__ == "__main__":
    asyncio.run(main())
