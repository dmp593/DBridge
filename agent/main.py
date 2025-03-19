import os
import asyncio
import logging
import signal

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s"
)


PROXY_HOST = os.getenv("PROXY_HOST", "localhost")
PROXY_PORT_AGENTS = int(os.getenv("PROXY_PORT_AGENTS", 4000))
PROXY_PORT_NOTIFICATIONS = int(os.getenv("PROXY_PORT_NOTIFICATIONS", 8000))

DATABASE_HOST = os.getenv("DATABASE_HOST", "localhost")
DATABASE_PORT = int(os.getenv("DATABASE_PORT", 3306))


notification_create_connection = b"create_connection"


async def forward(source: asyncio.StreamReader, destination: asyncio.StreamWriter):
    try:
        while True:
            data = await source.read(4096)

            if not data:
                break

            destination.write(data)
            await destination.drain()

    except asyncio.CancelledError:
        pass

    finally:
        destination.close()
        await destination.wait_closed()


async def handle_proxy_connection():
    proxy_reader, proxy_writer = await asyncio.open_connection(PROXY_HOST, PROXY_PORT_AGENTS)
    db_reader, db_writer = await asyncio.open_connection(DATABASE_HOST, DATABASE_PORT)

    await asyncio.gather(
        asyncio.create_task(
            forward(proxy_reader, db_writer)  # Proxy → Agent → DB
        ),
        asyncio.create_task(
            forward(db_reader, proxy_writer)  # DB → Agent → Proxy
        )
    )


async def listen_for_notifications():
    reader, writer = await asyncio.open_connection(PROXY_HOST, PROXY_PORT_NOTIFICATIONS)
    logging.info("三三ᕕ{ •̃_•̃ }ᕗ")

    try:
        while True:
            data = await reader.read(4096)

            if not data:
                break

            if data == notification_create_connection:
                asyncio.create_task(handle_proxy_connection())  # Spawn new connection

    except asyncio.CancelledError:
        pass

    writer.close()
    await writer.wait_closed()


async def shutdown(loop):
    logging.info(f"{{𓌻‸𓌻}} ᴜɢʜ...")

    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]

    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)

    loop.call_soon(loop.close)


async def main():
    loop = asyncio.get_event_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig,
            lambda: asyncio.create_task(shutdown(loop))
        )

    while True:
        try:
            # Start the background task for the proxy connection
            asyncio.create_task(handle_proxy_connection())

            # Await the notifications listener (this will run indefinitely)
            await listen_for_notifications()
        except OSError:
            logging.info("ಥ_ಥ")

            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    asyncio.run(
        main()
    )
