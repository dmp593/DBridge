import os
import asyncio
import logging
import uuid


logging.basicConfig(level=logging.INFO, format="%(message)s")

PROXY_HOST = os.getenv("PROXY_HOST", "localhost")
PROXY_PORT = int(os.getenv("PROXY_PORT", 4000))

DATABASE_HOST = os.getenv("DATABASE_HOST", "localhost")
DATABASE_PORT = int(os.getenv("DATABASE_PORT", 3306))

DELAY_RETRY_SECONDS = max(float(os.getenv("DELAY_RETRY_SECONDS", 1.0)), 0.3)


async def forward(source: asyncio.StreamReader, destination: asyncio.StreamWriter):
    while True:
        data = await source.read(4096)

        if not data:
            break

        destination.write(data)
        await destination.drain()

    destination.close()
    await destination.wait_closed()


async def run_agent():
    try:
        proxy_reader, proxy_writer = await asyncio.open_connection(PROXY_HOST, PROXY_PORT)
        token = uuid.uuid4()

        logging.info("三三ᕕ{ •̃_•̃ }ᕗ    ➤➤➤    %s" % token)

        proxy_writer.write(token.bytes)  # Send my identification
        await proxy_writer.drain()

        if await proxy_reader.read(7) != b"connect":
            logging.info("🤨 Unexpected response from proxy.")
            asyncio.create_task(run_agent())  # Spawn a new connection

            proxy_writer.close()
            return await proxy_writer.wait_closed()

        db_reader, db_writer = await asyncio.open_connection(DATABASE_HOST, DATABASE_PORT)
        asyncio.create_task(run_agent())  # Spawn a new connection

        proxy_writer.write(b"ready")
        await proxy_writer.drain()

        await asyncio.gather(
            forward(proxy_reader, db_writer),  # Proxy → Agent → DB
            forward(db_reader, proxy_writer)   # DB → Agent → Proxy
        )

    except (OSError, asyncio.IncompleteReadError) as e:
        logging.info("{╥﹏╥} Connection error, retrying in %.2f second(s)...", DELAY_RETRY_SECONDS)
        await asyncio.sleep(DELAY_RETRY_SECONDS)
        asyncio.create_task(run_agent())  # Spawn a new connection


async def shutdown(loop):
    logging.info(f"{{𓌻‸𓌻}} ᴜɢʜ...")

    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]

    for task in tasks:
        task.cancel()

    # Wait until all tasks are finished or canceled
    await asyncio.gather(*tasks, return_exceptions=True)

    loop.call_soon(loop.stop)


async def main():
    loop = asyncio.get_running_loop()

    try:
        asyncio.create_task(run_agent())
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        await shutdown(loop)


if __name__ == "__main__":
    asyncio.run(main())
