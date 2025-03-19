import asyncio
import logging
import random
from collections import namedtuple


HOST = '0.0.0.0'


PORT_NOTIFICATIONS = 8000
PORT_CLIENTS_FWD = 3000
PORT_AGENTS_FWD = 4000


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


listeners: list[tuple[asyncio.StreamReader, asyncio.StreamWriter]] = []
agents: list[tuple[asyncio.StreamReader, asyncio.StreamWriter]] = []


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


async def handle_listeners(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    logging.info("new listener connected")

    listener = (reader, writer)
    listeners.append(listener)

    await writer.wait_closed()

    if listener in listeners:
        listeners.remove(listener)


async def handle_agent(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    logging.info("new agent connected")

    agent = (reader, writer)
    agents.append(agent)

    await writer.wait_closed()

    if agent in agents:
        agents.remove(agent)


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    logging.info("new client connected")

    if len(listeners) > 0:
        (listener_reader, listener_writer) = random.choice(listeners)
        listener_writer.write(b'create_connection')
        await listener_writer.drain()

    if len(agents) == 0:
        writer.close()
        await writer.wait_closed()

    (agent_reader, agent_writer) = agents.pop()

    await asyncio.gather(
        asyncio.create_task(
            forward(agent_reader, writer),
        ),
        asyncio.create_task(
            forward(reader, agent_writer)
        )
    )

async def run_forever(servers):
    try:
        # This keeps the event loop running indefinitely
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        # Handle the cleanup in case of cancellation
        for server in servers:
            server.close()
            await server.wait_closed()


async def main():
    servers = await asyncio.gather(
        asyncio.start_server(handle_listeners, HOST, PORT_NOTIFICATIONS),
        asyncio.start_server(handle_agent, HOST, PORT_AGENTS_FWD),
        asyncio.start_server(handle_client, HOST, PORT_CLIENTS_FWD)
    )

    logging.info("Proxy is running and listening on ports.")

    await run_forever(servers)


if __name__ == '__main__':
    asyncio.run(main())
