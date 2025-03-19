import asyncio
import logging
import random
import signal

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s"
)


HOST = '0.0.0.0'
PORT_LISTENERS = 8000
PORT_AGENTS = 4000
PORT_CLIENTS = 3000


notification_create_connection = b"create_connection"


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


async def save_connection(connections: list, connection: tuple[asyncio.StreamReader, asyncio.StreamWriter]):
    connections.append(connection)

    try:
        await connection[1].wait_closed()
    finally:
        if connection in connections:
            connections.remove(connection)


async def handle_listener(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    await save_connection(listeners, (reader, writer))


async def handle_agent(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    await save_connection(agents, (reader, writer))


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    if len(listeners) > 0:
        (listener_reader, listener_writer) = random.choice(listeners)
        listener_writer.write(notification_create_connection)
        await listener_writer.drain()

    if len(agents) == 0:
        writer.close()
        await writer.wait_closed()

    (agent_reader, agent_writer) = agents.pop(0)

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


async def shutdown(loop, servers):
    logging.info(f"(𓌻‸𓌻) ᴜɢʜ...")

    for listener in listeners:
        listener[1].close()
        await listener[1].wait_closed()

    for agent in agents:
        agent[1].close()
        await agent[1].wait_closed()

    for server in servers:
        server.close()
        await server.wait_closed()

    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)

    loop.call_soon(loop.stop)


async def main():
    loop = asyncio.get_event_loop()

    servers = await asyncio.gather(
        asyncio.start_server(handle_listener, HOST, PORT_LISTENERS),
        asyncio.start_server(handle_agent, HOST, PORT_AGENTS),
        asyncio.start_server(handle_client, HOST, PORT_CLIENTS)
    )

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig,
            lambda: asyncio.create_task(shutdown(loop, servers))
        )

    logging.info("三三ᕕ( ᐛ )ᕗ")
    await run_forever(servers)


if __name__ == '__main__':
    asyncio.run(main())
