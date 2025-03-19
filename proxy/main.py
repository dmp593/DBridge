import asyncio
import logging
import random


logging.basicConfig(level=logging.INFO, format="%(message)s")

HOST = '0.0.0.0'
PORT_AGENTS = 8000
PORT_FORWARDS_AGENTS = 4000
PORT_FORWARDS_CLIENTS = 3000


notification_connection_create = b"connection.create"


class Agent:
    token: str

    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter

    forwards: list[
        tuple[asyncio.StreamReader, asyncio.StreamWriter]
    ]

    def __init__(self, token: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.token = token
        self.reader = reader
        self.writer = writer
        self.forwards = []

    def pop(self, index: int = 0) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        return self.forwards.pop(index) if len(self.forwards) > 0 else (None, None)

class State:
    lock: asyncio.Lock
    agents: dict[str, Agent]

    def __init__(self):
        self.lock = asyncio.Lock()
        self.agents = {}

    async def add_agent_streams(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        token = await reader.read(36)
        token = token.decode()

        print(f"🎉 new agent: {token}")

        async with self.lock:
            self.agents[token] = Agent(token, reader, writer)

        while not reader.at_eof():
            await asyncio.sleep(0.3)

        async with self.lock:
            print(f"🗑️ removing agent: {token}")
            agent = self.agents.pop(token)

        for (_, forward_writer) in agent.forwards:
            print(f"🗑️ removing forward stream (agent: {token})")
            forward_writer.close()
            await forward_writer.wait_closed()

    async def add_agent_forward_streams(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        token = await reader.read(36)
        token = token.decode()

        print(f"🎉 new forward stream (agent: {token})")

        async with self.lock:
            self.agents[token].forwards.append((reader, writer))

    async def pop_agent_forward_streams(self):
        async with self.lock:
            agent: Agent = random.choice(list(self.agents.values()))
            fw_stream = agent.forwards.pop(0)

        agent.writer.write(notification_connection_create)
        await agent.writer.drain()

        print(f"🍾 popping forward stream (agent: {agent.token})")
        return fw_stream


state = State()


async def forward(source: asyncio.StreamReader, destination: asyncio.StreamWriter):
    while True:
        data = await source.read(4096)

        if not data:
            break

        destination.write(data)
        await destination.drain()

    destination.close()
    await destination.wait_closed()


async def handle_agent(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    await state.add_agent_streams(reader, writer)


async def handle_forward_agent(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    await state.add_agent_forward_streams(reader, writer)


async def handle_forward_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    print("🎉 new client")

    try:
        fwd_reader, fwd_writer = await state.pop_agent_forward_streams()
    except IndexError:
        writer.close()
        return await writer.wait_closed()

    await asyncio.gather(
        asyncio.create_task(
            forward(reader, fwd_writer)
        ),
        asyncio.create_task(
            forward(fwd_reader, writer)
        )
    )


async def shutdown(loop, servers):
    logging.info(f"(𓌻‸𓌻) ᴜɢʜ...")

    for server in servers:
        server.close()
        await server.wait_closed()

    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)

    loop.call_soon(loop.stop)


async def main():
    loop = asyncio.get_running_loop()

    servers = await asyncio.gather(
        asyncio.start_server(handle_agent, HOST, PORT_AGENTS),
        asyncio.start_server(handle_forward_agent, HOST, PORT_FORWARDS_AGENTS),
        asyncio.start_server(handle_forward_client, HOST, PORT_FORWARDS_CLIENTS)
    )

    try:
        logging.info("三三ᕕ( ᐛ )ᕗ")
        await asyncio.Event().wait()

    except asyncio.CancelledError:
        await shutdown(loop, servers)


if __name__ == '__main__':
    asyncio.run(main())
