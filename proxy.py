import os
import argparse
import asyncio
import logging
import uuid
import typing
import ssl
from asyncio import CancelledError


class Agent:
    token: uuid.UUID

    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter

    ping_pong_interval: float
    task_ping_pong: asyncio.Task | None = None

    def __init__(self, token: uuid.UUID, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, ping_pong_interval: float):
        self.token = token
        self.reader = reader
        self.writer = writer
        self.ping_pong_interval = ping_pong_interval

    async def start_ping_pong(self):
        if self.task_ping_pong:
            await self.stop_ping_pong()

        self.task_ping_pong = asyncio.create_task(self.run_ping_pong())

    async def run_ping_pong(self):
        while True:
            try:
                self.writer.write(b"ping")
                await self.writer.drain()

                data = await asyncio.wait_for(self.reader.read(4), timeout=5)
                if data != b"pong": raise Exception()

                logging.debug("🏓 ping-pong agent (%s)", self.token)

                await asyncio.sleep(self.ping_pong_interval)

            except CancelledError:
                logging.debug("🏓 Stopping ping-pong with agent (%s)", self.token)
                break

            except Exception:
                logging.error("🏓 Not a ping-pong. Error on agent (%s)", self.token)

                self.writer.close()
                await self.writer.wait_closed()

                break

    async def stop_ping_pong(self):
        self.task_ping_pong.cancel()
        await self.task_ping_pong
        self.task_ping_pong = None


class Context:
    lock: asyncio.Lock
    agents: list[Agent]

    def __init__(self):
        self.lock = asyncio.Lock()
        self.agents = []

    async def length(self):
        async with self.lock:
            return len(self.agents)

    async def add_agent(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, ping_pong_interval: float):
        token = uuid.UUID(bytes=await reader.read(16))
        logging.info(f"🎉 new agent (%s) %s:%d", token, *writer.transport.get_extra_info('peername'))

        agent = Agent(token, reader, writer, ping_pong_interval)
        await agent.start_ping_pong()

        async with self.lock:
            self.agents.append(agent)

    async def clean_zombies(self, sleep_interval: float):
        while True:
            await asyncio.sleep(sleep_interval)

            closing_zombies = []

            async with self.lock:
                agents = list(self.agents)

            for agent in agents:
                if agent.reader.at_eof():
                    logging.info("🧟 zombie agent (%s) detected.", agent.token)

                    try:
                        async with self.lock:
                            self.agents.remove(agent)  # 🗑 discarding!

                        agent.writer.close()
                        closing_zombies.append(agent.writer.wait_closed())

                    except ValueError:  # agent already popped by another thread...
                        logging.info("🧼 zombie agent (%s) already gone.", agent.token)

            await asyncio.gather(*closing_zombies, return_exceptions=True)

            if len(closing_zombies) > 0:
                logging.info("🗑️ cleaned %d zombie agent(s)", len(closing_zombies))

    async def pop_agent(self) -> Agent | None:
        while True:
            try:
                async with self.lock:
                    agent = self.agents.pop(0)

                await agent.stop_ping_pong()

                if agent.reader.at_eof():
                    logging.info("🗑️ zombie agent (%s) discarded", agent.token)
                    agent.writer.close()
                    await agent.writer.wait_closed()
                    continue

                logging.info("🍾 agent (%s) popped", agent.token)
                return agent

            except IndexError:  # no agents left.
                return None


context = Context()


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


def validate_wait_agent_retry_interval(value):
    try:
        f_value = float(value)
        if f_value <= 0 or f_value > 300:
            raise argparse.ArgumentTypeError("must be > 0 and <= 300")
        return f_value
    except ValueError:
        raise argparse.ArgumentTypeError("Invalid float value.")


def validate_zombies_clean_interval(value):
    if value == "off":
        return 0

    try:
        f_value = float(value)
        if f_value < 0 or f_value > 300:
            raise argparse.ArgumentTypeError("must be >= 0 and <= 300")
        return f_value
    except ValueError:
        raise argparse.ArgumentTypeError("Invalid float value.")


def validate_ping_pong_interval(value):
    try:
        f_value = float(value)
        if f_value <= 0 or f_value >= 30:
            raise argparse.ArgumentTypeError("must be > 0 and < 30")
        return f_value
    except ValueError:
        raise argparse.ArgumentTypeError("Invalid float value.")


def validate_log_level(value: str):
    value = value.upper()
    log_levels = logging.getLevelNamesMapping()

    if value not in log_levels:
        raise argparse.ArgumentTypeError(f"options are {", ".join(log_levels)}")

    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Proxy server for forwarding connections.")

    default_host = os.getenv("HOST", "0.0.0.0")
    default_port_liveness = parse(to=int, value=os.getenv("PORT_LIVENESS"), or_default=3130)
    default_port_readiness = parse(to=int, value=os.getenv("PORT_READINESS"), or_default=4260)
    default_port_agents = parse(to=int, value=os.getenv("PORT_AGENTS"), or_default=7000)
    default_port_clients = parse(to=int, value=os.getenv("PORT_CLIENTS"), or_default=9000)

    default_allowed_agents = os.getenv("ALLOWED_AGENTS", "*")
    default_allowed_clients = os.getenv("ALLOWED_CLIENTS", "*")

    default_wait_agent_max_tries = parse(to=int, value=os.getenv("WAIT_AGENT_MAX_TRIES"), or_default=10)
    default_wait_agent_retry_interval = parse(to=float, value=os.getenv("WAIT_AGENT_RETRY_INTERVAL"), or_default=0.7)

    default_zombies_clean_interval=parse(to=float, value=os.getenv("ZOMBIES_CLEAN_INTERVAL"), or_default=0)

    default_ping_pong_interval_seconds = parse(to=float, value=os.getenv("PING_PONG_INTERVAL_SECONDS"), or_default=21)

    default_log_level = os.getenv("LOG_LEVEL", "INFO")

    default_use_ssl = parse_bool(value=os.getenv("USE_SSL"), or_default=False)
    default_ssl_cert = os.getenv("SSL_CERT", None)
    default_ssl_key = os.getenv("SSL_KEY", None)

    parser.add_argument("-x", "--host", default=default_host,
                        help=f"Host to listen on (default: {default_host})")

    parser.add_argument("-l", "--port-liveness", type=int, default=default_port_liveness,
                        help=f"Port for liveness prob (default: {default_port_liveness})")

    parser.add_argument("-r", "--port-readiness", type=int, default=default_port_readiness,
                        help=f"Port for readiness prob (default: {default_port_readiness})")

    parser.add_argument("-a", "--port-agents", type=int, default=default_port_agents,
                        help=f"Port for agent connections (default: {default_port_agents})")

    parser.add_argument("-p", "--port-clients", type=int, default=default_port_clients,
                        help=f"Port for client connections (default: {default_port_clients})")

    parser.add_argument("-q", "--allow-agent", type=str, nargs='+', default=default_allowed_agents,
                        help=f"Agent hostname or IP addresses allowed to access (default: {default_allowed_agents})")

    parser.add_argument("-i", "--allow-client", type=str, nargs='+', default=default_allowed_clients,
                        help=f"Client hostname or IP addresses allowed to access (default: {default_allowed_clients})")

    parser.add_argument("-t", "--wait-agent-max-tries", type=int, default=default_wait_agent_max_tries,
                        help=f"Maximum number of attempts to find an agent (default: {default_wait_agent_max_tries})")

    parser.add_argument("-j", "--wait-agent-retry-interval", type=validate_wait_agent_retry_interval, default=default_wait_agent_retry_interval,
                        help=f"Time (secs) to sleep between retries when no agent is available (default: {default_wait_agent_retry_interval:.2f})")

    parser.add_argument("-z", "--zombies-clean-interval", type=validate_zombies_clean_interval, default=default_zombies_clean_interval,
                        help=f"Interval (secs) for cleaning zombie agents. Set to 0 to disable automatic cleanup: zombies will only be discarded when getting an available agent. (Default: {default_zombies_clean_interval:.2f})")

    parser.add_argument("-u", "--ping-pong-interval", type=validate_ping_pong_interval, default=default_ping_pong_interval_seconds,
                        help=f"Interval (secs) to do a ping-pong to keep connection alive between the proxy and the agents. (Default: {default_ping_pong_interval_seconds:.2f})")

    parser.add_argument("-v", "--log-level", type=validate_log_level, default=default_log_level,
                        help=f"Log Level (default: {default_log_level})")

    parser.add_argument("-s", "--ssl", action="store_true", default=default_use_ssl,
                        help="Enable SSL for secure connections (default: no)")

    parser.add_argument("-c", "--cert", default=default_ssl_cert,
                        help=f"SSL certificate file (optional)")

    parser.add_argument("-k", "--key", default=default_ssl_key,
                        help=f"SSL private key file (optional)")

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
            "😢 { (%s) source: %s:%d <-> destination: %s:%d } Unexpected EOF",
            agent_token, *source_peername, *destination.transport.get_extra_info('peername')
        )

    except ConnectionResetError:
        logging.error(
            "😩 { (%s) source: %s:%d <-> destination: %s:%d } Connection reset",
            agent_token, *source_peername, *destination.transport.get_extra_info('peername')
        )

    except Exception:
        logging.error(
            "👽 { (%s) source: %s:%d <-> destination: %s:%d } Something crazy happened",
            agent_token, *source_peername, *destination.transport.get_extra_info('peername')
        )

    finally:
        destination.close()
        await destination.wait_closed()


async def handle_liveness(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    writer.write("🚀 alive and forwarding packets like a champ!".encode())
    await writer.drain()

    writer.close()
    await writer.wait_closed()


async def handle_readiness(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    if await context.length() == 0:
        message = "⚠️ not ready! waiting for agents to join..."
    else:
        message = "🚀 ready to forward packets like a pro!"

    writer.write(message.encode("utf-8"))
    await writer.drain()

    writer.close()
    await writer.wait_closed()


async def handle_agent(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, allowed_hosts: list[str], ping_pong_interval: float):
    agent_peername = writer.transport.get_extra_info('peername')

    if '*' not in allowed_hosts and agent_peername[0] not in allowed_hosts:
        logging.warning(
            "🚨 ACCESS DENIED 🚨 | Unauthorized agent attempted to connect! "
            "IP: %s | Possible Intrusion Attempt! 🏴‍☠️",
            agent_peername[0]
        )

        writer.close()
        await writer.wait_closed()

        return

    await context.add_agent(reader, writer, ping_pong_interval)


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, allowed_hosts: list[str], wait_agent_max_tries: int, wait_agent_retry_interval: float):
    client_peername = writer.transport.get_extra_info('peername')

    if '*' not in allowed_hosts and client_peername[0] not in allowed_hosts:
        logging.warning(
            "🚨 ACCESS DENIED 🚨 | Unauthorized client attempted to connect! "
            "IP: %s | Possible Intrusion Attempt! 🏴‍☠️",
            client_peername[0]
        )

        writer.close()
        await writer.wait_closed()

        return

    logging.info("🎉 new client %s:%d", *client_peername)

    agent = None

    for i in range(wait_agent_max_tries):
        agent = await context.pop_agent()
        if agent: break

        logging.info("🍃 no agents available, retrying in %.2f sec(s)...", wait_agent_retry_interval)
        await asyncio.sleep(wait_agent_retry_interval)
    else:
        logging.info("👻 no agents available.")

    if not agent:
        writer.close()
        return await writer.wait_closed()

    agent.writer.write(b"connect")
    await agent.writer.drain()

    agent_peername = agent.writer.transport.get_extra_info('peername')

    if await agent.reader.read(5) != b"ready":
        writer.close()
        agent.writer.close()

        await asyncio.gather(
            writer.wait_closed(),
            agent.writer.wait_closed(),
            return_exceptions=True
        )

        return

    try:
        logging.info(
            "💿 { agent %s:%d (%s) <-> client %s:%d } started",
            *agent_peername, agent.token, *client_peername
        )

        await asyncio.gather(
            asyncio.create_task(
                forward(agent.token, client_peername, reader, agent.writer)
            ),
            asyncio.create_task(
                forward(agent.token, agent_peername, agent.reader, writer)
            )
        )

        logging.info(
            "🏁 { agent %s:%d (%s) <-> client %s:%d } finished",
            *agent_peername, agent.token, *client_peername
        )
    except Exception:
        logging.error(
            "🛸 { agent %s:%d (%s) <-> client %s:%d } Something crazy happened",
            *agent_peername, agent.token, *client_peername
        )

        agent.writer.close()
        writer.close()

        await asyncio.gather(
            agent.writer.wait_closed(),
            writer.wait_closed(),
            return_exceptions=True
        )


async def shutdown(loop, servers: tuple):
    logging.debug("️( -_•)︻デ═一💥 killing...")

    await asyncio.sleep(0.5)  # Allow logs to flush

    closing_sockets: list[typing.Coroutine] = []

    while await context.length() != 0:
        agent = await context.pop_agent()

        if agent:
            agent.writer.close()
            closing_sockets.append(agent.writer.wait_closed())

    for server in servers:
        server.close()
        closing_sockets.append(server.wait_closed())

    tasks = {t for t in asyncio.all_tasks() if t is not asyncio.current_task()}

    for task in tasks:
        task.cancel()

    await asyncio.gather(*closing_sockets, *tasks, return_exceptions=True)
    loop.call_soon(loop.stop)


async def main():
    args = parse_args()
    loop = asyncio.get_running_loop()

    logging.basicConfig(level=args.log_level, format="%(message)s")

    ssl_context = None
    if args.ssl:
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_context.load_cert_chain(certfile=args.cert, keyfile=args.key)

    servers = await asyncio.gather(
        asyncio.start_server(
            handle_liveness, args.host, args.port_liveness
        ),

        asyncio.start_server(
            handle_readiness, args.host, args.port_readiness
        ),

        asyncio.start_server(
            lambda r, w: handle_agent(r, w, args.allow_agent, args.ping_pong_interval),
            args.host,
            args.port_agents,
            ssl=ssl_context
        ),

        asyncio.start_server(
            lambda r, w: handle_client(r, w, args.allow_client, args.wait_agent_max_tries, args.wait_agent_retry_interval),
            args.host,
            args.port_clients,
            ssl=ssl_context
        )
    )

    if args.zombies_clean_interval > 0:
        asyncio.create_task(context.clean_zombies(args.zombies_clean_interval))

    try:
        logging.info("三三ᕕ( ᐛ )ᕗ")
        await asyncio.Event().wait()

    except asyncio.CancelledError:
        logging.debug("🛑 Proxy was cancelled.")
        await shutdown(loop, servers)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.debug("⌨️ Interrupted by user.")
