import logging
import random
import threading
import socket

import forward


HOST = '0.0.0.0'
AGENT_PORT = 3333
CLIENT_PORT = 4444


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


agents_lock = threading.Lock()
agents: dict[socket.socket, tuple[str, int]] = {}


def handle_agent(conn: socket.socket, addr: tuple[str, int]):
    print(f"New agent connected: {addr=}")

    with agents_lock:  # Acquire the lock before adding a new agent connection
        agents[conn] = addr


def handle_client(conn, addr):
    logging.info(f"New client connected: {addr}")

    with agents_lock:
        if len(agents) == 0:
            logging.error("No agent connected!")
            conn.close()
            return

        agent_conn, agent_addr = agents.popitem()

    forward.bidirectional(conn, agent_conn)


def websocker_server(host, port, handler):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))  # Bind server to port
    server.listen()  # Listen for connections

    print(f"Server listening on {host}:{port}")

    while True:
        conn, addr = server.accept()  # Accept a new client
        thread = threading.Thread(target=handler, args=(conn, addr))
        thread.start()


def create_websocket_server(host, port, handler):
    thread_websocket_server = threading.Thread(target=websocker_server, args=(host, port, handler))
    thread_websocket_server.start()
    return thread_websocket_server


def main():
    thread_agent_websocket_server = create_websocket_server(HOST, AGENT_PORT, handle_agent)
    thread_client_websocket_server = create_websocket_server(HOST, CLIENT_PORT, handle_client)

    thread_client_websocket_server.join()
    thread_agent_websocket_server.join()

    for agent_conn in agents:
        agent_conn.close()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Shutting down server...")