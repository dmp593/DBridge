import socket
import threading
import logging
import os

from forward import forward


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

PROXY_HOST = os.getenv("PROXY_HOST", "localhost")
PROXY_PORT = int(os.getenv("PROXY_PORT", 3333))

DATABASE_HOST = os.getenv("DATABASE_HOST", "localhost")
DATABASE_PORT = int(os.getenv("DATABASE_PORT", 3306))


def handle_connection(proxy_socket):
    db_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        db_socket.connect((DATABASE_HOST, DATABASE_PORT))
        logging.info(f"Connected to database at {DATABASE_HOST}:{DATABASE_PORT}")

        # Start bidirectional forwarding
        t1 = threading.Thread(target=forward, args=(proxy_socket, db_socket))
        t2 = threading.Thread(target=forward, args=(db_socket, proxy_socket))

        t1.start()
        t2.start()

        # Wait for both threads to finish
        t1.join()
        t2.join()

    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
    finally:
        proxy_socket.close()
        db_socket.close()


def main():
    proxy_socket = None

    while True:
        try:
            proxy_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            proxy_socket.connect((PROXY_HOST, PROXY_PORT))
            logging.info(f"Connected to proxy server at {PROXY_HOST}:{PROXY_PORT}")

            handle_connection(proxy_socket)

            # Wait here until disconnected
            threading.Event().wait()
        except Exception as e:
            logging.error(f"Connection error: {e}")

            if proxy_socket:
                proxy_socket.close()


if __name__ == "__main__":
    main()
