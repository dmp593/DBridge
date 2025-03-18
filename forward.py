import threading
from logging import error
from threading import Thread
from socket import socket


def forward(src: socket, dst: socket, buffer_size: int = 4096):
    try:
        while True:
            data = src.recv(buffer_size)

            if not data:
                break

            dst.sendall(data)

    except Exception as e:
        error("Forwarding error.")


def bidirectional(conn1: socket, conn2: socket, buffer_size: int = 4096, on_disconnect: callable = None):
    try:
        thread_conn1 = Thread(target=forward, args=(conn1, conn2, buffer_size))
        thread_conn2 = Thread(target=forward, args=(conn2, conn1, buffer_size))

        thread_conn1.start()
        thread_conn2.start()

        thread_conn1.join()
        thread_conn1.join()

        if callable(on_disconnect):
            on_disconnect(threading.current_thread())
    finally:
        conn1.close()
        conn2.close()
