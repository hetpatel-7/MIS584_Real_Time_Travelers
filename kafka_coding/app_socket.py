import socket
import asyncio
from tl_utils.constants import SOCKET_SERVER_HOST, SOCKET_SERVER_PORT
import selectors
import types


def accept_wrapper(sock, sel):
    conn, addr = sock.accept()  # Should be ready to read
    print(f"Accepted connection from {addr}")
    conn.setblocking(False)
    data = types.SimpleNamespace(addr=addr, inb=b"", outb=b"")
    events = selectors.EVENT_READ | selectors.EVENT_WRITE
    sel.register(conn, events, data=data)

def service_connection(key, mask, sel):
    sock = key.fileobj
    data = key.data
    if mask & selectors.EVENT_READ:
        recv_data = sock.recv(1024)  # Should be ready to read
        if recv_data:
            data.outb += recv_data
        else:
            print(f"Closing connection to {data.addr}")
            sel.unregister(sock)
            sock.close()
    if mask & selectors.EVENT_WRITE:
        if data.outb:
            print(f"Echoing {data.outb!r} to {data.addr}")
            sent = sock.send(data.outb)  # Should be ready to write
            data.outb = data.outb[sent:]

def start_socket_listening():
    # From Jennings (n.d.) https://realpython.com/python-sockets/

    sel = selectors.DefaultSelector()
    # Listening socket
    lsock = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    )
    # Bind the server to the host and port
    lsock.bind(
        (SOCKET_SERVER_HOST, SOCKET_SERVER_PORT)
    )
    # Start listening
    lsock.listen()
    print(f"Listening on {(SOCKET_SERVER_HOST, SOCKET_SERVER_PORT)}")
    # Calls made to this socket will no longer block.
    lsock.setblocking(False)
    # This registers the socket to be monitored.
    sel.register(lsock, selectors.EVENT_READ, data=None)

    try:
        while True:
            events = sel.select(timeout=None)
            for key, mask in events:
                if key.data is None:
                    accept_wrapper(key.fileobj, sel)
                else:
                    service_connection(key, mask, sel)
    except KeyboardInterrupt:
        print("Caught keyboard interrupt, exiting")
    finally:
        sel.close()
            