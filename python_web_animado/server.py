from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import socket
import webbrowser


ROOT = Path(__file__).resolve().parent


def free_port(start=8765):
    for port in range(start, start + 80):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(("127.0.0.1", port))
            except OSError:
                continue
            return port
    raise RuntimeError("Nao encontrei uma porta livre.")


def main():
    port = free_port()
    handler = partial(SimpleHTTPRequestHandler, directory=str(ROOT))
    server = ThreadingHTTPServer(("127.0.0.1", port), handler)
    url = f"http://127.0.0.1:{port}/index.html"
    print(f"Simulador aberto em: {url}")
    print("Feche esta janela para encerrar o servidor.")
    webbrowser.open(url)
    server.serve_forever()


if __name__ == "__main__":
    main()
