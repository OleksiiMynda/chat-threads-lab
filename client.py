"""
Консольний клієнт чату.

  * підключається до сервера (127.0.0.1:порт), першим рядком надсилає нікнейм;
  * основний потік читає ввід користувача й надсилає рядки на сервер;
  * окремий потік-приймач отримує повідомлення інших клієнтів і показує їх;
  * відключення: команда /quit, Ctrl+C або Ctrl+Z+Enter (EOF).

Запуск (порт виводить сервер):
    python client.py --port 50123
    python client.py --port 50123 --name alice
"""

from __future__ import annotations

import argparse
import os
import socket
import sys
import threading

HOST = "127.0.0.1"
ENCODING = "utf-8"
PROMPT = "> "


def receive_loop(reader, stop: threading.Event) -> None:
    """Потік-приймач: показує все, що прислав сервер."""
    try:
        for raw in reader:  # ітерація по рядках; завершується, коли сервер закрив з'єднання
            text = raw.decode(ENCODING, errors="replace").rstrip("\r\n")
            # \r повертає курсор на початок, щоб повідомлення не склеїлось з рядком вводу
            print(f"\r{text}\n{PROMPT}", end="", flush=True)
    except OSError:
        pass
    if not stop.is_set():
        # Сервер закрився/обірвався, а основний потік застряг у input().
        # У Windows input() не можна перервати з іншого потоку, тому виходимо одразу.
        print("\nЗ'єднання з сервером втрачено. Клієнт завершує роботу.", flush=True)
        os._exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Клієнт чату")
    parser.add_argument("--host", default=HOST)
    parser.add_argument("--port", type=int, required=True, help="порт сервера")
    parser.add_argument("--name", help="нікнейм (якщо не вказано - буде запитано)")
    args = parser.parse_args()

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding=ENCODING, errors="replace")

    name = args.name or input("Ваш нікнейм: ").strip() or "anon"

    try:
        sock = socket.create_connection((args.host, args.port), timeout=5)
    except OSError as exc:
        sys.exit(f"Не вдалося підключитись до {args.host}:{args.port}: {exc}")
    sock.settimeout(None)

    stop = threading.Event()
    reader = sock.makefile("rb")
    receiver = threading.Thread(target=receive_loop, args=(reader, stop), daemon=True)

    def send_line(text: str) -> None:
        sock.sendall((text + "\n").encode(ENCODING))

    try:
        send_line(name)
        receiver.start()
        while True:
            try:
                line = input(PROMPT)
            except (EOFError, KeyboardInterrupt):
                line = "/quit"
            line = line.strip()
            if not line:
                continue
            if line == "/quit":
                stop.set()              # закриття з'єднання далі - наше рішення, не помилка
                send_line(line)
                break
            send_line(line)
    except OSError as exc:
        print(f"Помилка зв'язку: {exc}")
    finally:
        stop.set()
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        sock.close()
        receiver.join(timeout=1)
        print("Відключено від сервера.")


if __name__ == "__main__":
    main()
