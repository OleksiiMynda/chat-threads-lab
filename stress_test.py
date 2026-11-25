"""
Автоматична перевірка чату: сервер запускається в цьому ж процесі (в окремому
потоці), до нього підключаються боти-клієнти.

Сценарій:
  * N "стабільних" ботів одночасно надсилають по M повідомлень кожен;
  * K "аварійних" ботів надсилають кілька повідомлень і потім рвуть з'єднання
    без /quit (TCP RST через SO_LINGER=0);
  * після цього перевіряється:
      1. кожен стабільний бот отримав усі (N-1)*M повідомлень інших стабільних;
      2. спільний список клієнтів на сервері порожній (ніхто не "завис" у ньому);
      3. файл журналу: усі рядки цілі, формат не зіпсовано, лічильники подій сходяться.

Запуск:  python stress_test.py [--stable 8] [--crashing 3] [--messages 50]
"""

from __future__ import annotations

import argparse
import re
import socket
import struct
import sys
import tempfile
import threading
import time
from pathlib import Path

from server import ChatLogger, ChatServer

HOST = "127.0.0.1"
LINE_RE = re.compile(
    r"^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3} \| [^|]+\| [A-Z]+\s*\| .*$"
)


class Bot:
    def __init__(self, name: str, port: int):
        self.name = name
        self.sock = socket.create_connection((HOST, port), timeout=5)
        self.sock.settimeout(None)
        self.received: list[str] = []
        self._thread = threading.Thread(target=self._recv, daemon=True)
        self._thread.start()
        self.send(name)

    def _recv(self) -> None:
        # recv замість makefile: відкритий makefile утримує сокет, і close() не
        # закрив би його по-справжньому (RST не було б надіслано).
        buf = b""
        try:
            while chunk := self.sock.recv(4096):
                buf += chunk
                *done, buf = buf.split(b"\n")
                self.received.extend(d.decode("utf-8") for d in done)
        except OSError:
            pass

    def send(self, text: str) -> None:
        self.sock.sendall((text + "\n").encode("utf-8"))

    def quit(self) -> None:
        self.send("/quit")
        self._thread.join(timeout=3)   # сервер сам закриє з'єднання у відповідь
        self.sock.close()

    def crash(self) -> None:
        """Різке закриття: замість FIN надсилається RST (як при падінні процесу)."""
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        self.sock.close()


def wait_until(predicate, timeout: float = 5.0) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stable", type=int, default=8)
    parser.add_argument("--crashing", type=int, default=3)
    parser.add_argument("--messages", type=int, default=50)
    args = parser.parse_args()
    n, k, m = args.stable, args.crashing, args.messages

    log_path = Path(tempfile.mkdtemp()) / "test_chat_log.txt"
    logger = ChatLogger(log_path, echo=False)
    server = ChatServer(HOST, 0, logger)
    threading.Thread(target=server.serve_forever, name="acceptor", daemon=True).start()
    print(f"Сервер у тестовому режимі: {HOST}:{server.port}, журнал: {log_path}")

    stable = [Bot(f"s{i}", server.port) for i in range(n)]
    crashing = [Bot(f"x{i}", server.port) for i in range(k)]
    ok = wait_until(lambda: len(server.snapshot()) == n + k)
    print(f"[{'OK' if ok else 'FAIL'}] підключено {len(server.snapshot())} з {n + k} клієнтів")
    failed = not ok

    def burst(bot: Bot, count: int) -> None:
        for i in range(count):
            bot.send(f"{bot.name} msg {i}")

    threads = [threading.Thread(target=burst, args=(b, m)) for b in stable]
    threads += [threading.Thread(target=burst, args=(b, 5)) for b in crashing]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for bot in crashing:
        bot.crash()                                   # несподіване відключення

    expected = (n - 1) * m
    from_stable = lambda bot: sum(
        1 for r in bot.received if re.match(r"^\[s\d+\] s\d+ msg \d+$", r)
    )
    wait_until(lambda: all(from_stable(b) >= expected for b in stable), timeout=10)
    bad = [(b.name, from_stable(b)) for b in stable if from_stable(b) != expected]
    print(f"[{'FAIL' if bad else 'OK'}] кожен стабільний бот отримав {expected} повідомлень "
          f"від інших стабільних" + (f"; відхилення: {bad}" if bad else ""))
    failed |= bool(bad)

    for bot in stable:
        bot.quit()
    ok = wait_until(lambda: len(server.snapshot()) == 0)
    print(f"[{'OK' if ok else 'FAIL'}] спільний список клієнтів після відключення всіх: "
          f"{len(server.snapshot())} (очікується 0)")
    failed |= not ok

    server.stop()
    logger.close()

    lines = log_path.read_text(encoding="utf-8").splitlines()
    broken = [ln for ln in lines if not LINE_RE.match(ln)]
    count = lambda ev: sum(1 for ln in lines if re.search(rf"\| {ev}\s*\|", ln))
    msgs_stable = sum(1 for ln in lines if re.search(r"\| MSG\s*\| s\d+: s\d+ msg \d+$", ln))
    print(f"[{'FAIL' if broken else 'OK'}] журнал: {len(lines)} рядків, пошкоджених: {len(broken)}")
    for ln in broken[:5]:
        print("    пошкоджений рядок:", repr(ln))
    print(f"[{'OK' if msgs_stable == n * m else 'FAIL'}] MSG від стабільних у журналі: "
          f"{msgs_stable} (очікується {n * m})")
    print(f"[{'OK' if count('CONNECT') == n + k else 'FAIL'}] CONNECT: {count('CONNECT')}, "
          f"JOIN: {count('JOIN')}, LEAVE: {count('LEAVE')}, DISCONNECT: {count('DISCONNECT')}")
    failed |= bool(broken) or msgs_stable != n * m or count("CONNECT") != n + k
    # кожен вхід має рівно одне завершення: LEAVE (/quit) або DISCONNECT (обрив)
    ends = count("LEAVE") + count("DISCONNECT")
    print(f"[{'OK' if ends == count('JOIN') else 'FAIL'}] JOIN = LEAVE + DISCONNECT "
          f"({count('JOIN')} = {ends}) - ніхто не оброблений двічі й не загублений")
    failed |= ends != count("JOIN")

    print("\nРЕЗУЛЬТАТ:", "ПОМИЛКИ" if failed else "усі перевірки пройдені")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
