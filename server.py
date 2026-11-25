"""
Багатопотоковий чат-сервер (TCP, сокети, потоки).

Що робить сервер:
  * створює серверний сокет на 127.0.0.1 і вільний порт (за замовчуванням порт 0 -
    операційна система сама вибирає довільний вільний порт, сервер його виводить);
  * у циклі приймає підключення (accept) і для КОЖНОГО клієнта запускає окремий потік;
  * зберігає підключених клієнтів у СПІЛЬНОМУ списку, доступ до якого захищено
    threading.Lock (додавання, видалення, знімок для розсилки);
  * пересилає повідомлення кожного клієнта всім ІНШИМ підключеним клієнтам;
  * коректно обробляє несподівані відключення (скидання з'єднання, обрив, помилка
    відправлення) - клієнт видаляється зі списку рівно один раз;
  * пише події (підключення, повідомлення, відключення) у текстовий файл через
    ChatLogger з власним Lock, щоб рядки з різних потоків не перемішувались;
  * виводить свій PID і порт, щоб їх можна було знайти через netstat -ano.

Протокол (текст UTF-8, одне повідомлення = один рядок, що закінчується "\\n"):
  клієнт -> сервер:  перший рядок - нікнейм; далі - текст повідомлень;
                     /users - список учасників;  /quit - коректно відключитись
  сервер -> клієнт:  готові до показу рядки ("[нік] текст", "* нік приєднався", ...)

Запуск:
    python server.py                 # вільний порт вибере ОС
    python server.py --port 5000     # конкретний порт
    python server.py --log my.txt    # інший файл журналу
"""

from __future__ import annotations

import argparse
import os
import socket
import sys
import threading
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

HOST = "127.0.0.1"
ENCODING = "utf-8"
MAX_LINE = 4096          # максимальна довжина одного рядка від клієнта (байт)
MAX_NICK = 20
HANDSHAKE_TIMEOUT = 30   # скільки чекати нікнейм від нового з'єднання (с)
DEFAULT_LOG = Path(__file__).with_name("chat_log.txt")


def sanitize(text: str) -> str:
    """Прибирає керівні символи (\\r, \\x00, ...), щоб текст не ламав формат журналу."""
    return "".join(ch if ch == "\t" or ch.isprintable() else " " for ch in text).strip()


# --------------------------------------------------------------------------- #
#  Журнал у файл
# --------------------------------------------------------------------------- #
class ChatLogger:
    """Потокобезпечний запис подій у текстовий файл.

    Один Lock охоплює і формування запису, і запис у файл, і flush, тому кожен
    рядок потрапляє у файл цілим, а порядок рядків збігається з порядком подій.
    Дублює запис у консоль сервера.
    """

    def __init__(self, path: Path, fsync: bool = False, echo: bool = True):
        self.path = Path(path)
        self._lock = threading.Lock()
        # append - журнал накопичується між запусками; buffering=1 - построчна буферизація
        self._file = open(self.path, "a", encoding=ENCODING, buffering=1)
        self._fsync = fsync
        self._echo = echo

    def log(self, event: str, text: str = "") -> None:
        stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        thread = threading.current_thread().name
        line = f"{stamp} | {thread:<9} | {event:<10} | {text}\n"
        with self._lock:
            if self._echo:
                print(line, end="", flush=True)
            if self._file.closed:
                return
            try:
                self._file.write(line)
                self._file.flush()
                if self._fsync:
                    os.fsync(self._file.fileno())
            except OSError as exc:  # диск заповнений тощо - сервер не повинен падати
                print(f"[!] не вдалося записати в журнал: {exc}", file=sys.stderr)

    def close(self) -> None:
        with self._lock:
            if not self._file.closed:
                self._file.close()


# --------------------------------------------------------------------------- #
#  Клієнт на боці сервера
# --------------------------------------------------------------------------- #
@dataclass(eq=False)  # eq=False: порівняння за ідентичністю об'єкта (потрібно для list.remove)
class ClientConn:
    sock: socket.socket
    addr: tuple
    nick: str = ""
    # Різні потоки можуть одночасно слати в один і той самий сокет (розсилка
    # від двох відправників) - без цього Lock'а байти двох повідомлень змішаються.
    send_lock: threading.Lock = field(default_factory=threading.Lock)

    @property
    def addr_text(self) -> str:
        return f"{self.addr[0]}:{self.addr[1]}"

    def send(self, text: str) -> bool:
        """Надсилає один рядок. False - якщо з'єднання вже непридатне."""
        data = (text + "\n").encode(ENCODING)
        with self.send_lock:
            try:
                self.sock.sendall(data)
                return True
            except OSError:
                return False

    def close(self) -> None:
        try:
            self.sock.shutdown(socket.SHUT_RDWR)  # будить потік, що чекає в recv
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass


# --------------------------------------------------------------------------- #
#  Сервер
# --------------------------------------------------------------------------- #
class ChatServer:
    def __init__(self, host: str, port: int, logger: ChatLogger):
        self.host = host
        self.logger = logger
        self.clients: list[ClientConn] = []          # СПІЛЬНИЙ список клієнтів
        self.clients_lock = threading.Lock()         # ... і його замок
        self._stop = threading.Event()
        self._counter = 0                            # лічильник для імен потоків
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if os.name != "nt":
            # На Windows SO_REUSEADDR дозволяє "перехопити" чужий порт - не ставимо.
            self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind((host, port))            # port=0 -> ОС вибирає вільний
        self._listener.listen()
        self.port = self._listener.getsockname()[1]

    # ---- робота зі спільним списком (усе - під clients_lock) ---------------
    def add_client(self, conn: ClientConn, wanted_nick: str) -> str:
        """Додає клієнта; робить нікнейм унікальним (alice -> alice_2)."""
        base = sanitize(wanted_nick).replace(" ", "_")[:MAX_NICK] or "anon"
        with self.clients_lock:
            taken = {c.nick for c in self.clients}
            nick, n = base, 1
            while nick in taken:
                n += 1
                nick = f"{base}_{n}"
            conn.nick = nick
            self.clients.append(conn)
        return nick

    def remove_client(self, conn: ClientConn) -> bool:
        """Видаляє клієнта. True - лише для того, хто видалив його першим."""
        with self.clients_lock:
            try:
                self.clients.remove(conn)
            except ValueError:
                return False
            return True

    def snapshot(self) -> list[ClientConn]:
        """Копія списку: далі по ній можна ітерувати без утримання замка."""
        with self.clients_lock:
            return list(self.clients)

    # ---- розсилка ---------------------------------------------------------
    def broadcast(self, text: str, exclude: ClientConn | None = None) -> None:
        # Замок утримується лише на час копіювання списку. Мережеві виклики
        # (sendall може блокуватись) під замком робити не можна.
        for client in self.snapshot():
            if client is exclude:
                continue
            if not client.send(text):
                self.finish_client(client, "DISCONNECT", "помилка надсилання")

    def finish_client(self, conn: ClientConn, event: str, detail: str) -> None:
        """Єдина точка завершення клієнта: видалення зі списку + журнал + сповіщення.

        Може викликатись одночасно з двох місць (потік клієнта і розсилка, яка
        наштовхнулась на мертвий сокет). Завдяки значенню, яке повертає
        remove_client, подія буде оброблена тільки раз.
        """
        was_present = self.remove_client(conn)
        conn.close()
        if not was_present:
            return
        shown = conn.nick
        if event == "LEAVE":
            self.logger.log("LEAVE", f"{shown} ({conn.addr_text}) - {detail}")
            self.broadcast(f"* {shown} залишив чат", exclude=conn)
        else:
            self.logger.log("DISCONNECT", f"{shown} ({conn.addr_text}) - {detail}")
            self.broadcast(f"* {shown} відключився (обрив з'єднання)", exclude=conn)

    # ---- обробка одного клієнта (виконується в окремому потоці) ------------
    @staticmethod
    def _readline(reader) -> str | None:
        """Читає рядок з сокета. None - сторона закрила з'єднання."""
        raw = reader.readline(MAX_LINE)
        if not raw:
            return None
        if not raw.endswith(b"\n") and len(raw) >= MAX_LINE:
            # занадто довгий рядок: відкидаємо залишок, щоб не розсинхронізуватись
            while True:
                rest = reader.readline(MAX_LINE)
                if not rest or rest.endswith(b"\n"):
                    break
        return sanitize(raw.decode(ENCODING, errors="replace"))

    def handle_client(self, conn: ClientConn) -> None:
        self.logger.log("CONNECT", conn.addr_text)
        reader = conn.sock.makefile("rb")
        joined = False
        event, detail = "DISCONNECT", "з'єднання закрито без /quit"
        try:
            conn.sock.settimeout(HANDSHAKE_TIMEOUT)
            first = self._readline(reader)
            conn.sock.settimeout(None)
            if first is None:
                detail = "закрив з'єднання до передачі нікнейму"
                return                                      # запис у журнал - у finally
            nick = self.add_client(conn, first)
            joined = True
            self.logger.log("JOIN", f"{nick} ({conn.addr_text}), онлайн: {len(self.snapshot())}")
            conn.send(f"Вітаю, {nick}! Команди: /users - хто онлайн, /quit - вийти.")
            self.broadcast(f"* {nick} приєднався до чату", exclude=conn)

            while not self._stop.is_set():
                line = self._readline(reader)
                if line is None:
                    break                                   # обрив без /quit
                if not line:
                    continue
                if line == "/quit":
                    event, detail = "LEAVE", "команда /quit"
                    break
                if line == "/users":
                    names = ", ".join(c.nick for c in self.snapshot())
                    conn.send(f"* онлайн: {names}")
                    continue
                self.logger.log("MSG", f"{nick}: {line}")
                self.broadcast(f"[{nick}] {line}", exclude=conn)
        except socket.timeout:
            detail = f"не надіслав нікнейм за {HANDSHAKE_TIMEOUT} с"
        except OSError as exc:                              # RST, обрив мережі тощо
            detail = f"помилка з'єднання: {type(exc).__name__}"
        finally:
            try:
                reader.close()
            except OSError:
                pass
            if joined:
                self.finish_client(conn, event, detail)
            else:
                conn.close()
                self.logger.log("DROPPED", f"{conn.addr_text} - {detail}")

    # ---- головний цикл ----------------------------------------------------
    def serve_forever(self) -> None:
        self.logger.log("SERVER", f"слухаю {self.host}:{self.port}, PID {os.getpid()}")
        # accept() із таймаутом: у Windows блокуючий accept не переривається Ctrl+C
        self._listener.settimeout(0.5)
        while not self._stop.is_set():
            try:
                sock, addr = self._listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break                                       # listener закрито в stop()
            self._counter += 1
            conn = ClientConn(sock, addr)
            threading.Thread(target=self.handle_client, args=(conn,),
                             name=f"client-{self._counter}", daemon=True).start()

    def stop(self) -> None:
        if self._stop.is_set():
            return
        self._stop.set()
        try:
            self._listener.close()
        except OSError:
            pass
        for client in self.snapshot():
            client.send("* сервер зупиняється")
            if self.remove_client(client):
                self.logger.log("LEAVE", f"{client.nick} ({client.addr_text}) - сервер зупинено")
            client.close()
        self.logger.log("SERVER", "зупинено")


def main() -> None:
    parser = argparse.ArgumentParser(description="Чат-сервер (TCP, потоки)")
    parser.add_argument("--host", default=HOST, help=f"адреса (за замовчуванням {HOST})")
    parser.add_argument("--port", type=int, default=0,
                        help="порт; 0 (за замовчуванням) - довільний вільний")
    parser.add_argument("--log", default=str(DEFAULT_LOG), help="файл журналу")
    parser.add_argument("--fsync", action="store_true",
                        help="після кожного запису вимагати від ОС фізичний запис на диск")
    args = parser.parse_args()

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding=ENCODING, errors="replace")

    logger = ChatLogger(Path(args.log), fsync=args.fsync)
    server = ChatServer(args.host, args.port, logger)
    print(f"PID сервера: {os.getpid()}")
    print(f"Адреса:      {args.host}:{server.port}")
    print(f"Журнал:      {logger.path}")
    print("Ctrl+C - зупинити сервер\n", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nОтримано Ctrl+C, зупиняю сервер...")
    finally:
        server.stop()
        logger.close()


if __name__ == "__main__":
    main()
