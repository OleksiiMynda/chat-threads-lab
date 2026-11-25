"""
Дослідження запису в текстовий файл з декількох потоків.

Порівнюються способи запису журналу (T потоків пишуть по N записів):

  no_lock         - без Lock; запис складається з кількох write() (як у наївному логері:
                    час, потім текст, потім "\\n"), між ними потік може бути перемкнений;
  lock            - той самий запис, але під threading.Lock;
  lock_flush      - Lock + flush() після кожного запису (запис одразу іде в ОС);
  lock_fsync      - Lock + flush() + os.fsync() (чекаємо фізичного запису на диск);
  open_each       - Lock + відкриття/закриття файлу на кожен запис;
  queue_writer    - потоки лише кладуть записи в queue.Queue, один потік-писар пише у файл
                    (Lock для файлу не потрібен - файл належить одному потоку).

Для кожного способу вимірюється час і перевіряється цілісність: кожен рядок має бути
рівно у форматі "<потік> <номер> <текст>", а кількість рядків - T*N.

Між частинами запису без Lock додано time.sleep(0) ("вікно гонки"): через GIL потоки у
CPython перемикаються рідко, і без цього пошкодження проявлялись би нерегулярно
(така сама методика, як у лабораторній 1).

Запуск:  python file_io_experiment.py [--threads 8] [--records 2000]
"""

from __future__ import annotations

import argparse
import csv
import os
import queue
import re
import tempfile
import threading
import time
from pathlib import Path

LINE_RE = re.compile(r"^T\d+ #\d+ payload-[a-z]{20}$")
PAYLOAD = "payload-" + "abcdefghijklmnopqrst"


def write_parts(f, tid: int, i: int, window: bool) -> None:
    """Один запис, складений з кількох write(): місце, де без Lock рядки перемішуються."""
    f.write(f"T{tid}")
    if window:
        time.sleep(0)
    f.write(f" #{i}")
    if window:
        time.sleep(0)
    f.write(f" {PAYLOAD}")
    if window:
        time.sleep(0)
    f.write("\n")


def run_threads(target, threads: int) -> float:
    barrier = threading.Barrier(threads)

    def runner(tid: int) -> None:
        barrier.wait()
        target(tid)

    workers = [threading.Thread(target=runner, args=(t,)) for t in range(threads)]
    start = time.perf_counter()
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    return time.perf_counter() - start


def mode_no_lock(path: Path, threads: int, records: int) -> float:
    with open(path, "w", encoding="utf-8") as f:
        return run_threads(
            lambda tid: [write_parts(f, tid, i, True) for i in range(records)], threads)


def mode_lock(path: Path, threads: int, records: int, flush=False, fsync=False) -> float:
    lock = threading.Lock()
    with open(path, "w", encoding="utf-8") as f:
        def work(tid: int) -> None:
            for i in range(records):
                with lock:
                    write_parts(f, tid, i, True)
                    if flush or fsync:
                        f.flush()
                    if fsync:
                        os.fsync(f.fileno())
        return run_threads(work, threads)


def mode_open_each(path: Path, threads: int, records: int) -> float:
    lock = threading.Lock()
    path.write_text("", encoding="utf-8")

    def work(tid: int) -> None:
        for i in range(records):
            with lock:
                with open(path, "a", encoding="utf-8") as f:
                    write_parts(f, tid, i, True)
    return run_threads(work, threads)


def mode_queue_writer(path: Path, threads: int, records: int) -> float:
    q: queue.Queue = queue.Queue()
    done = object()

    def writer() -> None:
        with open(path, "w", encoding="utf-8") as f:
            while (item := q.get()) is not done:
                f.write(item)

    def work(tid: int) -> None:
        for i in range(records):
            q.put(f"T{tid} #{i} {PAYLOAD}\n")

    start = time.perf_counter()
    wt = threading.Thread(target=writer)
    wt.start()
    run_threads(work, threads)
    q.put(done)
    wt.join()                       # час включає дописування решти черги
    return time.perf_counter() - start


def check(path: Path, threads: int, records: int) -> tuple[int, int, int]:
    """(усього рядків, пошкоджених рядків, втрачених записів)."""
    lines = path.read_text(encoding="utf-8").split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    good = sum(1 for ln in lines if LINE_RE.match(ln))
    return len(lines), len(lines) - good, threads * records - good


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--records", type=int, default=2000)
    parser.add_argument("--repeat", type=int, default=3, help="скільки разів повторити вимір")
    parser.add_argument("--out", default=str(Path(__file__).with_name("results_file_io.csv")))
    args = parser.parse_args()
    t, n = args.threads, args.records

    modes = {
        "no_lock": mode_no_lock,
        "lock": mode_lock,
        "lock_flush": lambda p, a, b: mode_lock(p, a, b, flush=True),
        "lock_fsync": lambda p, a, b: mode_lock(p, a, b, fsync=True),
        "open_each": mode_open_each,
        "queue_writer": mode_queue_writer,
    }
    tmp = Path(tempfile.mkdtemp())
    rows = []
    print(f"{t} потоків x {n} записів = {t * n} очікуваних рядків; файл у {tmp}\n")
    print(f"{'спосіб':<14}{'час, мс':>10}{'рядків':>9}{'пошкодж.':>10}{'втрачено':>10}")
    for name, fn in modes.items():
        times = []
        for _ in range(args.repeat):
            path = tmp / f"{name}.txt"
            times.append(fn(path, t, n) * 1000)
            total, broken, lost = check(path, t, n)
        ms = sorted(times)[len(times) // 2]   # медіана
        rows.append([name, t, n, round(ms, 1), total, broken, lost])
        print(f"{name:<14}{ms:>10.1f}{total:>9}{broken:>10}{lost:>10}")

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["mode", "threads", "records", "median_ms", "lines", "broken", "lost"])
        w.writerows(rows)
    print(f"\nЗбережено: {args.out}")


if __name__ == "__main__":
    main()
