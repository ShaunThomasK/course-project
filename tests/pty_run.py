#!/usr/bin/env python3
"""Runs a program in a pseudo-terminal, like a person at a real terminal.

    pty_run.py OUTPUT_FILE PROGRAM [ARGS...]  < commands

Everything the program prints is appended to OUTPUT_FILE as raw bytes.
Lines read from stdin are instructions:

    TYPE <text>   type text without pressing Enter
    LINE <text>   type text and press Enter
    CTRL_C        press Ctrl+C
    QUIT          stop the program and exit
"""
import os
import pty
import select
import signal
import sys


def main():
    out = open(sys.argv[1], "ab", buffering=0)
    pid, fd = pty.fork()
    if pid == 0:
        os.execv(sys.argv[2], sys.argv[2:])

    stdin = sys.stdin.fileno()
    pending = b""
    while True:
        ready, _, _ = select.select([fd, stdin], [], [], 0.5)
        if fd in ready:
            try:
                data = os.read(fd, 4096)
            except OSError:
                break
            if not data:
                break
            out.write(data)
        if stdin in ready:
            chunk = os.read(stdin, 4096)
            if not chunk:
                break
            pending += chunk
            while b"\n" in pending:
                line, pending = pending.split(b"\n", 1)
                cmd, _, arg = line.partition(b" ")
                if cmd == b"TYPE":
                    os.write(fd, arg)
                elif cmd == b"LINE":
                    os.write(fd, arg + b"\n")
                elif cmd == b"CTRL_C":
                    os.write(fd, b"\x03")
                elif cmd == b"QUIT":
                    os.kill(pid, signal.SIGKILL)
                    return
    os.kill(pid, signal.SIGKILL)


if __name__ == "__main__":
    main()
