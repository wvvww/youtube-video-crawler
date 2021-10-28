import argparse

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-w", "--workers",
        type=int,
        default=8)
    parser.add_argument(
        "-t", "--threads",
        type=int,
        default=50)
    parser.add_argument(
        "-p", "--proxy-file",
        required=True,
        type=argparse.FileType("r", encoding="UTF-8", errors="ignore"))
    parser.add_argument(
        "-q", "--queue-file",
        required=True,
        type=argparse.FileType("r", encoding="UTF-8", errors="ignore"))
    parser.add_argument(
        "--redis-host",
        type=str,
        default="127.0.0.1")
    parser.add_argument(
        "--redis-port",
        type=str,
        default=6379)
    parser.add_argument(
        "--redis-password",
        type=str,
        default=None)
    parser.add_argument(
        "--redis-db",
        type=int,
        default=6)
    return parser.parse_args()