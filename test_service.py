import json
import os
import sys

import requests


SERVICE_URL = os.getenv("SERVICE_URL", "http://127.0.0.1:8000")
HEADERS = {"Content-type": "application/json", "Accept": "text/plain"}
LOG_PATH = "test_service.log"


class Tee:
    def __init__(self, *streams):
        self.streams = streams

    def write(self, text):
        for stream in self.streams:
            stream.write(text)
            stream.flush()

    def flush(self):
        for stream in self.streams:
            stream.flush()


def post(path, params):
    response = requests.post(
        SERVICE_URL + path,
        headers=HEADERS,
        params=params,
        timeout=30,
    )
    if response.status_code == 200:
        return response.json()

    print(f"status code: {response.status_code}")
    print(response.text)
    return None


def print_result(name, result):
    print(f"\n{name}")
    print(json.dumps(result, ensure_ascii=False, indent=2))


def main():
    try:
        response = requests.get(SERVICE_URL + "/health", timeout=30)
        response.raise_for_status()
    except requests.RequestException as error:
        print(f"Recommendation service is unavailable: {error}")
        print(
            "Start it first: "
            "uvicorn recommendations_service:app --host 0.0.0.0 --port 8000"
        )
        raise SystemExit(1)

    print_result("Service health", response.json())

    # 1. Пользователь без персональных рекомендаций.
    result = post(
        "/recommendations",
        {"user_id": 2000000, "k": 5},
    )
    print_result("User without personal recommendations", result)

    # 2. Пользователь с персональными рекомендациями, но без online-истории.
    result = post(
        "/recommendations",
        {"user_id": 1, "k": 5},
    )
    print_result("User with personal recommendations without history", result)

    # 3. Добавляем online-событие и получаем смешанные рекомендации.
    result = post(
        "/events",
        {"user_id": 0, "item_id": 26},
    )
    print_result("Put online event", result)

    result = post(
        "/recommendations",
        {"user_id": 0, "k": 10},
    )
    print_result("User with personal recommendations and online history", result)


if __name__ == "__main__":
    with open(LOG_PATH, "w", encoding="utf-8") as log_file:
        original_stdout = sys.stdout
        original_stderr = sys.stderr
        sys.stdout = Tee(original_stdout, log_file)
        sys.stderr = Tee(original_stderr, log_file)
        try:
            main()
        finally:
            sys.stdout = original_stdout
            sys.stderr = original_stderr
