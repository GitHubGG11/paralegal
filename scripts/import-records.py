#!/usr/bin/env python3
"""Import normalized records; JSONL streams large datasets in bounded batches."""
import argparse
import json
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def records(path):
    with path.open() as file:
        if path.suffix in ('.jsonl', '.ndjson'):
            for line in file:
                if line.strip():
                    yield json.loads(line)
        else:
            data = json.load(file)
            if not isinstance(data, list):
                raise ValueError('JSON input must be an array; use JSONL for large exports')
            yield from data


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('file', type=Path)
    args = parser.parse_args()
    origin = 'https://twilio.genehuntley.com'
    password = (Path(__file__).resolve().parents[1] / '.console-password').read_text().strip()
    request = Request(origin + '/session', data=urlencode({'password': password}).encode(), headers={'Origin': origin})
    with urlopen(request, timeout=20) as response:
        cookie = response.headers['Set-Cookie'].split(';')[0]
    count = 0
    def send(batch):
        nonlocal count
        if not batch:
            return
        request = Request(origin + '/knowledge', data=json.dumps(batch).encode(), headers={
            'Origin': origin, 'Cookie': cookie, 'Content-Type': 'application/json'})
        with urlopen(request, timeout=30) as response:
            count += json.load(response)['imported']
        print(f'Imported {count} records', flush=True)
    batch = []
    for record in records(args.file):
        if len(json.dumps([record]).encode()) > 60000:
            raise ValueError('Record exceeds request size; split long documents into records')
        if len(batch) == 100 or len(json.dumps(batch + [record]).encode()) > 60000:
            send(batch)
            batch = []
        batch.append(record)
    send(batch)


if __name__ == '__main__':
    main()
