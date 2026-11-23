#!/usr/bin/env python3
import argparse
import json
import threading
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--sku", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--count", type=int, default=10)
    args = parser.parse_args()
    barrier = threading.Barrier(args.count)

    def create(index):
        body = json.dumps({
            "customer_id": f"chaos-{args.label}-{index}",
            "amount_cents": 19900,
            "items": [{"sku": args.sku, "quantity": 1}],
        }).encode()
        request = urllib.request.Request(
            args.url + "/orders",
            data=body,
            method="POST",
            headers={"Content-Type": "application/json", "Idempotency-Key": str(uuid.uuid4())},
        )
        barrier.wait(timeout=10)
        with urllib.request.urlopen(request, timeout=15) as response:
            if response.status != 202:
                raise RuntimeError(f"order {index}: HTTP {response.status}")
            return json.load(response)["order_id"]

    with ThreadPoolExecutor(max_workers=args.count) as pool:
        order_ids = list(pool.map(create, range(args.count)))
    print("\n".join(order_ids))


if __name__ == "__main__":
    main()
