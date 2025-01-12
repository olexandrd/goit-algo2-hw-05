import timeit
import json
import csv
from datasketch import HyperLogLog

hll = HyperLogLog(p=12)


def load_data(file_path):
    items = []
    with open(file_path, "r", encoding="UTF-8") as file:
        readlines = file.readlines()
        for line in readlines:
            items.append(json.loads(line))
    return items


def count_unique_ip_by_set(data):
    unique_ips = set()
    for item in data:
        unique_ips.add(item["remote_addr"])
    return len(unique_ips)


def count_unique_ip_by_hll(data):
    for item in data:
        hll.update(item["remote_addr"].encode("utf-8"))
    return hll.count()


def measure_time(func, data):
    execution_time = timeit.timeit(lambda: func(data), number=100)
    return execution_time


def main():
    data = load_data("lms-stage-access.log")
    print("Результати порівняння:")
    set_execution_time = measure_time(count_unique_ip_by_set, data)
    hll_execution_time = measure_time(count_unique_ip_by_hll, data)
    print(
        "За допомогою множини: кількість унікальних IP -",
        count_unique_ip_by_set(data),
        "шт.",
        "час виконання -",
        set_execution_time,
        "сек.",
    )
    print(
        "За допомогою HyperLogLog: кількість унікальних IP -",
        count_unique_ip_by_hll(data),
        "шт.",
        "час виконання -",
        hll_execution_time,
        "сек.",
    )


if __name__ == "__main__":
    main()
