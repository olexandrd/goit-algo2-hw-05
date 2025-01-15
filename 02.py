import timeit
import json
import re
from datasketch import HyperLogLog
from multiprocessing import Pool, cpu_count


hll = HyperLogLog(p=12)


def load_data(file_path):
    items = []
    with open(file_path, "r", encoding="UTF-8") as file:
        readlines = file.readlines()
        for line in readlines:
            items.append(json.loads(line))
    return items


def process_line(line):
    item = re.search(r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}", line)
    if item:
        return item.group()
    return None


def count_unique_ip_by_set(file_path):
    unique_ips = set()
    with open(file_path, "r", encoding="UTF-8") as file:
        with Pool(cpu_count()) as pool:
            results = pool.imap(process_line, file, chunksize=1000)
            for result in results:
                if result:
                    unique_ips.add(result)

    return len(unique_ips)


def count_unique_ip_by_hll(file_path):
    with open(file_path, "r", encoding="UTF-8") as file:
        with Pool(cpu_count()) as pool:
            results = pool.imap(process_line, file, chunksize=1000)
            for result in results:
                if result:
                    hll.update(result.encode("utf-8"))
    return hll.count()


def measure_time(func, data):
    execution_time = timeit.timeit(lambda: func(data), number=5)
    return execution_time


def main():
    # data = load_data("lms-stage-access.log")
    print("Результати порівняння:")
    # print(count_unique_ip_by_hll("lms-stage-access.log"))
    # print(count_unique_ip_by_set("lms-stage-access.log"))
    set_execution_time = measure_time(count_unique_ip_by_set, "lms-stage-access.log")
    hll_execution_time = measure_time(count_unique_ip_by_hll, "lms-stage-access.log")
    print(
        "За допомогою множини: кількість унікальних IP -",
        count_unique_ip_by_set("lms-stage-access.log"),
        "шт.",
        "час виконання -",
        set_execution_time,
        "сек.",
    )
    print(
        "За допомогою HyperLogLog: кількість унікальних IP -",
        count_unique_ip_by_hll("lms-stage-access.log"),
        "шт.",
        "час виконання -",
        hll_execution_time,
        "сек.",
    )


if __name__ == "__main__":
    main()
