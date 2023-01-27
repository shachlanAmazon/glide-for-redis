import csv
import json
import os
import sys

output_file_name = sys.argv[-1]
with open(output_file_name, "w+") as output_file:
    writer = csv.writer(output_file)
    base_fields = [
        "language",
        "client",
        "num_of_tasks",
        "data_size",
        "tps",
        "read_p50_latency",
        "read_p90_latency",
        "read_p99_latency",
        "read_average_latency",
        "read_std_dev_latency",
        "read_p50_size",
        "read_p90_size",
        "read_p99_size",
        "read_average_size",
        "read_std_dev_size",
        "write_p50_size",
        "write_p90_size",
        "write_p99_size",
        "write_average_size",
        "write_std_dev_size",
        "write_p50_latency",
        "write_p90_latency",
        "write_p99_latency",
        "write_average_latency",
        "write_std_dev_latency",
    ]

    writer.writerow(base_fields)

    for json_file_full_path in sys.argv[1:-1]:
        with open(json_file_full_path) as file:
            json_objects = json.load(file)

            json_file_name = os.path.basename(json_file_full_path)

            languages = ["csharp", "node", "python"]
            language = next(
                (language for language in languages if language in json_file_name), None
            )

            if not language:
                raise "Unknown language for " + json_file_name
            for json_object in json_objects:
                json_object["language"] = language
                values = [json_object[field] for field in base_fields]
                writer.writerow(values)

for json_file_full_path in sys.argv[1:-1]:
    os.remove(json_file_full_path)
