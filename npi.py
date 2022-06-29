#!/usr/bin/env python3

import argparse
import csv
import os
import requests
import simplejson as json
import sys

class doctors:
    def __init__(self):
        self.csv_headings = []
        self.data = []

    def doctor_list(self):
        return self.data

    def directory(self, path):
        if not os.path.isfile(path):
            print("Error: file does not exist: {}".format(path))
            sys.exit(1)

        with open(path, mode="r", newline="") as file:
            if csv.Sniffer().sniff(file.read(1024)):
                self._read_csv(path)
            else:
                self._read_excel(file)

        self.csv_headings = list(self.data[0].keys())
        self._clean_data()
        self._add_surrogate_key()

    def export(self, file_path):
        with open(file_path, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames = self.csv_headings, restval='', extrasaction='ignore')
            writer.writeheader()
            writer.writerows(self.data)

    def insert_npi(self, results):
        self.csv_headings.append("npi")

        for row in self.data:
            for result in results:
                if row["surrogate_key"] == result["surrogate_key"]:
                    row["npi"] = result["npi"]
                    break

    def _add_surrogate_key(self):
        for count, row in enumerate(self.data):
            row["surrogate_key"] = count

    def _clean_data(self):
        for row in self.data:
            for k, v in row.items():
                row[k] = v.strip()

    def _read_csv(self, path):
        with open(path, mode="r", newline="") as csv_file:
            for line in csv.DictReader(csv_file):
                self.data.append(line)

    def _read_excel(self, path):
        pass


class npi:
    def __init__(self, registry):
        self.api = "api/?version=2.1"
        self.search_terms = []
        self.registry = registry

    def search_criteria(self, data):
        self.search_terms.clear()
        self.search_terms.extend(data)

    def query(self):
        _npi = []

        for search_term in self.search_terms:
            _query = self._construct_query(search_term)
            _response = self._run_query(_query)

            _json_response = json.loads(_response)
            _result_count = _json_response["result_count"]

            if _result_count < 1:
                print("No results found for: {} {}".format(search_term["first_name"], search_term["last_name"]))
            elif _result_count == 1:
                _npi.append(
                    {"surrogate_key": search_term["surrogate_key"], "npi": self._parse_npi(_json_response)}
                )
            elif _result_count > 1:
                print("Found {} results for: {} {}".format(_result_count, search_term["first_name"], search_term["last_name"]))
            else:
                print("Error parsing 'result_count'")

        return _npi

    def _construct_query(self, search_data):
        _first_name = "&first_name={}".format(search_data["first_name"].replace(" ","+"))
        _last_name = "&last_name={}".format(search_data["last_name"].replace(" ","+"))
        _state = "&state={}".format(search_data["state"])
        _query = "{}/{}{}{}{}".format(self.registry, self.api, _first_name, _last_name, _state)

        return _query

    def _parse_npi(self, json_response):
        return json_response["results"][0]["number"]

    def _run_query(self, query):
        _response = requests.get(query)
        _contents = _response.text

        return _contents


def main():
    doc = doctors()
    doc.directory(args.file)

    id = npi(args.registry)
    id.search_criteria(doc.doctor_list())
    results = id.query()

    doc.insert_npi(results)
    doc.export(args.export)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Query NPPES website to retrieve Doctor's NPI number.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "file",
        help = "Path to a CSV or Excel file with input data",
    )
    parser.add_argument(
        "--export",
        default = "./export.csv",
        help = "Path to exported CSV file containing NPI data",
    )
    parser.add_argument(
        "--registry",
        default = "https://npiregistry.cms.hhs.gov",
        help = "URL to NPI registry",
    )

    args = parser.parse_args()

    main()
