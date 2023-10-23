import os
import re
import glob
import logging
import subprocess

from string import Template
from knobtool import constants as my_constants
from knobtool.workload.basic_workload import BasicWorkload


class Benchbase(BasicWorkload):
    def __init__(
        self, is_load=False, metric="through", workload=None, args=None
    ) -> None:
        self.metric = metric
        self.workload = str(workload).lower()
        self.result_path = None
        self.tmp_xml_path = None
        self.args = args
        # self_execute
        self.is_load = is_load

    def _produce_xml(self):
        # add absolute path base & xml & result

        self.base = my_constants.WORKLOAD_TOOL_PATH
        self.result_path = os.path.join(
            self.base, f"results/{self.workload}_{self.args['db_name']}"
        )
        self.tmp_xml_path = os.path.join(
            self.base, f"tmp/sample_{self.workload}_config.xml"
        )

        with open(
            os.path.join(self.base, f"templates_xml/sample_{self.workload}_config.xml"),
            "r",
        ) as f:
            src = Template(f.read())
            context = src.substitute(self.args)
        with open(self.tmp_xml_path, "w") as f:
            f.write(context)

        ## RM COMMAND clear old data before run
        rm_cmd = "sudo -S rm -rf " + self.result_path
        _ = subprocess.run(
            rm_cmd,
            shell=True,
            input=f"{my_constants.SYS_PASSWD}\n",
            capture_output=True,
            text=True,
            timeout=None,
        )

    def _execute(self):
        current_path = os.getcwd()
        benchbase_run_cmd = (
            f"cd {os.path.join(current_path, self.base)} && "
            + f"java -jar benchbase.jar "
            + f"-b {self.workload} -c {os.path.join(current_path,self.tmp_xml_path)} "
            + f"-d  {os.path.join(current_path,self.result_path)} "
            + f"--clear=true --create=true --load=true --execute=true"
        )
        collection = subprocess.run(
            benchbase_run_cmd,
            shell=True,
            input=f"{my_constants.SYS_PASSWD}\n",
            capture_output=True,
            text=True,
            timeout=None,
        )

        if collection.stderr != "":
            print(collection.stderr)

    def _retrieve_file(self):
        file_list = sorted(list(glob.glob(os.path.join(self.result_path, "*summary*"))))
        if len(file_list) == 0:
            # no test data found
            print(self.result_path, "is not found")
            return ""
        with open(file_list[-1], "r") as f:
            context = f.read()
        return context

    def _retrieve(self):
        metrics_list = [
            "min",
            "med",
            "max",
            "avg",
            "late_95",
            "late_25",
            "late_90",
            "late_99",
            "late_75",
            "through",
        ]

        re_dict = {
            "min": r'"Minimum Latency \(microseconds\)":\s*(\d+)',
            "med": r'"Median Latency \(microseconds\)":\s*(\d+)',
            "max": r'"Maximum Latency \(microseconds\)":\s*(\d+)',
            "avg": r'"Average Latency \(microseconds\)":\s*(\d+)',
            "late_95": r'"95th Percentile Latency \(microseconds\)":\s*(\d+)',
            "late_25": r'"25th Percentile Latency \(microseconds\)":\s*(\d+)',
            "late_90": r'"90th Percentile Latency \(microseconds\)":\s*(\d+)',
            "late_99": r'"99th Percentile Latency \(microseconds\)":\s*(\d+)',
            "late_75": r'"75th Percentile Latency \(microseconds\)":\s*(\d+)',
            "through": r'"Throughput \(requests/second\)":\s*(\d+.\d+)',
        }

        metric_groups = {metric_name: [] for metric_name in metrics_list}

        benchbase_text = self._retrieve_file()
        logging.info(f"container {self.ogdb.name} test completed, start extracting")

        for metric_name, regular_express in re_dict.items():
            tmp = re.findall(regular_express, benchbase_text)
            if len(tmp) != 0:
                metric_groups[metric_name].append(float(tmp[0]))

        result = {}
        for metric_name, metric_values in metric_groups.items():
            result[metric_name] = (
                None
                if len(metric_values) == 0
                else round(sum(metric_values) / len(metric_values), 2)
            )

        return result

    def evaluate(self):
        self._produce_xml()
        self._execute()
        result = self._retrieve()
        return result
