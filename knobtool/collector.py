import csv
import logging

from knobtool.knobs_manager import Knobs
from knobtool.database.basicdb import BasicDB
from knobtool.workload.basic_workload import BasicWorkload
from knobtool.sample_policy import bucket_policy, lhs_policy


class Collector:
    """
    collector, sample knob configs and collect data.
    """

    def __init__(
        self, knobs: Knobs, database: BasicDB, workload: BasicWorkload, args=None
    ) -> None:
        if knobs == None:
            raise ValueError(f"please input knobs")

        # init knobs info from knobs csv and candidates file
        self.knobs = knobs
        self.database = database
        self.workload = workload
        self.metric = workload.metric
        self.args = args

        # knobs_list
        self.config_results = []  # cover by sample

        # define sample methods
        if args["sample_policy"] == "lhs":
            self.sample_method = lhs_policy

        elif args["sample_policy"] == "bucket":
            self.sample_method = bucket_policy
        else:
            raise ValueError

    def sample(self, num):
        knobs = self.knobs
        candidates_info = knobs.candidates_info

        float_cans = [ci for ci in candidates_info if ci["category"] == "float"]
        int_cans = [ci for ci in candidates_info if ci["category"] == "int"]
        enum_cans = [ci for ci in candidates_info if ci["category"] == "enum"]

        # l_bounds, u_bounds need
        l_bounds = [float(ci["lower_bound"]) for ci in candidates_info]
        u_bounds = [float(ci["upper_bound"]) for ci in candidates_info]

        # samples logits
        samples = self.sample_method(
            len(knobs.candidates), num, seed=self.args["seeds"]
        )

        from scipy.stats import qmc

        samples = [spl for spl in qmc.scale(samples, l_bounds, u_bounds)]
        # int & enum samples items
        for spl_num in range(len(samples)):
            spl = samples[spl_num].astype(object)
            # float
            findex = [candidates_info.index(fc) for fc in float_cans]
            spl[findex] = [round(value, 2) for value in spl[findex]]
            # int
            iindex = [candidates_info.index(ic) for ic in int_cans]
            spl[iindex] = [int(value) for value in spl[iindex]]
            # enum
            eindex = [candidates_info.index(ec) for ec in enum_cans]
            spl[eindex] = [
                self.process_list(candidates_info[index]["enum_choices"])[
                    int(spl[index])
                ]
                for index in eindex
            ]

            # process finished, turn into dict
            sample_dict = {}
            for i, name in enumerate([ci["name"] for ci in candidates_info]):
                sample_dict[name] = spl[i]
            samples[spl_num] = sample_dict

        self.config_results = samples
        return self.config_results

    def execute(self, result_path, num=50, is_restart=True, is_clear_cache=False):
        samples = self.sample(num=num)

        if type(samples[0]) != dict:
            raise TypeError(
                f"the sample type should be dict, but get {type(samples[0])}"
            )

        total_list = []
        for i, s in enumerate(samples):
            self.database.update(
                s, is_clear_cache=is_clear_cache, is_restart=is_restart
            )
            result = self.workload.evaluate()[self.metric]
            logging.info(f"{i} {self.metric}: {result}")
            # add result
            s[self.metric], s[""] = result, i
            total_list.append(s)
            # record
            if result_path is not None:
                columns = [c for c in total_list[0].keys()]
                columns.remove("")
                columns = [""] + columns
                with open(result_path, "w", newline="") as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=columns)
                    writer.writeheader()
                    writer.writerows(total_list)
        return result_path, total_list

    @staticmethod
    def process_list(list_str):
        # add quotes to item
        list_str = list_str.strip()[1:-1]
        items = list_str.split(",")
        items = [item.strip() for item in items]
        return items
