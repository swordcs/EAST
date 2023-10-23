import csv
import numpy as np

from sklearn.preprocessing import MinMaxScaler


class Knobs:
    def __init__(self, knobs_csv_path, candidates=None) -> None:
        # knobs name upper lower bound file
        self.knobs_csv_path = knobs_csv_path
        with open(knobs_csv_path, "r") as f:
            reader = csv.DictReader(f)
            self.knobs_info = [row for row in reader]
        self.all_knobs_range = {}
        self.all_knobs_default = {}
        self.enum_choices = []
        self.enum_turn_to_int = {"off": 0, "on": 1}
        self.candidates = candidates
        # knob range and enum choices for turn enum to int
        for row in self.knobs_info:
            knob_name = row["name"]
            if row["category"] in ["int", "float"]:
                self.all_knobs_range[knob_name] = [
                    row["lower_bound"],
                    row["upper_bound"],
                ]
                self.all_knobs_default[knob_name] = row["default"]
            else:
                choices = row["enum_choices"].strip("[]").split(",")
                standard_choices = [choice.strip().strip("'") for choice in choices]
                self.enum_choices.append(standard_choices)
                self.all_knobs_range[knob_name] = [0, len(choices) - 1]
        self.enum_choices.sort(key=len)

    @property
    def candidates(self):
        return self._candidates

    @candidates.setter
    def candidates(self, value):
        self._candidates = value
        knobs = [ki["name"] for ki in self.knobs_info]
        if not all([cand in knobs for cand in self._candidates]):
            raise ValueError("eval knobs not found")
        self.candidates_info = [
            ki for ki in self.knobs_info if ki["name"] in self._candidates
        ]

    def knob_normalization(self, knob_data, knobs):
        min_values = [self.all_knobs_range[knob][0] for knob in knobs]
        max_values = [self.all_knobs_range[knob][1] for knob in knobs]
        scaler = MinMaxScaler(feature_range=(0, 1))
        scaler.fit(np.array([min_values, max_values]))
        return scaler.transform(knob_data)

    def enum_turn(self, knob_data):
        for choices in self.enum_choices:
            value = 0
            ban_value = {
                choice: self.enum_turn_to_int[choice]
                for choice in choices
                if choice in self.enum_turn_to_int.keys()
            }
            for choice in choices:
                if choice not in ban_value.keys():
                    while value in ban_value.values():
                        value = value + 1
                    self.enum_turn_to_int[choice] = value
                    value = value + 1

        def replace_func(value):
            return self.enum_turn_to_int.get(value, value)

        v_func = np.vectorize(replace_func)
        return v_func(knob_data)
