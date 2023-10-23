import os
import pickle
import logging
import numpy as np
import knobtool.constants as my_constants

from knobtool.experience import ExpPool
from collect import collect
from knobtool.knobs_manager import Knobs
from knob_evaluator.utils import load_data


def evaluate(
    new_conf,
    old_conf,
    use_exp=False,
    data_path=None,
    wkld_feature=None,
    model_path=None,
    exp_path=None,
    config=None,
):
    def _predict(model, conf: dict, knobs_info):
        tknobs = model.feature_names
        X = []
        for tb in tknobs:
            if tb not in conf.keys():
                X.append(knobs_info.all_knobs_default[tb])
            else:
                X.append(conf[tb])
        X = knobs_info.enum_turn(np.array([X]))
        X = X.astype(np.float64)
        X = knobs_info.knob_normalization(X, tknobs)
        return model.predict(X)[0]

    knobs_info = Knobs(my_constants.DB_KNOBS_INFO)

    if use_exp:
        assert exp_path is not None
        assert data_path is not None
        assert wkld_feature is not None
        exp_pool = ExpPool(my_constants.CM_EXP_POOL)
        exps = exp_pool.get_knobs_rank_similarity(wkld_feature, 5)
        weights = np.array([x[1] for x in exps])
        weights = ((weights - weights.min()) / weights.max) * 0.9 + 0.1

        knob_importance = {}
        for i, exp in enumerate(exps):
            for k, v in exp.importance:
                knob_importance[k] = knob_importance.get(k, 0) + v * weights[i]
        important_knobs = sorted(
            [x[0] for x in knob_importance.items()], key=lambda x: -x[1]
        )[:6]

        tmp_path = os.path.join(
            config["collect"]["file_dir"],
            "evaluate_tmp.csv",
        )

        data_file, _ = collect(config, result_path=tmp_path, candidates=important_knobs)
        X, y, _, _ = load_data(data_file)
        distr_sim = exp_pool.get_data_distr_similarity(exps, (X, y))
        weights = np.array([x[1] for x in exps])
        weights = ((weights - weights.min()) / weights.max) * 0.9 + 0.1

        pred_res_new = 0
        pred_res_old = 0
        for i in range(len(exps)):
            pred_res_new = pred_res_new + weights[i] * _predict(
                distr_sim[i][0], new_conf, knobs_info=knobs_info
            )
            pred_res_old = pred_res_old + weights[i] * _predict(
                distr_sim[i][0], old_conf, knobs_info=knobs_info
            )
    else:
        assert model_path is not None
        model = None
        with open(model_path, "rb") as f:
            model = pickle.load(f)
        pred_res_new = _predict(model, new_conf, knobs_info=knobs_info)
        pred_res_old = _predict(model, old_conf, knobs_info=knobs_info)

    percentage = abs((pred_res_new - pred_res_old) / pred_res_old)
    if percentage < 0.1:
        output_string = (
            "For the current evaluation configuration ["
            + new_conf
            + "] changed "
            + str(percentage[0])
            + "% \compared to the previous configuration ["
            + old_conf
            + "],we consider this change to be neutral"
        )
        logging.info(output_string)
    elif pred_res_old < pred_res_new:
        output_string = (
            "For the current evaluation configuration ["
            + new_conf
            + "] improved "
            + str(percentage[0])
            + "% \compared to the previous configuration ["
            + old_conf
            + "],we consider this change to be positive"
        )
        logging.info(output_string)
    else:
        output_string = (
            "For the current evaluation configuration ["
            + new_conf
            + "] decreased "
            + str(percentage[0])
            + "% compared to the previous configuration ["
            + old_conf
            + "],we consider this change to be negative"
        )
        logging.info(output_string)
