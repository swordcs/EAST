import os
import yaml
import logging
import argparse
import knobtool.constants as my_constants

from knobtool.experience import ExpPool
from knobtool.experience import Experience
from knobtool.utils import get_feature_from_log

logging.basicConfig(level=logging.INFO)


def get_argv_paser():
    parser = argparse.ArgumentParser(
        prog="An Efficient Estimation System for the Knob Tuning under Dynamic Workload",
        epilog="Nice:)",
    )

    parser.add_argument(
        "--config",
        "-c",
        nargs="?",
        action="store",
        const="config.yaml",
        default="config.yaml",
        help="config file used to connect database and execute workload",
    )

    parser.add_argument(
        "--collect", action="store_true", help="execute collect command"
    )
    parser.add_argument(
        "--two_stage",
        action="store_true",
        help="collect performance data using two-stage strategy",
    )

    parser.add_argument("--rank", action="store_true", help="execute rank command")

    parser.add_argument("--evaluate", action="store_true", help="evaluate knob performance for given config")
    parser.add_argument(
        "--train",
        action="store_true",
        help="train model for knob performance evaluation",
    )
    parser.add_argument(
        "--save",
        action="store_true",
        help="save current experience into pool",
    )

    return parser


def _update_knobtool_constants(config):
    if config is None:
        return
    my_constants.CM_EXP_POOL = config["common"]["exp_pool"]
    my_constants.CM_DATA_PATH = config["common"]["data_path"]
    my_constants.DB_HOST = config["database"]["host"]
    my_constants.DB_PORT = config["database"]["port"]
    my_constants.DB_DRIVER = config["database"]["driver"]
    my_constants.DB_USER = config["database"]["user"]
    my_constants.DB_NAME = config["database"]["name"]
    my_constants.DB_PASSWD = config["database"]["password"]  # TODO: replace
    my_constants.DB_BAK_FILE = config["database"]["bak_file"]
    my_constants.DB_LOG_FILE = config["database"]["log_file"]
    my_constants.DB_KNOBS_INFO = config["database"]["knobs_info"]
    my_constants.DOCKER_NAME = config["docker"]["name"]
    my_constants.DOCKER_VOLUMN = config["docker"]["volumn"]
    my_constants.WORKLOAD_NAME = config["workload"]["name"]
    my_constants.WORKLOAD_TOOL_PATH = config["workload"]["tool_path"]
    my_constants.SYS_USER = config["system"]["user"]
    my_constants.SYS_PASSWD = config["system"]["password"]  # TODO: replace


def main(argv):
    parser = get_argv_paser()
    args = parser.parse_args(argv)

    with open(args.config, "r") as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logging.info(exc)

    _update_knobtool_constants(config)

    from rank import rank
    from collect import collect
    from knob_evaluator import utils as keutils
    from knob_evaluator.evaluate import evaluate

    data_file = None
    if args.collect:
        data_file, _ = collect(config["collect"], args.two_stage)

    if args.rank:
        rank_data = data_file if data_file is not None else my_constants.CM_DATA_PATH
        rank_res = rank(rank_data)
        logging.info(rank_res)

    if args.evaluate:
        if args.train:
            evaluate_data = (
                data_file if data_file is not None else my_constants.CM_DATA_PATH
            )
            model = keutils.load_model()
            X, y, knobs, _ = keutils.load_data(evaluate_data, normalize=True)
            watershed = int(0.7 * X.shape[0])
            X_train, _ = (X[:watershed], X[watershed:])
            y_train, _ = (y[:watershed], y[watershed:])
            keutils.train_model(X_train, y_train, model, knobs)
            keutils.save_model(config["evaluate"]["model_path"], model)
        else:
            evaluate(
                new_conf=config["evaluate"]["new_conf"],
                old_conf=config["evaluate"]["old_conf"],
                use_exp=True,
                exp_path=my_constants.CM_EXP_POOL,
            )
    if args.save:
        assert args.collect and args.rank and args.evaluate and args.train

        exp_pool = ExpPool(my_constants.CM_EXP_POOL)
        # workload feature
        if os.path.exists(my_constants.DB_LOG_FILE):
            wkld_feature = get_feature_from_log(
                my_constants.DB_LOG_FILE, normalize=True
            )
        exp = Experience(
            feature=wkld_feature,
            importance=rank_res,
            model=(model,),
            knobs=knobs,
            data_set=(X, y),
            data_num=X.shape[0],
        )
        exp_pool.add(exp)
        exp_pool.save()
