import os
import time
import logging
import knobtool.constants as my_constants

from knobtool.workload import benchbase
from knobtool.collector import Collector
from knobtool.knobs_manager import Knobs
from rank import create_rank_info
from knobtool.database.opengauss import GaussDB

WORKLOAD = {"benchbase": benchbase.Benchbase}


def collect(config, two_stage=False, result_path=None, candidates=None):
    wkld_cls = "benchbase"

    wkld_name = config["workload"]["name"].upper()
    weights = config["workload"].get("weights", None)
    if candidates is None:
        with open(config["evaluate_knob"], "r") as f:
            context = f.readlines()
            candidates = [line.strip() for line in context if line.strip()[0] != "#"]

    dbms = GaussDB.get_db(
        name=my_constants.DB_NAME,
        port=my_constants.DB_PORT,
        knobs=knobs,
    )
    wkld_args = {
        "weights": weights,
        "db_port": dbms.port,
        "db_user": dbms.db_user,
        "db_passwd": dbms.db_passwd,
        "db_name": dbms.db_name,
    }
    workload = WORKLOAD[wkld_cls](metric="through", workload=wkld_name, args=wkld_args)

    knobs = Knobs(
        knobs_csv_path=my_constants.DB_KNOBS_INFO,
        candidates=candidates,
    )

    if two_stage:
        logging.info(f"{wkld_name} stage one start")
        tmp_path = os.path.join(
            config["file_dir"],
            "collect_tmp.csv",
        )
        data_file, total_list = _collect(
            knobs=knobs,
            dbms=dbms,
            worklaod=workload,
            size=10,
            result_path=tmp_path,
        )
        # create_rank_info
        rank_res = create_rank_info(data_file).rank
        candidates = [_ for _ in rank_res.keys()][:6]
        knobs = Knobs(
            knobs_csv_path=my_constants.DB_KNOBS_INFO,
            candidates=candidates,
        )
        os.remove(tmp_path)

    # collect
    logging.info(f"{wkld_name} start")
    start_time = time.time()

    data_file, total_list = _collect(
        knobs=knobs,
        dbms=dbms,
        workload=workload,
        size=config["size"],
        result_path=result_path
        if result_path is not None
        else os.path.join(
            config["file_dir"],
            my_constants.DOCKER_NAME + "_" + wkld_name + ".csv",
        ),
    )

    cost_time = time.time() - start_time
    logging.info(f"{wkld_name} cost time: {cost_time}\n")

    return data_file, total_list


def _collect(knobs, workload, size, result_path, sample_policy="lhs", seed=100):
    dbms = GaussDB.get_db(
        name=my_constants.DB_NAME,
        port=my_constants.DB_PORT,
        knobs=knobs,
    )
    col_args = {"sample_policy": sample_policy, "seeds": seed}
    col = Collector(knobs, dbms, workload, col_args)
    return col.execute(num=size, result_path=result_path)
