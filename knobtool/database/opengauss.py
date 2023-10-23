import os
import time
import logging
import psycopg2
import subprocess

from typing import Union
from knobtool import constants as my_constants
from knobtool.database.basicdb import BasicDB


class GaussDB(BasicDB):
    # Friendly with PG
    def __init__(
        self,
        name,
        port,
        knobs,
        volumn=my_constants.DOCKER_VOLUMN,
        bak_path=my_constants.DB_BAK_FILE,
        db_host=my_constants.DB_HOST,
        db_name=my_constants.DB_NAME,
        db_user=my_constants.DB_USER,
        db_passwd=my_constants.DB_PASSWD,
        sys_user=my_constants.SYS_USER,
        sys_passwd=my_constants.SYS_PASSWD,
    ) -> None:
        self.target_name = "postgresql.conf"
        # Docker container name
        self.name = name
        self.port = port
        self.knobs = knobs
        self.volumn = os.path.join(volumn, self.name)
        self.bak = self._load_default(back_path=bak_path)
        self.db_host = db_host
        self.db_name = db_name
        self.db_user = db_user
        self.db_passwd = db_passwd
        self.sys_user = sys_user
        self.sys_passwd = sys_passwd

        self.create_database_if_not_exists(self.db_name)

    def update(self, sample: Union[dict, str], is_clear_cache=False, is_restart=True):
        """
        create config file and write to local docker container.
        """
        if sample == "default":
            context = self._output(sample, is_default=True)
        else:
            context = self._output(sample)

        self._produce_config(context)

        if is_restart:
            self.restart()

    def _load_default(self, back_path):
        if os.path.exists(back_path):
            with open(back_path, "r") as f:
                context = f.read()
        else:
            raise ValueError("no back postgres conf found")
        return context

    def _output(self, config_results, is_default=False):
        """
        output config_results to database context file,
        the config may come from sample or outside input.
        list: self.config_results: [config_num, knob_num]
        dict: self.config_results: [{name:value}]

        now in db only input dict
        """
        # return configs & knobs dataframe

        if is_default:
            return self.bak

        # candidates sort
        candidates = [ci["name"] for ci in self.knobs.candidates_info]

        config_dict = {}
        config_text = []
        for i, name in enumerate(candidates):
            # check config type
            if type(config_results) is list:
                # for native produced from lhs policy list samples
                config_dict[name] = config_results[i]
            elif type(config_results) is dict:
                # for input from configspace
                config_dict[name] = config_results[name]
            else:
                raise ValueError(
                    f"wrong config type {type(config_results)} input, should be [list, dict]"
                )

            config_text.append(
                f"{name} = {config_dict[name]}{[ki for ki in self.knobs.knobs_info if ki['name']==name][0]['unit']}"
            )

            # add bak
            text = "\n".join(config_text)
            total_text = self.bak + "\n" + text + "\n"

        return total_text

    def _produce_config(self, context):
        with open(f"tmp_{self.port}", "w") as f:
            f.write(context)
        cmd = f"sudo -S mv tmp_{self.port} {os.path.join(self.volumn,self.target_name)}"
        _ = subprocess.run(
            cmd,
            shell=True,
            input=self.sys_passwd + "\n",
            capture_output=True,
            text=True,
            timeout=None,
        )

    def _exec_only(self, statement, database="postgres", autocommit=False):
        try:
            conn = psycopg2.connect(
                f"dbname={database} user={self.db_user} password={self.db_passwd} host={self.db_host} port={self.port}"
            )
            conn.autocommit = autocommit
            cur = conn.cursor()
            cur.execute(statement)
            conn.close()
        except Exception:
            return False
        return True

    def _exec_fetch(self, statement, one=False, database="postgres", autocommit=False):
        try:
            conn = psycopg2.connect(
                f"dbname={database} user={self.db_user} password={self.db_passwd} host={self.db_host} port={self.port}"
            )
            cur = conn.cursor()
            cur.execute(statement)
            if not one:
                res = cur.fetchall()
            else:
                res = cur.fetchone()
            conn.close()
        except Exception as e:
            logging.info(e)
            return None
        return res

    def create_database_if_not_exists(self, database):
        res = self._exec_fetch(
            "SELECT datname FROM pg_catalog.pg_database WHERE datname = '{}';".format(
                database
            ),
            one=True,
        )
        if not res:
            self._exec_only("CREATE DATABASE {};".format(database), autocommit=True)

    def refresh(self):
        return self._exec_only("SELECT pg_reload_conf();")

    def touch(self):
        return self._exec_only(
            "select pg_database.datname, pg_database_size(pg_database.datname) AS size from pg_database;"
        )

    def restart(self, time_bound=20):
        logging.info(f" -- container {self.name} restarting")

        # first stop
        stop_start = time.time()
        _ = subprocess.run(
            ["sudo", "-S", "docker", "stop", "--time=400", self.name],
            input=self.sys_passwd + "\n",
            text=True,
            capture_output=True,
        )
        stop_finish = time.time()
        logging.info(
            f" -- container {self.name} stopping takes {stop_finish-stop_start}"
        )
        # then restart
        _ = subprocess.run(
            ["sudo", "-S", "docker", "restart", self.name],
            input=self.sys_passwd + "\n",
            text=True,
            capture_output=True,
        )
        record_time = time.time()
        while time.time() - record_time < time_bound:
            time.sleep(10)
            state = self.touch()
            if state:
                logging.info(f" -- restart {self.name} with {time.time()-record_time}")
                return True
            logging.info(f" -- waiting for {self.name} {time.time()-record_time}")
        return False
