import json


def _get_operators(obj, res):
    NODE_ATTR_SUB = {"Plan", "Plans"}
    assert isinstance(obj, dict)
    node_type = str(obj.get("Node Type", "")).replace(" ", "_").lower()
    if node_type != "":
        res[node_type] = res.get(node_type, 0) + 1

    for key in NODE_ATTR_SUB:
        if key in obj.keys():
            if key == "Plan":
                _get_operators(obj[key], res)
            if key == "Plans":
                for plan in obj[key]:
                    _get_operators(plan, res)


def get_feature_from_log(path, normalize=False):
    iflag = 0
    total = 0
    ithreshold = 10
    tthreshold = 100_000

    opts_feature = {}
    suid_feature = {"INSERT": 0, "SELECT": 0, "DELETE": 0, "UPDATE": 0}

    with open(f"{path}", "r") as f:
        while True:
            if not (log_line := f.readline()) or total > tthreshold:
                break
            while "LOG:  duration:" in log_line:
                plan_strs = []
                log_line = f.readline()
                while " CST [" not in log_line:
                    plan_strs.append(log_line)
                    log_line = f.readline()
                    if not log_line:
                        break
                try:
                    plan_obj = json.loads(s="".join(plan_strs))
                except:
                    continue
                query_text = plan_obj.get("Query Text", "").upper()
                # get suid fearture
                for key in suid_feature.keys():
                    if key in query_text:
                        total += 1
                        suid_feature[key] = suid_feature[key] + 1
                        if key == "INSERT":
                            iflag += 1
                            if iflag == ithreshold:
                                suid_feature[key] = suid_feature[key] - ithreshold
                                total -= ithreshold
                        else:
                            iflag = 0
                _get_operators(plan_obj, opts_feature)

    if normalize:
        suid_sum = sum(suid_feature.values())
        opts_sum = sum(opts_feature.values())
        for key in suid_feature.keys():
            suid_feature[key] = suid_feature[key] / suid_sum
        for key in opts_feature.keys():
            opts_feature[key] = opts_feature[key] / opts_sum
    return suid_feature, opts_feature

