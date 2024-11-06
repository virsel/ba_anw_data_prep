from pattern_ranker import *
import argparse

log_path = dirname(__file__) + '/log/' + str(datetime.datetime.now().strftime(
    '%Y-%m-%d')) + '_nezha.log'
logger = Logger(log_path, logging.DEBUG, __name__).getlog()


def get_miner(ns="hipster"):
    config = TemplateMinerConfig()
    config.load("input/log_template/drain3_" + ns + ".ini")
    config.profiling_enabled = False

    path = 'input/log_template/' + ns + ".bin"
    persistence = FilePersistence(path)
    template_miner = TemplateMiner(persistence, config=config)

    return template_miner

if __name__ == '__main__':
    level = "inner_service"

    normal_time1 = "2022-08-22 03:51"
    path1 = "input/fault_suffering/2022-08-22/2022-08-22-fault_list.json"
    normal_time2 = "2022-08-23 17:00"
    path2 = "input/fault_suffering/2022-08-23/2022-08-23-fault_list.json"
    log_template_miner = get_miner()
    inject_list = [path1, path2]
    normal_time_list = [normal_time1, normal_time2]
    if level=="service":
        logger.info("------- OnlineBoutique Result at service level -------")
        evaluation_pod(normal_time_list, inject_list,log_template_miner)
    else:
        logger.info("------- OnlineBoutique Result at inner service level -------")
        evaluation(normal_time_list, inject_list,log_template_miner)

