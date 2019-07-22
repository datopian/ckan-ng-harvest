from logs import logger
import config


def write_results_to_ckan(rows):
    """ each row it's a dataset to delete/update/create """
    actions = {}
    for row in rows:
        yield row
        action = row['comparison_results']['action']
        if action not in actions.keys():
            actions[action] = 0
        actions[action] += 1
    logger.info(f'Actions detected {actions}')


def write_final_report():
    """ take all generated data and write a final report """
    pass
