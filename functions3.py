from logs import logger
import config
from libs.ckan_adapters import DataJSONSchema1_1
import json


def write_results_to_ckan(rows):
    """ each row it's a dataset to delete/update/create """

    actions = {}
    c = 0
    for row in rows:
        c += 1
        yield row
        action = row['comparison_results']['action']
        if action not in actions.keys():
            actions[action] = 0
        actions[action] += 1

        dataset = row['comparison_results']['new_data']
        dmp = json.dumps(dataset, indent=4)
        logger.info(f'Dataset {dmp}')

        djss = DataJSONSchema1_1(original_dataset=dataset)
        # ORG is required!
        djss.ckan_owner_org_id = config.CKAN_OWNER_ORG

        ckan_dataset = djss.transform_to_ckan_dataset()
        dmp = json.dumps(ckan_dataset, indent=4)
        logger.info(f'Converted {dmp}')

        if c == 3:
            raise Exception('out')

    logger.info(f'Actions detected {actions}')


def write_final_report():
    """ take all generated data and write a final report """
    pass
