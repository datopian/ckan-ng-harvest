''' transform datasets to CKAN datasets '''
from abc import ABC, abstractmethod


class CKANDatasetAdapter(ABC):
    ''' transform other datasets objects into CKAN datasets '''

    def __init__(self, original_dataset):
        self.original_dataset = original_dataset

    def get_base_ckan_dataset(self):
        # creates the Dict base for a CKAN dataset
        # Check for required fields: https://docs.ckan.org/en/2.8/api/#ckan.logic.action.create.package_create

        pkg = {
            'name': '',  # no spaces, just lowercases, - and _
            'title': '',
            'owner_org': '',  # (string) – the id of the dataset’s owning organization, see organization_list() or organization_list_for_user() for available values. This parameter can be made optional if the config option ckan.auth.create_unowned_dataset is set to True.
            'private': False,
            'author': None,  # (string) – the name of the dataset’s author (optional)
            'author_email': None,  # (string) – the email address of the dataset’s author (optional)
            'maintainer': None,  # (string) – the name of the dataset’s maintainer (optional)
            'maintainer_email': None,  # (string) – the email address of the dataset’s maintainer (optional)
            # just aded when license exists
            # 'license_id': None,  # (license id string) – the id of the dataset’s license, see license_list() for available values (optional)
            'notes':  None,  # (string) – a description of the dataset (optional)
            'url': None,  # (string) – a URL for the dataset’s source (optional)
            'version': None,  # (string, no longer than 100 characters) – (optional)
            'state': 'active',  # (string) – the current state of the dataset, e.g. 'active' or 'deleted'
            'type': None,  # (string) – the type of the dataset (optional), IDatasetForm plugins associate themselves with different dataset types and provide custom dataset handling behaviour for these types
            'resources': None,  # (list of resource dictionaries) – the dataset’s resources, see resource_create() for the format of resource dictionaries (optional)
            'tags': None,  # (list of tag dictionaries) – the dataset’s tags, see tag_create() for the format of tag dictionaries (optional)
            'extras': [  # (list of dataset extra dictionaries) – the dataset’s extras (optional), extras are arbitrary (key: value) metadata items that can be added to datasets, each extra dictionary should have keys 'key' (a string), 'value' (a string)
                {'key': 'resource-type', 'value': 'Dataset'}
            ],
            'relationships_as_object': None,  # (list of relationship dictionaries) – see package_relationship_create() for the format of relationship dictionaries (optional)
            'relationships_as_subject': None,  # (list of relationship dictionaries) – see package_relationship_create() for the format of relationship dictionaries (optional)
            'groups': None,  # (list of dictionaries) – the groups to which the dataset belongs (optional), each group dictionary should have one or more of the following keys which identify an existing group: 'id' (the id of the group, string), or 'name' (the name of the group, string), to see which groups exist call group_list()
        }

        return pkg

    @abstractmethod
    def transform_to_ckan_dataset(self):
        pass
