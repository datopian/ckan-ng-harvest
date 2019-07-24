""" transform datasets to CKAN datasets """


class DataJSONSchema1_1:
    """ Data.json dataset from Schema 1.0"""

    def get_mapping_fields(self):
        # https://github.com/GSA/ckanext-datajson/blob/07ca20e0b6dc1898f4ca034c1e073e0c27de2015/ckanext/datajson/harvester_base.py#L478
        MAPPING = {
            "title": "title",
            "description": "notes",
            "keyword": "tags",
            "modified": "extras__modified",  # ! revision_timestamp
            "publisher": "extras__publisher",  # !owner_org
            "contactPoint": {"fn":"maintainer", "hasEmail":"maintainer_email"},
            "identifier": "extras__identifier",  # !id
            "accessLevel": "extras__accessLevel",

            "bureauCode": "extras__bureauCode",
            "programCode": "extras__programCode",
            "rights": "extras__rights",
            "license": "extras__license",  # !license_id
            "spatial": "extras__spatial",  # Geometry not valid GeoJSON, not indexing
            "temporal": "extras__temporal",

            "theme": "extras__theme",
            "dataDictionary": "extras__dataDictionary",  # !data_dict
            "dataQuality": "extras__dataQuality",
            "accrualPeriodicity":"extras__accrualPeriodicity",
            "landingPage": "extras__landingPage",
            "language": "extras__language",
            "primaryITInvestmentUII": "extras__primaryITInvestmentUII",  # !PrimaryITInvestmentUII
            "references": "extras__references",
            "issued": "extras__issued",
            "systemOfRecords": "extras__systemOfRecords",

            "distribution": None,
        }

        return MAPPING


class DataJSONSchema1_0:
    """ Data.json dataset from Schema 1.0"""

    def get_mapping_fields(self):
        # https://github.com/GSA/ckanext-datajson/blob/07ca20e0b6dc1898f4ca034c1e073e0c27de2015/ckanext/datajson/harvester_base.py#L443
        MAPPING = {
            "title": "title",
            "description": "notes",
            "keyword": "tags",
            "modified": "extras__modified", # ! revision_timestamp
            "publisher": "extras__publisher", # !owner_org
            "contactPoint": "maintainer",
            "mbox": "maintainer_email",
            "identifier": "extras__identifier", # !id
            "accessLevel": "extras__accessLevel",

            "bureauCode": "extras__bureauCode",
            "programCode": "extras__programCode",
            "accessLevelComment": "extras__accessLevelComment",
            "license": "extras__license", # !license_id
            "spatial": "extras__spatial", # Geometry not valid GeoJSON, not indexing
            "temporal": "extras__temporal",

            "theme": "extras__theme",
            "dataDictionary": "extras__dataDictionary", # !data_dict
            "dataQuality": "extras__dataQuality",
            "accrualPeriodicity":"extras__accrualPeriodicity",
            "landingPage": "extras__landingPage",
            "language": "extras__language",
            "primaryITInvestmentUII": "extras__primaryITInvestmentUII", # !PrimaryITInvestmentUII
            "references": "extras__references",
            "issued": "extras__issued",
            "systemOfRecords": "extras__systemOfRecords",

            "accessURL": None,
            "webService": None,
            "format": None,
            "distribution": None,
        }

        return MAPPING
