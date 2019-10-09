from settings import CKAN_ORG_ID

r1 = {
    'comparison_results': {
        'action': 'create',
        'new_data': {
            'identifier': 'USDA-9000',  # data.json id
            'isPartOf': 'USDA-8000',
            'title': 'R1 the first datajson',
            'headers': {
                "schema_version": "1.1",
                "@context": "https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld",
                "@id": "https://www2.ed.gov/data.json",
                "@type": "dcat:Catalog",
                "conformsTo": "https://project-open-data.cio.gov/v1.1/schema",
                "describedBy": "https://project-open-data.cio.gov/v1.1/schema/catalog.json",
                }
            }
        },
    }

r2 = {
    'name': 'r2-second',
    'title': 'R2 the second',
    'owner_org': CKAN_ORG_ID,
    'resources': [],
    'comparison_results': {
        'action': 'update',
        'new_data': {
            'identifier': 'USDA-8000',  # data.json id
            'title': 'R2-second',
            'headers': {
                "schema_version": "1.1",
                "@context": "https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld",
                "@id": "https://www2.ed.gov/data.json",
                "@type": "dcat:Catalog",
                "conformsTo": "https://project-open-data.cio.gov/v1.1/schema",
                "describedBy": "https://project-open-data.cio.gov/v1.1/schema/catalog.json",
                }
            }
        },
    }

r3 = {
    'owner_org': CKAN_ORG_ID,
    'comparison_results': {
        'action': 'create',
        'new_data': {
            'identifier': 'USDA-7000',  # data.json id
            'isPartOf': 'USDA-1000',  # not exists
            'title': 'R3 the third',
            'headers': {
                "schema_version": "1.1",
                "@context": "https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld",
                "@id": "https://www2.ed.gov/data.json",
                "@type": "dcat:Catalog",
                "conformsTo": "https://project-open-data.cio.gov/v1.1/schema",
                "describedBy": "https://project-open-data.cio.gov/v1.1/schema/catalog.json",
                }
            }
        },
    }

r4 = {
    'name': 'r4-fourth',
    'title': 'R4 the fourth',
    'owner_org': CKAN_ORG_ID,
    'resources': [],
    'comparison_results': {
        'action': 'update',
        'new_data': {
            'identifier': 'USDA-6000',  # data.json id
            'isPartOf': 'USDA-8000',
            'title': 'R4-fourth',
            'headers': {
                "schema_version": "1.1",
                "@context": "https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld",
                "@id": "https://www2.ed.gov/data.json",
                "@type": "dcat:Catalog",
                "conformsTo": "https://project-open-data.cio.gov/v1.1/schema",
                "describedBy": "https://project-open-data.cio.gov/v1.1/schema/catalog.json",
                }
            }
        },
    }
