from settings import CKAN_ORG_ID

r1 = {
    'comparison_results': {
        'action': 'create',
        'new_data': {
            'identifier': 'USDA-9000',  # data.json id
            'isPartOf': 'USDA-8000',
            'title': 'R1 the first datajson',
            'description': 'some notes',
            'contactPoint': {
                "hasEmail": "mailto:j1@data.com",
                "@type": "vcard:Contact",
                "fn": "Jhon One"
                },
            'programCode': '009:102',
            'bureauCode': '003:01',
            'publisher': {'name': 'Some publisher'},
            'modified': '2019-02-02T21:36:22.693792',
            'keyword': ['tag32', 'tag90'],
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
    'public_access_level': 'public',
    'owner_org': CKAN_ORG_ID,
    'unique_id': 'USDA-8000',
    'contact_name': 'Jhon Contact',
    'program_code': '001:900',
    'bureau_code': '002:80',
    'contact_email': 'jhon@contact.com',
    'publisher': 'Publisher 2',
    'notes': 'Some notes',
    'modified': '2019-05-02T21:36:22.693792',
    'tag_string': 'tag19,tag33',
    'resources': [],
    'comparison_results': {
        'action': 'update',
        'new_data': {
            'identifier': 'USDA-8000',  # data.json id
            'title': 'R2-second',
            'contactPoint': {
                "hasEmail": "mailto:j2@data.com",
                "@type": "vcard:Contact",
                "fn": "Jhon Two"
                },
            'programCode': '002:302',
            'bureauCode': '008:88',
            'publisher': {'name': 'Some publisher II'},
            'modified': '2019-02-02T21:36:22.693792',
            'keyword': ['tag31', 'tag91'],
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
            'contactPoint': {
                "hasEmail": "mailto:j3@data.com",
                "@type": "vcard:Contact",
                "fn": "Jhon Three"
                },
            'programCode': '002:303',
            'bureauCode': '008:83',
            'publisher': {'name': 'Some publisher III'},
            'modified': '2019-03-02T21:36:22.693792',
            'keyword': ['tag33', 'tag93'],
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
    'public_access_level': 'public',
    'unique_id': 'USDA-6000',
    'contact_name': 'Jhon Contact II',
    'program_code': '003:200',
    'bureau_code': '007:10',
    'contact_email': 'jhonII@contact.com',
    'publisher': 'Publisher',
    'notes': 'Some notes II',
    'modified': '2012-05-02T21:36:22.693792',
    'tag_string': 'tag24,tag39',
    'resources': [],
    'comparison_results': {
        'action': 'update',
        'new_data': {
            'identifier': 'USDA-6000',  # data.json id
            'isPartOf': 'USDA-8000',
            'title': 'R4-fourth',
            'contactPoint': {
                "hasEmail": "mailto:j4@data.com",
                "@type": "vcard:Contact",
                "fn": "Jhon Four"
                },
            'programCode': '002:304',
            'bureauCode': '008:84',
            'publisher': {'name': 'Some publisher IV'},
            'modified': '2019-04-02T21:36:22.693792',
            'keyword': ['tag34', 'tag94'],
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