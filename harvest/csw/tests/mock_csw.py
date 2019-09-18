import json
from unittest import mock


class MockCatalogueServiceWeb:

    exceptionreport = None
    position = 0
    def __init__(self, url=None, timeout=30):
        self.version = '0'
        if url == 'https://some-source.com/404csw':
            raise Exception('Fail to connect. 404 Client Error')
        if url == 'https://some-source.com/2-records-csw':
            self.load_fom_file()
        else:  # defaults
            raise Exception('unknown URL')

    def load_fom_file(self, path_file='samples/csw_sample.json'):
        d = json.load(open(path_file))
        self.data = d

        self.version = d['version']
        idf = mock.Mock()

        setattr(idf, 'version', d['identification']['version'])
        setattr(idf, 'type', d['identification']['type'])
        setattr(idf, 'title', d['identification']['title'])
        setattr(idf, 'abstract', d['identification']['abstract'])
        setattr(idf, 'keywords', d['identification']['keywords'])
        setattr(idf, 'accessconstraints', d['identification']['accessconstraints'])
        setattr(idf, 'fees', d['identification']['fees'])
        self.identification = idf

        pvd = mock.Mock()
        setattr(pvd, 'name', d['provider']['name'])
        setattr(pvd, 'url', d['provider']['url'])
        self.provider = pvd

        ctc = mock.Mock()
        setattr(ctc, 'name', d['provider']['contact']['name'])
        setattr(ctc, 'organization', d['provider']['contact']['organization'])
        setattr(ctc, 'site', d['provider']['contact']['site'])
        setattr(ctc, 'instructions', d['provider']['contact']['instructions'])
        setattr(ctc, 'email', d['provider']['contact']['email'])
        setattr(ctc, 'country', d['provider']['contact']['country'])
        self.provider.contact = ctc

        oops = []
        for op in d['operations']:
            oop = mock.Mock()
            setattr(oop, 'methods', op['methods'])
            oops.append(oop)
        self.operations = oops

    def getrecords2(self, **kwa):

        startposition = kwa['startposition']
        maxrecords = kwa['maxrecords']

        d = self.data
        res = {'matches': len(d['records'].keys())}
        self.results = res

        recs = {}
        for k, v in d['records'].items():
            self.position += 1
            if startposition > self.position:
                continue
            if len(recs.keys()) > maxrecords:
                break
            rec = mock.Mock()
            f = open('samples/sample2.xml', 'rb')
            rec.xml = f.read()
            f.close()

            rec.identifier = k
            rec.parentidentifier = v['parentidentifier']
            rec.language = v['language']
            rec.dataseturi = v['dataseturi']
            rec.languagecode = v['languagecode']
            rec.datestamp = v['datestamp']
            rec.charset = v['charset']
            rec.hierarchy = v['hierarchy']

            ctcs = []
            for c in v['contact']:
                ctc = mock.Mock()
                ctc.name = c['name']
                ctc.organization = c['organization']
                ctc.email = c['email']
                ctc.country = c['country']
                ctcs.append(ctc)
            rec.contact = ctcs

            rec.datetimestamp = v['datetimestamp']
            rec.stdname = v['stdname']
            rec.stdver = v['stdver']
            rec.locales = []
            for lo in v['locales']:
                rec['locales'].append({'id': lo['id'],
                                       'languagecode': lo['languagecode'],
                                       'charset': lo['charset']})

            rec.identificationinfo = []
            for ii in v['identificationinfo']:
                iid = mock.Mock()
                iid.title = ii['title'],
                iid.abstract = ii['abstract']
                rec.identificationinfo.append(iid)

            rec.contentinfo = []
            for ci in v['contentinfo']:
                cid = {'xml': ci['xml']}
                rec.contentinfo.append(cid)

            rec.distribution = {}
            if v['distribution'] is not None:
                dd = mock.Mock()
                dd.format = v['distribution'].get('format', None)
                dd.version = v['distribution'].get('format', None)
                rec.distribution = dd

            recs[k] = rec

        self.records = recs
        self.exceptionreport = None
