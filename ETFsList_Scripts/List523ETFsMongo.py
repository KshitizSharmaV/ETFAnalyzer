import mongoengine
from ETFsList_Scripts.ETFListCollection import ETFListData
import getpass


class ETFListDocument(mongoengine.Document):
    Download_date = mongoengine.DateField()
    etflist = mongoengine.EmbeddedDocumentListField(ETFListData)

    if getpass.getuser() == 'ubuntu':
        meta = {
            'indexes': [
                {
                    'fields': ['Download_date'],
                    'unique': True
                }
            ],
            'db_alias': 'ETF_db',
            'collection': 'ETF523List'
        }
    else:
        meta = {
            'db_alias': 'ETF_db',
            'collection': 'ETF523List'
        }