import mongoengine
from ETFsList_Scripts.ETFListCollection import ETFListData



class ETFListDocument(mongoengine.Document):
    Download_date = mongoengine.DateField()
    etflist = mongoengine.EmbeddedDocumentListField(ETFListData)

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