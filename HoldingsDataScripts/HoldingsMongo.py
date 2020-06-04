import mongoengine

class Holdings(mongoengine.EmbeddedDocument):
    TickerName = mongoengine.StringField()
    TickerSymbol = mongoengine.StringField()
    TickerWeight = mongoengine.FloatField()
