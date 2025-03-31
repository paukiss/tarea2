# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class NewspaperCollectorItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


def select_data(value):
    if isinstance(value, tuple) and len(value) > 0: 
        return value[0]
    return value 

class NewspaperItem(scrapy.Item):

    titulo = scrapy.Field()
    descripcion = scrapy.Field()
    fecha = scrapy.Field()
    seccion = scrapy.Field()
    url = scrapy.Field()

    def __getitem__(self, key):
        value = super(NewspaperItem, self).__getitem__(key)
        field = self.fields[key]
        if 'serializer' in field:
            return field['serializer'](value)
        return value 
    