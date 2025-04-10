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

    data_id = scrapy.Field(serializer=select_data)
    titulo = scrapy.Field(serializer=select_data)
    descripcion = scrapy.Field(serializer=select_data)
    fecha = scrapy.Field(serializer=select_data)
    seccion = scrapy.Field(serializer=select_data)
    url = scrapy.Field(serializer=select_data)
    date_saved = scrapy.Field(serializer=select_data)
    source = scrapy.Field()

    def __getitem__(self, key):
        value = super(NewspaperItem, self).__getitem__(key)
        field = self.fields[key]
        if 'serializer' in field:
            return field['serializer'](value)
        return value 
    