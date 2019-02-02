# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item,Field


class ImdbItem(Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    title = Field()
    year = Field()
    certificate = Field()
    run_time = Field()
    genre = Field()
    summary = Field()
    rating = Field()
    rating_count = Field()
    gross = Field()
    actor = Field()
    serial = Field()
