#coding=utf-8
import scrapy
from scrapy.spiders import CrawlSpider
from scrapy.selector import Selector
from scrapy.http import Request
from imdb.items import ImdbItem
import urllib

class imdb(CrawlSpider):
    name = 'imdb'
    start_urls = ['https://www.imdb.com/search/title?year=2018-01-01,2018-12-31&start=1']

    for i in range(1986, 2018):
        for j in range(0, 99):
            start_urls.append("https://www.imdb.com/search/title?year=" + str(i) + "," + str(i) + "&sort=num_votes,desc&start=" + str(j * 50 + 1))
    url = 'https://www.imdb.com'

    def parse(self, response):
        item = ImdbItem()
        selector = Selector(response)
        movies = selector.xpath('//div[contains(@class, "lister-item mode-advanced")]')

        for movie in movies:
            title = movie.xpath('div/h3/a/text()').extract()
            title = "".join(title)

            year = movie.xpath('div/h3/span[2]/text()').extract()
            year = "".join(year)
            year = year.replace("(", "")
            year = year.replace(")", "")

            certificate = movie.xpath('div/p/span[contains(@class, "certificate")]/text()').extract()
            certificate = "".join(certificate)

            run_time = movie.xpath('div/p/span[contains(@class, "runtime")]/text()').extract()
            run_time = "".join(run_time)

            genre = movie.xpath('div/p/span[contains(@class, "genre")]/text()').extract()
            genre = "".join(genre).strip().split(" ")
            genre = " ".join(genre)

            summary = movie.xpath('div[contains(@class,"lister-item-content")]/p[2]/text()').extract()
            summary = "".join(summary).strip().split(" ")
            summary = " ".join(summary)

            rating = movie.xpath('div[contains(@class,"lister-item-content")]/div/div[contains(@class,"inline-block ratings-imdb-rating")]/strong/text()').extract()
            rating = "".join(rating)

            rating_count = movie.xpath('div[contains(@class,"lister-item-content")]/p[contains(@class,"sort-num_votes-visible")]/span[2]/@data-value').extract()
            rating_count = "".join(rating_count)

            gross = movie.xpath('div[contains(@class,"lister-item-content")]/p[contains(@class,"sort-num_votes-visible")]/span[5]/@data-value').extract()
            gross = "".join(gross).strip().split(",")
            gross = "".join(gross)

            actor_result = []
            actors = movie.xpath('div[contains(@class,"lister-item-content")]/p[3]/a')
            for actor in actors:
                actor_result.append(actor.xpath('text()').extract()[0])

            serial_address = movie.xpath('div/h3/a/@href').extract()[0]
            serial_address = serial_address.split("/")

            item['title'] = str(title)
            item['year'] = str(year)
            item['certificate'] = str(certificate)
            item['run_time'] = str(run_time)
            item['genre'] = str(genre)
            item['summary'] = str(summary)
            item['rating'] = rating
            item['rating_count'] = rating_count
            item['gross'] = gross
            item['actor'] = ",".join(actor_result)
            item['serial'] = serial_address[2]




            yield item
