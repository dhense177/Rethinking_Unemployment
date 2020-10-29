from scrapy import Spider, Request
from scrapy.selector import Selector
from livingwage.items import LivingwageItem
import urllib
import re




class WagesSpider(Spider):
    name = "wage_estimates"
    # download_delay = 5.0
    # allowed_urls = ['http://www.webmd.com/']
    # start_urls = ['https://doctor.webmd.com/find-a-doctor/specialties']

    def start_requests(self):
        urls = [
            'https://web.archive.org/web/20120622033154/https://livingwage.mit.edu/'
            #'https://web.archive.org/web/20130725202747/https://livingwage.mit.edu/',   #2013
            #'https://web.archive.org/web/20140626094717/http://livingwage.mit.edu/',    #2014
        ]
        for url in urls:
            yield Request(url=url, callback=self.parse_states)

    def parse_states(self, response):
        #Start by limiting to Acupuncture only
        # states = response.xpath('//div[contains(@class,"span-3")]').extract()
        # for i in range(len(specs)):
        # for i in range(4):
        # state = response.xpath('//div[@class="span-3"]//li/a/text()').extract()
        state = response.xpath('//ul/li/a/@href').extract()
        estimate = LivingwageItem()
        estimate['State'] = state
        return estimate
            # yield Request(response.urljoin(response.xpath('//ul/li[contains(@class,"alpha-")]/a/@href').extract()[i]), callback = self.parse_states, meta = {'Specialty': Specialty}, dont_filter= True)