from mrjob.job import MRJob
from mrjob.step import MRStep
import heapq
import re

url_pattern = '([\w\.-]+) - - \[(.*?)\] "(.*?)" (\d+) (\d+)'
sum_Requests = []

class MRCostCalc(MRJob):

    def cost_cal_mapper(self, _, key):
        if re.match(url_pattern, key) is not None:
            test = re.match(url_pattern, key)
            (host, d, request, status, bytes) = test.groups()
            sum_Requests.append(1)
            yield 1, int(bytes)

    def cost_cal_reducer(self, _, bytes):
        requests = sum (sum_Requests)
        sum_bytes= sum(bytes)
        fraction = float(1024 ** 3)
        data = round(sum_bytes / fraction,3)
        yield str(data)+' Transmitted data in GBs', str(requests)+' Total requests'
       

    def steps(self):
        return [MRStep(mapper=self.cost_cal_mapper,
                       reducer=self.cost_cal_reducer)
                
                ]


class MRMostFrequentDomains(MRJob):

    def a(self, _, key):
        if re.match(url_pattern2, key) is not None:
            a = re.match(url_pattern2, key)
            (host, d, request, status, bytes) = a.groups()
            new_url_pattern = re.compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
            result = new_url_pattern.match(host)
            if not result:
                words = host.split(".")
                domain_name = words[len(words)-2] +'.'+ words[len(words)-1]
                yield domain_name, 1
            else:
                yield host, 1

    def b(self, host, count):
        yield host, sum(count)

    def c(self, word, count):
        yield None, (int(count), word)

    def d(self, _, pairs):
        for key in (heapq.nlargest(5, pairs)):
            yield key[0], key[1]

    def steps(self):
        return [MRStep(mapper=self.a,
                       reducer=self.b),
                MRStep(mapper=self.c,
                       reducer=self.d
                       )
                ]    


if __name__ == '__main__':
    
    MRCostCalc.run()
    MRMostFrequentDomains.run()
    


