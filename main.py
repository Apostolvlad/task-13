import asyncio
import datetime
import time
from typing import Dict, List

import aiohttp
import requests

import load_file
import process_text

TOKEN = load_file.loud_txt('token')

if TOKEN is None or TOKEN == '':
    print('Ошибка, нет токена от redsale, вставьте токен в файл token.txt')
    load_file.save_txt([], 'token', '')
    exit(0)

def request_to_api(url:str, params:dict = {}):
    result =  requests.get(url, params = params).json()
    time.sleep(0.01)
    return result

to_json = lambda x:list(map(lambda y: y.to_json(), x))
to_ids = lambda x:list(map(lambda y: y.id, x))

class City:
    def __init__(self, cityId: int, beautify: str, **kwarg):
        self.id = cityId
        self.beautify = beautify
    
    def __repr__(self): return f'{self.beautify} id = {self.id}'

class Order:
    description, description_old = '', ''
    salary, count_char = 0, 0
    base_len, publish = dict(), dict()
    correct, used = False, False

    def __init__(self, orderId:int, number:int, createdAt:int, description:str,  **kwarg):
        self.id = orderId
        self.number = number
        self.created = datetime.datetime.utcfromtimestamp(createdAt /1000)
        self.description_old = description
        self.specialization = kwarg['section'].get('specialization', '')

        self.__dict__.update(process_text.check_description(description))
    
    def get_data(self):
        return {
            'description':f'<p>Условия работы - {self.description}</p><p><b>Зарплата: </b>{self.get_salary()}</p>',
            'createdAt':self.created_date,
            'number':self.number,
            'shortDesc':self.specialization
        }

    def get_salary(self):
        return f'{self.salary} руб.' if type(self.salary) is int else self.salary

    def to_json(self):
        return {
            'id':self.id, 
            'used':self.used,
            'correct':self.correct,
            'salary':self.salary, 
            'count_char':self.count_char,
            'number':self.number, 
            'created':self.created_date,
            'base_len':self.base_len, 
            'publish':self.publish,
            'description':self.description,
            'description_old':self.description_old
        }

    @property
    def created_date(self):return self.created.strftime('%Y-%m-%d')

    @property
    def is_use(self): return self.correct and not self.used

    def __repr__(self): return f'{self.id} created: {self.created_date}, description: {self.description}'

class ManagerOrder:
    orders = dict()

    @classmethod
    def get_or_create(cls, **data_order):
        order = cls.orders.get(data_order['orderId'])
        if order is None:
            order = Order(**data_order)
            cls.orders.update({order.id:order})
        return order

class Vacancy:
    section = None
    def __init__(self, vacancyId:int, **kwarg):
        self.id = vacancyId
        self.orders = list()

    def to_json(self):
        return {
            'id':self.id, 
            'orders':list(map(lambda x:(x.number, x.description), self.orders))
        }
    
    def add_order(self, order:Order):
        if len(self.orders) > 24 or order in self.orders: return False 
        self.orders.append(order)
        order.used = True
        return True

    def get_data_send(self):
        data = list()
        for order in self.orders:
            data.append(order.get_data())
        return (self.id, data)

class Section:
    def __init__(self, sectionId:int, **kwarg): #, vacancyId:int
        self.id = sectionId
        self.structure = dict()
        self.vacancies = list()
        self.orders = list()

    def __repr__(self): return f'{self.beautify} {self.id}. Count vacancies = {len(self.vacancies)} Count orders = {len(self.orders)}'

    def set_vacancies(self, base_vacancies:Dict[int, List[Vacancy]]):
        for id_ in self.structure:
            self.vacancies.extend(base_vacancies.get(id_, ()))
        
    def add_vacancy(self, vacancy:Vacancy):
        self.vacancies.append(vacancy)
    
    def add_order(self, order:Order):
        self.orders.append(order)

    def to_json(self):
        return {
            'id':self.id,
            'structure':self.structure,
            'vacancies':to_json(self.vacancies),
            'orders':list(map(lambda x:(x.number, x.used, x.correct, x.description), self.orders))
        }
    
    def get_base_orders(self):
        base_orders = list(filter(lambda x: x.is_use, self.orders))
        return list(sorted(base_orders, key = lambda x: (x.created, x.count_char), reverse=True))
    
    def fill_vacancy(self):
        i = 0
        i_max = len(self.vacancies) - 1
        for order in self.get_base_orders():
            self.vacancies[i].add_order(order)
            i = i+1 if i_max > i else 0 

def get_cities():
    params = {
        'token':TOKEN
    }
    cities = []
    for data in request_to_api('https://redsale.by/api/cities', params):
        cities.append(City(**data))
    return cities 

def process_section_vacancy():
    cities = get_cities()
    sections = dict()
    for city in cities:
        params = {
            'token':TOKEN,
            'cityId':city.id
        }
        for data in request_to_api('https://redsale.by/api/vacancies/sections', params):
            section = sections.get(data['sectionId'])
            if section is None:
                section = Section(**data)
                sections.update({section.id:section})
            section.add_vacancy(Vacancy(**data))
    return sections

async def get_section_structure(work_queue:asyncio.Queue):
    async with aiohttp.ClientSession() as session:
        while not work_queue.empty():
            section = await work_queue.get()
            async with session.get(f'https://redsale.by/api/sections/{section.id}/children?token=6PmWUehjZMugwn8mNxdrVqyG5F3wUmm') as response:
                for data in await response.json():
                    section.structure.update({data['sectionId']:data['specialization']})
            #await asyncio.sleep(0.01)
            print(work_queue.qsize())
            work_queue.task_done()

async def fill_section_structure(sections:Dict[int, Section]):
    work_queue = asyncio.Queue()
 
    for section in sections.values():
        await work_queue.put(section)

    await asyncio.gather(
        asyncio.create_task(get_section_structure(work_queue)),
        asyncio.create_task(get_section_structure(work_queue)),
        asyncio.create_task(get_section_structure(work_queue)),
    )

async def process_orders(work_queue:asyncio.Queue):
    async with aiohttp.ClientSession() as session:
        while not work_queue.empty():
            section, id_ = await work_queue.get()
            
            params = {
                'token':TOKEN,
                'sectionId':id_
            }
            async with session.get('https://redsale.by/api/orders', params = params) as response:
                for data_order in await response.json():
                    section.add_order(ManagerOrder.get_or_create(**data_order))
            print(work_queue.qsize())
            work_queue.task_done()

async def fill_orders(sections:Dict[int, Section]):

    work_queue = asyncio.Queue()
 
    for section in sections.values():
        for id_ in section.structure:
            await work_queue.put((section, id_))

    await asyncio.gather(
        asyncio.create_task(process_orders(work_queue)),
        asyncio.create_task(process_orders(work_queue)),
        asyncio.create_task(process_orders(work_queue)),
        asyncio.create_task(process_orders(work_queue)),
        asyncio.create_task(process_orders(work_queue)),
    )

async def vacancy_send_data(work_queue):
    async with aiohttp.ClientSession() as session:
        while not work_queue.empty():
            id_, data = await work_queue.get()
            async with session.post(f'https://redsale.by/api/vacancies/{id_}?token=6PmWUehjZMugwn8mNxdrVqyG5F3wUmm', json = data) as response:
                response = response
            print(work_queue.qsize())
            work_queue.task_done()

async def send_vacancy(sections:Dict[int, Section]):

    work_queue = asyncio.Queue()
    
    for section in sections.values():
        section.fill_vacancy()
        for vacancy in section.vacancies:
            await work_queue.put(vacancy.get_data_send())

    await asyncio.gather(
        asyncio.create_task(vacancy_send_data(work_queue)),
        asyncio.create_task(vacancy_send_data(work_queue)),
        asyncio.create_task(vacancy_send_data(work_queue)),
    )

async def start_fill(sections:Dict[int, Section]):
    await fill_section_structure(sections)
    await fill_orders(sections)
    await send_vacancy(sections)
    load_file.save_json(to_json(sections.values()), 'sections')

if __name__ == '__main__':
    start_time = time.monotonic()
    sections = process_section_vacancy()
    asyncio.run(start_fill(sections))
    end_time = time.monotonic()
    print(end_time - start_time)