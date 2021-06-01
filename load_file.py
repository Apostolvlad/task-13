import json
import os
import re

def loud_json(name):
    if not os.path.exists(f'{name}.json'): return None
    with open(f'{name}.json', encoding='UTF-8') as f:
        return json.load(f)

def save_json(base, name):
    with open(f'{name}.json', "w", encoding='UTF-8') as f:
        f.write(json.dumps(base, indent=4, ensure_ascii=False))

def loud_txt(name, symbol_split = '\n'):
    if not os.path.exists(f'{name}.txt'): return None
    with open(f'{name}.txt', encoding='UTF-8') as f:
        return f.read().split(symbol_split)

def save_txt(base, name, symbol_split = '\n'):
    with open(f'{name}.txt', "w", encoding='UTF-8') as f:
        f.write(symbol_split.join(base))

def convert_cvs_json(filename, titles):
    result = list()
    with open(filename, encoding='UTF-8') as f:
        result = f.read().replace(',,', ',0,').replace('"/n"', '"\n"')
        if result.find('"') != -1: 
            result = re.sub('".*?"',  lambda x: x.group().replace(',', '$@'), result)
            result = result.replace(',', ";")
            result = result.replace('$@', ",")
        else:
            result = result.replace(',', ";")
        sh = ('(.*?);+' * (len(titles)))[:-2]
        result = re.findall(f'{sh}\n+', result) #re.findall(r'"(.*?)","(.*?)","(\d*?)","(\d*?)","(.*?)","(\d*?)",(.*?),(.*?)', base_orders)#
        result.pop(0)
        result = tuple(map(lambda x: tuple(map(lambda y: y.replace('"', ''), x)), result))
        result = tuple(map(lambda x: dict(zip(titles, x)), result))
    return result