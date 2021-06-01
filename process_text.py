import re

BLACK_KEYS = (
    'цена', 
    'стоимот', 
    'место выполнения работы'
)

def check_price(description:str):
    i_price = description.find('стоимость')
    if i_price == -1: i_price = description.find('цена')
    salary = 'договорная'
    if i_price != -1:
        f_salary = re.findall('.*?([0-9]{1,2})', description[i_price:])
        description = description[:description.rfind('. ', 1, i_price)]
        if len(f_salary): salary = int(f_salary[0])
    return description, salary

def check_adress(description:str):
    i_address = description.find('Адрес')
    if i_address != -1:
        description = description[:i_address]
    return description

def get_items(split_description:list):
    base_description = dict()
    key = split_description.pop(0)
    while len(split_description) > 0:
        value = split_description.pop(0)
        i_key = value.rfind('. ')
        key_new = ''
        if i_key != -1:
            key_new = value[i_key + 2:]
            value = value[:i_key]
        base_description.update({key:value})
        key = key_new
    return base_description

def check_upper(description:str):
    for d in description.split(' '):
        if d.isupper(): return True
    return False
            
def check_description(description:str):
    if description is None or check_upper(description): return {'correct':False}
    description = description.replace('"', '')
    description, salary = check_price(description)
    description = check_adress(description)
    split_description = description.split(': ')
    if len(split_description) < 2: return {'correct':False, 'description':description, 'salary':salary}
    base_description = get_items(split_description)
    for key in tuple(base_description.keys()):
        key_ = key.lower()
        for word in BLACK_KEYS:
            if key_.find(word) != -1:
                base_description.pop(key)
                break
    base_len = dict(map(lambda x: (x[0], len(x[1])), base_description.items()))
    base_len = dict(sorted(base_len.items(), key = lambda x:x[1], reverse=True))
    base_len_key = list(base_len.keys())[:5]
    count_char = sum(list(base_len.values())[:5])
    for key, value in list(base_description.items()):
        if key in base_len_key and len(value) > 2: continue
        base_description.pop(key)
    description = list()
    for key, value in base_description.items():
        description.append(key)
        description.append(': ')
        description.append(value)
        if value.strip()[-1] != '.': description.append('.')
        description.append(' ')
    description = ''.join(description)
    return {'correct':len(base_description) > 1, 'description':description, 'salary':salary, 'publish':base_description, 'count_char':count_char, 'base_len':base_len}


if __name__ == '__main__':
    print(check_description('Требуется специалист косметолог для чистки лица. Необходимо подобрать самый оптимальный вариант чистки лица и выполнить её в любое ближайшее время. Место выполнения работы косметологом не важно.'))