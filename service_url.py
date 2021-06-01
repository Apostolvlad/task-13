def get_url_type(url):
    #if url.find('redsale.by/vacancy/repetitory') !=-1 : return 5
    if url.find('redsale.by/vacancy') != -1: return 4
    if url.find('redsale.by/mebel') !=-1 : return 3
    return 3

def get_url_level(url):
    url = url.replace('/tag/', '/').replace('/price/', '/')
    level = tuple(url).count('/') - get_url_type(url) + 1
    return level

def get_url_parent0(url):
    url = url.replace('/tag/', '/').replace('/price/', '/')
    url_l = url.split('/')
    return '/'.join(url_l[:3])

def get_url_parent(url):
    url = url.replace('/tag/', '/').replace('/price/', '/')
    url_l = url.split('/')
    return '/'.join(url_l[:len(url_l) - 1])

def get_url_part(url, level):
    url = url.replace('/tag/', '/').replace('/price/', '/')
    type_url = get_url_type(url)
    url_l = url.split('/')
    return '/'.join(url_l[:type_url + level])

def get_url_body(url):
    return '/'.join(url.split('/')[3:])

def get_url_end(url):
    url = url.replace('/tag/', '/').replace('/price/', '/')
    return url.split('/')[-1]

def test_level():
    print('======================================================================================')
    print('ТЕСТИРОВАНИЕ НА УРОВЕНЬ')
    for info in TEST_BASES:
        if not (info.get('url') and info.get('level')): 
            print('пропускаем', info)
            continue
        print(get_url_level(info['url']), '==', info['level'], 'OK' if get_url_level(info['url'])==info['level'] else 'ERROR')

def test_parent0():
    print('======================================================================================')
    print('ТЕСТИРОВАНИЕ НА PARENT0')
    for info in TEST_BASES:
        if not (info.get('url') and info.get('parent0')): 
            print('пропускаем', info)
            continue
        print(get_url_parent0(info['url']), '==', info['parent0'], 'OK' if get_url_parent0(info['url']) ==info['parent0'] else 'ERROR')

def test_parent():
    print('======================================================================================')
    print('ТЕСТИРОВАНИЕ НА PARENT0')
    for info in TEST_BASES:
        if not (info.get('url') and info.get('parent0')): 
            print('пропускаем', info)
            continue
        print(get_url_parent(info['url']), '==', info['parent'], 'OK' if get_url_parent(info['url']) ==info['parent'] else 'ERROR')

def test_part():
    print('======================================================================================')
    print('ТЕСТИРОВАНИЕ НА PART')
    for info in TEST_BASES:
        if not (info.get('url') and info.get('parent0')): 
            print('пропускаем', info)
            continue
        print(info['level_part'], get_url_part(info['url'], info['level_part']), '==', info['part'], 'OK' if get_url_part(info['url'], info['level_part']) == info['part'] else 'ERROR')

def test_end():
    print('======================================================================================')
    print('ТЕСТИРОВАНИЕ НА END')
    for info in TEST_BASES:
        if not (info.get('url') and info.get('end')): 
            print('пропускаем', info)
            continue
        print(get_url_end(info['url']), '==', info['end'], 'OK' if get_url_end(info['url']) == info['end'] else 'ERROR')

def test_body():
    print('======================================================================================')
    print('ТЕСТИРОВАНИЕ НА BODY')
    for info in TEST_BASES:
        if not (info.get('url') and info.get('body')): 
            print('пропускаем', info)
            continue
        print(get_url_body(info['url']), '==', info['body'], 'OK' if get_url_body(info['url']) == info['body'] else 'ERROR')

TEST_BASES = [
    {
        'url':'https://redsale.by/vacancy/repetitory',
        'level':1,
        'parent':'https://redsale.by/vacancy',
        'parent0':'https://redsale.by',
        'level_part':1,
        'part':'https://redsale.by/vacancy/repetitory',
        'body':'vacancy/repetitory',
        'end':'repetitory'
    },
    {
        'url':'https://redsale.by/vacancy/repetitory/mxk',
        'level':2,
        'parent':'https://redsale.by/vacancy/repetitory',
        'parent0':'https://redsale.by',
        'level_part':1,
        'part':'https://redsale.by/vacancy/repetitory',
        'body':'vacancy/repetitory/mxk',
        'end':'mxk'
    },
    {
        'url':'https://redsale.by/vacancy/artist/florists',
        'level':2,
        'parent':'https://redsale.by/vacancy/artist',
        'parent0':'https://redsale.by',
        'level_part':1,
        'part':'https://redsale.by/vacancy/artist',
        'body':'vacancy/artist/florists',
        'end':'florists'
    },
    {
        'url':'https://redsale.by/mebel/peretyazhka-mebeli/peretyazhka-stulev',
        'level':3,
        'parent':'https://redsale.by/mebel/peretyazhka-mebeli',
        'parent0':'https://redsale.by',
        'level_part':2,
        'part':'https://redsale.by/mebel/peretyazhka-mebeli',
        'body':'mebel/peretyazhka-mebeli/peretyazhka-stulev',
        'end':'peretyazhka-stulev'
    },
    {
        'url':'https://redsale.by/mebel/peretyazhka-mebeli/peretyazhka-divana/tag/peretyazhka-kuhonnogo-ugolka',
        'level':4,
        'parent':'https://redsale.by/mebel/peretyazhka-mebeli/peretyazhka-divana',
        'parent0':'https://redsale.by',
        'level_part':1,
        'part':'https://redsale.by/mebel',
        'body':'mebel/peretyazhka-mebeli/peretyazhka-divana/tag/peretyazhka-kuhonnogo-ugolka',
        'end':'peretyazhka-kuhonnogo-ugolka'
    },
    {
        'url':'https://redsale.by/mebel/peretyazhka-mebeli/frunzenskii-raion',
        'level':2, 
        'parent':'https://redsale.by/mebel/peretyazhka-mebeli',
        'parent0':'https://redsale.by',
        'level_part':1,
        'part':'https://redsale.by/mebel',
        'body':'mebel/peretyazhka-mebeli/frunzenskii-raion',
        'end':'frunzenskii-raion'
    }
]

if __name__ == '__main__':
    #assert get_url_main2('https://redsale.by/vacancy/artist/florists', 2) == 'https://redsale.by/vacancy/artist', get_url_main2('https://redsale.by/vacancy/artist/florists', 2)
    #assert get_url_main4('https://redsale.by/vacancy/artist/florists', 3) == 'https://redsale.by/vacancy/artist/florists', get_url_main4('https://redsale.by/vacancy/artist/florists', 3)
    #assert get_url_main2('https://redsale.by/mebel', 1) == 'https://redsale.by/mebel', get_url_main2('https://redsale.by/mebel', 1)
    #assert get_url_main2('https://redsale.by/mebel/kamennaya-gorka', 1) == 'https://redsale.by/mebel', get_url_main2('https://redsale.by/mebel/kamennaya-gorka', 1)
    
    #assert get_url_main3('https://redsale.by/mebel/kamennaya-gorka') == 'https://redsale.by', get_url_main3('https://redsale.by/mebel/kamennaya-gorka')

    test_level()
    test_parent0()
    test_parent()
    test_part()
    test_body()
    test_end()
    print()
    