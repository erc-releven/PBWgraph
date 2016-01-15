import requests
from lxml import etree


def etree_to_json(t):
    result = []
    for child in t:
        if child.tag != 'geoname':
            continue
        loc = {'geometry':
               {'location':
                {'lat': float(child.find("lat").text),
                 'lng': float(child.find("lng").text)}},
               'name': child.find("name").text,
               'types': child.find("fclName").text}
        # Does it have a bounding box?
        bbx = child.find('bbox')
        if bbx is not None:
            loc['geometry']['viewport'] = {'southwest':
                                           {'lat': float(bbx.find('south').text),
                                            'lng': float(bbx.find('west').text)},
                                           'northeast':
                                           {'lat': float(bbx.find('north').text),
                                            'lng': float(bbx.find('east').text)}}
        result.append(loc)
    return result


def lookup_place(querystring):
    search_endpoint = 'http://api.geonames.org/search'
    search_params = {
        'username': 'aurum',
        'q': querystring,
        'type': 'xml',
        'style': 'FULL'
    }

    r = requests.get(search_endpoint, params=search_params)
    return etree_to_json(etree.fromstring(r.content))
