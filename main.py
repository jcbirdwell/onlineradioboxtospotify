from bs4 import BeautifulSoup
import requests
import asyncio
import aiohttp
from custom_classes import tk, Host, fracture, lp, sp
import datetime


host = Host()


async def get_page(session, url):
    async with session.get(url) as resp:
        return await resp.content.read()


async def weeks_tracks(
        station: str
) -> dict:
    """
    Scrapes the track list for a given station from https://onlineradiobox.com

    Parameters
    ----------
    station
        the radio box uri for your desired station including country code ie 'cc/station'

    Returns
    -------
    list[days] of lists[tracks] each of which contains tracks as dict[artist, track]
    """

    domain = 'https://onlineradiobox.com'

    # detecting if country code present in provided uri, defaulting to us if not
    cc_station = station if '/' in station else f'us/{station}'

    # building full url with country coded station uri
    full_request = f'{domain}/{cc_station}/playlist/'
    print(full_request)
    invalid_count = 0

    # init return pack
    pack = {'station': cc_station, 'tracks': []}

    # grab the weeks page content asynchronously
    async with aiohttp.ClientSession() as session:
        tasks = []
        for day in range(7):
            url = f'{full_request}{day}'
            tasks.append(asyncio.ensure_future(get_page(session, url)))
        responses = await asyncio.gather(*tasks)

    for idx, resp in enumerate(responses):
        tracks = []

        # parse page content
        soup = BeautifulSoup(resp, "html.parser")

        # set time range from dates present on site
        if idx == 0:
            for item in soup.find_all('li', {'class': 'active', 'role': 'menuitem'}):
                if len(item['class']) == 1:
                    pack['start'] = item.findChild('span').text

        if idx == 6:
            for item in soup.find_all('li', {'class': 'active', 'role': 'menuitem'}):
                if len(item['class']) == 1:
                    pack['end'] = item.findChild('span').text

        # grab all artist - track strings from page
        for element in soup.find_all("td", {'class': 'track_history_item'}):
            at_lst = element.text.rstrip('\n').split(' - ')

            # ignore (some) oddities ie "Be right back!"
            try:
                tracks.append({'artist': at_lst[0], 'track': at_lst[1]})
            except IndexError:
                invalid_count += 1
        pack['tracks'].append(tracks)
    print(f'{invalid_count} invalid tracks.')
    print(pack['start'], pack['end'])
    return pack


def no_dupe_counts(
        station: dict,
        ordered: bool | None = False,
) -> dict:
    """
    Returns a list of dicts

    Parameters
    ----------
    station
        output of weeks_tracks
    ordered
        if true, returns tracks sorted by count in descending order

    Returns
    -------
    list of a dict for each unique track plus its total presence count
    """

    # init output dict
    no_dupes = {}

    # flatten input list
    all_tracks = [x for x in station['tracks'] for x in x]

    # convert dict to string and use hash of str as key for duplicates
    for item in all_tracks:

        # add if not present, tick matching counter if present
        if (h := hash(str(item))) not in no_dupes:
            no_dupes[h] = {**item, 'count': 1}
        else:
            no_dupes[h]['count'] += 1

    # drop temp hash key
    vals = list(no_dupes.values())

    # order if requested and edit input dict
    if ordered:
        station['tracks'] = sorted(vals, key=lambda x: x['count'], reverse=True)
    else:
        station['tracks'] = vals

    # return edited data
    return station


station_list = [
    'lightning100',
    'theindependentfm',
    '113fmaltnationradio',
    '113indienationindiepoprock'
]


async def pull_tracks(
        stations: list[dict]
) -> list[dict]:
    """
    Updates stations track data to include  spotify track uri, and standardized ISRC.

    Parameters
    ----------
    stations
        A list of station dicts from weeks tracks

    Returns
    -------
    The same list of station dicts with updated information, new keys = ['spotify_uri', 'isrc']

    """

    # loads primitive query string based dict for the tracks to avoid lookup
    cache = lp('track_cache', {})

    for station in stations:
        print(f"pulling {station['station']}")

        # init index reference and query tasklist
        search_ref = []
        searches = []
        for track in station['tracks']:

            # build query str in spotify search format and add it to track dict for final lookup
            track['query'] = f"artist:{track['artist']} track:{track['track']}"

            # check against cache, append index reference and tasklist
            if track['query'] not in cache:
                search_ref.append(track['query'])
                searches.append(host.asp.search(track['query'], types=('track',), limit=1))

        # async search requests
        results = await asyncio.gather(*searches)

        # use reference to match keys
        for idx, item in enumerate(search_ref):
            result = results[idx][0].items

            # check if result found, cache desired data accordingly
            if result:
                r = result[0]
                cache[item] = {'isrc': r.external_ids.get('isrc'), 'spotify_uri': r.uri}
            else:
                cache[item] = {'isrc': None, 'uri': None}

        # add data to original dicts using newly updated cache
        for track in station['tracks']:

            # use previously set query string as cache key lookup
            track.update(cache[track['query']])

    # update cache file and return updated station dict
    sp(cache, 'track_cache')
    return stations


def set_station_pl(
        stations: list[dict]
) -> list[str]:
    """
    Takes a list of data-filled station dicts and sets to a spotify playlist

    Parameters
    ----------
    stations
        list of station dicts, requires saturated track data be present

    Returns
    -------
    list[str]
        A list of spotify playlist links

    Raises
    ------
    KeyError
        when track data not present

    """

    links = []
    for station in stations:

        # build uris list for playlist
        uris = []
        for track in station['tracks']:
            if z := track.get('spotify_uri'):
                uris.append(z)

        # if no uris found, assume data not present
        if station['tracks'] and not uris:
            raise KeyError("Track data not present")

        # build description from station time delta keys
        desc = f"Detected tracks for station between {station['start']} and {station['end']}"

        # create playlist with station name
        pl = host.sp.playlist_create(user_id=host.user.id, name=station['station'], description=desc)

        # add tracks in chunks of 100
        for chunk in fracture(uris):
            host.sp.playlist_add(pl.id, chunk)

        # setting description (setting on creation rarely works correctly)
        host.sp.playlist_change_details(pl.id, description=desc)

        # append link to output
        links.append(pl.external_urls['spotify'])

    # return all playlist links
    return links


async def station_to_playlist(stations: list) -> list[str]:
    """

    Args:
        stations: list of radiobox station uris

    Returns: list of urls for the playlists of given stations

    """
    station_tracks = []
    for x in stations:

        # getting tracks, removing duplicates, and ordering by play count for each station
        station_tracks.append(no_dupe_counts(await weeks_tracks(x), True))

    # getting all track data and updating stations
    full_prepped = await pull_tracks(station_tracks)

    # setting playlists and returning links
    return set_station_pl(full_prepped)


def to_links(stations):
    # perform asynchronously
    return asyncio.run(station_to_playlist(stations))
