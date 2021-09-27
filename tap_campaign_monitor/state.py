import json
import singer

from dateutil.parser import parse

LOGGER = singer.get_logger()


def get_last_record_value_for_table(state, table):
    last_value = state.get('bookmarks', {}) \
                      .get(table, {}) \
                      .get('last_record')

    if last_value is None:
        return None

    return parse(last_value)

def incorporate(state, table, field, value):
    if value is None:
        return state

    new_state = state.copy()

    parsed = parse(value).strftime("%Y-%m-%d %H:%M:%S")

    if 'bookmarks' not in new_state:
        new_state['bookmarks'] = {}

    if(new_state['bookmarks'].get(table, {}).get('last_record') is None or
       new_state['bookmarks'].get(table, {}).get('last_record') < value):
        new_state['bookmarks'][table] = {
            'field': field,
            'last_record': parsed,
        }

    return new_state

# TODO: this needs a proper refactoring, but ¯\_(ツ)_/¯ for now
def get_last_page_value_for_table(state, table):
    last_value = state.get('bookmarks', {}) \
                      .get(table, {}) \
                      .get('last_page')

    if last_value is None:
        return 1
    
    try:
        return int(last_value)
    except:
        return 1

def incorporate_page(state, table, field, value):
    if value is None:
        return state

    new_state = state.copy()

    parsed = str(value)

    if 'bookmarks' not in new_state:
        new_state['bookmarks'] = {}

    if(new_state['bookmarks'].get(table, {}).get('last_page') is None or
       new_state['bookmarks'].get(table, {}).get('last_page') < value):
        new_state['bookmarks'][table] = {
            'field': field,
            'last_page': parsed,
        }

    return new_state

def save_state(state):
    if not state:
        return

    LOGGER.info('Updating state.')

    singer.write_state(state)


def load_state(filename):
    if filename is None:
        return {}

    try:
        with open(filename) as handle:
            return json.load(handle)
    except:
        LOGGER.fatal("Failed to decode state file. Is it valid json?")
        raise RuntimeError
