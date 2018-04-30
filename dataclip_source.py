"""Heroku dataclip event source"""

import csv
from datetime import datetime
import json
import time
import requests
import dateutil.parser

from eventplayback import EventSource


class DataClipSource(EventSource):
    """A source of events, replayed from a Heroku dataclip.

    See dataclip.json.example for a sample config file."""

    def __init__(self, config_file):
        self._date_parser = dateutil.parser

        with open(config_file) as config_in:
            config = json.load(config_in)
            self.url = config['url']
            self._window_size = config['window_size']
            self._window_offset = config['window_offset']
            self._time_column = config['time_column']

    def get_date(self, event):
        return self._date_parser.parse(event[self._time_column]+'Z')

    @property
    def window_size(self):
        return self._window_size

    @property
    def window_offset(self):
        return self._window_offset

    def _read_clip(self):
        result = requests.get(self.url)

        if result.status_code == 200:
            return list(csv.DictReader(result.text.splitlines()))

        print("Error accessing data: {}".format(result))
        return []

    def events(self, start, end):
        print("Window: {} and {}".format(datetime.fromtimestamp(start/1000),
                                         datetime.fromtimestamp(end/1000)))
        try:
            unfiltered_events = self._read_clip()
            events = [event for event in unfiltered_events
                      if start <= (self.get_date(event).timestamp() * 1000) < end]
            events_earlier = [event for event in unfiltered_events
                              if (self.get_date(event).timestamp() * 1000) < start]
            events_later = [event for event in unfiltered_events
                            if (self.get_date(event).timestamp() * 1000) >= end]
            print("{:d}/{:d} events in window ({:d} earlier, {:d} later)".format(
                len(events), len(unfiltered_events), len(events_earlier), len(events_later)))
            print("{:d}/{:d} events in window".format(len(events), len(unfiltered_events)))
            if unfiltered_events:
                print("First is {} last is {}".format(
                    self.get_date(unfiltered_events[0]), self.get_date(unfiltered_events[-1])))

            return events

        except requests.RequestException as err:
            print("Got an error polling the dataclip <{}>. Waiting a minute and retrying...".format(err))
            time.sleep(60)
        return []
