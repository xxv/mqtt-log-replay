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
        self._stats = {}

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

    @property
    def stats(self):
        return self._stats

    def events(self, start, end):
        print("Window: {} and {}".format(datetime.fromtimestamp(start/1000),
                                         datetime.fromtimestamp(end/1000)))
        try:
            unfiltered_events = self._read_clip()
            total_event_count = len(unfiltered_events)
            events = [event for event in unfiltered_events
                      if start <= (self.get_date(event).timestamp() * 1000) < end]
            events_earlier = len([event for event in unfiltered_events
                              if (self.get_date(event).timestamp() * 1000) < start])
            events_later = len([event for event in unfiltered_events
                            if (self.get_date(event).timestamp() * 1000) >= end])
            self._stats['events_total'] = total_event_count
            self._stats['events_in_window'] = len(events)
            self._stats['events_before_window'] = events_earlier
            self._stats['events_after_window'] = events_later
            print("{:d}/{:d} events in window ({:d} earlier, {:d} later)".format(
                len(events), total_event_count, events_earlier, events_later))
            print("{:d}/{:d} events in window".format(len(events), total_event_count))
            self._stats['event_date_first'] = None
            self._stats['event_date_last'] = None
            if unfiltered_events:
                first = self.get_date(unfiltered_events[0])
                last = self.get_date(unfiltered_events[-1])
                self._stats['event_date_first'] = str(first)
                self._stats['event_date_last'] = str(last)
                print("First is {} last is {}".format(first, last))

            return events

        except requests.RequestException as err:
            print("Got an error polling the dataclip <{}>. Waiting a minute and retrying...".format(err))
            time.sleep(60)
        return []
