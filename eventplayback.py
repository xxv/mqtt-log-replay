"""Event playback"""

from __future__ import print_function
import datetime
import time
import threading
import queue
import pytz


class EventSource(object):
    """A time-windowed source of events"""

    def events(self, start, end):
        """Return a list of events within the time window"""
        pass

    def get_date(self, event):
        """Gets the date for the given event"""
        pass

    @property
    def window_size(self):
        """Returns the window size, in seconds."""

    @property
    def window_offset(self):
        """Returns the window playback offset, in seconds."""


class PlaybackTarget(object):
    def on_event(self, event):
        raise Exception("must implement this method")


class DebugTarget(PlaybackTarget):
    def on_event(self, event):
        print("Event: {}".format(event))


class EventPlayback(object):
    """Replays the events from the given EventSource to the PlaybackTarget"""
    events_queue = queue.Queue()
    load_lock = threading.Event()
    event = None
    offset_event_time = None

    def __init__(self, target, event_source, min_buffer_size=40):
        self.target = target
        self.event_source = event_source
        self.load_window = datetime.timedelta(seconds=event_source.window_size)
        self.playback_offset = datetime.timedelta(seconds=event_source.window_offset)
        self.min_buffer_size = min_buffer_size

    def to_microseconds(self, atime):
        return int(time.mktime(atime.timetuple()) * 1000 + atime.microsecond/1000)

    def load_events(self):
        last_load = None
        while True:
            self.load_lock.wait()
            print("Loading events...")

            interval = int(self.load_window.total_seconds() * 1000)
            end = self.to_microseconds(datetime.datetime.now() - self.playback_offset) + interval
            if last_load:
                delay = max(0, (last_load + interval/2) - end)
                if delay:
                    print("Reloading too fast. Waiting...")
                time.sleep(delay / 1000)
                end = self.to_microseconds(datetime.datetime.now() - self.playback_offset) + interval
            else:
                last_load = end - interval
            events = self.event_source.events(last_load, end)
            for event in events:
                self.events_queue.put(event)
            print("Loaded {:d} events.".format(len(events)))
            last_load = end
            self.load_lock.clear()

    def start(self):
        thread = threading.Thread(target=self.load_events)
        thread.daemon = True
        thread.start()

    def tick(self):
        # if the buffer is low, fill it up
        if self.events_queue.qsize() < self.min_buffer_size and not self.load_lock.is_set():
            self.load_lock.set()
        if not self.event:
            try:
                self.event = self.events_queue.get(block=False)
                self.offset_event_time = self.event_source.get_date(self.event) + self.playback_offset
            except queue.Empty:
                self.event = None
        if self.event and self.offset_event_time <= datetime.datetime.now(tz=pytz.utc):
            print('.', end='', flush=True)
            self.target.on_event(self.event)
            self.event = None
