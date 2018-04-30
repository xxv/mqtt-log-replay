"""Event playback"""

from __future__ import print_function
from datetime import datetime, timedelta
import time
import threading
import queue
import pytz


class EventSource(object):
    """A time-windowed source of events."""

    def events(self, start, end):
        """Return a list of events within the time window."""
        pass

    def get_date(self, event):
        """Get the date for the given event."""
        pass

    @property
    def window_size(self):
        """Return the window size, in seconds."""
        pass

    @property
    def window_offset(self):
        """Return the window playback offset, in seconds."""
        pass

    @property
    def stats(self):
        """Return a dictionary of stats."""
        return {}


class PlaybackTarget(object):
    """A class that receives on_event() callbacks."""

    def on_event(self, event):
        """Called when an event occurs."""
        raise Exception("must implement this method")

    def debug_message(self, message):
        print(message)

    def publish_stats(self, stats):
        print(stats)


class DebugTarget(PlaybackTarget):
    def on_event(self, event):
        print("Event: {}".format(event))


class EventPlayback(object):
    """Replays the events from the given EventSource to the PlaybackTarget"""

    def __init__(self, target, event_source, min_buffer_size=40):
        self._events_queue = queue.Queue()
        self._load_lock = threading.Event()
        self._event = None
        self._offset_event_time = None
        self._stats_interval = timedelta(seconds=1)
        self._stats_last_publish = None
        self._target = target
        self._event_source = event_source
        self._load_window = timedelta(seconds=event_source.window_size)
        self._playback_offset = timedelta(seconds=event_source.window_offset)
        self._min_buffer_size = min_buffer_size

    @staticmethod
    def _to_ms(atime):
        """Convert a datetime to milliseconds."""
        return int(time.mktime(atime.timetuple()) * 1000 + atime.microsecond/1000)

    def _publish_stats(self):
        stats = {}
        stats['buffer_size'] = self._events_queue.qsize()
        stats['min_buffer_size'] = self._min_buffer_size
        stats['source'] = self._event_source.stats
        self._target.publish_stats(stats)

    def _load_events(self):
        last_load = None
        while True:
            self._load_lock.wait()
            self._target.debug_message("Loading events...")

            interval = int(self._load_window.total_seconds() * 1000)
            end = EventPlayback._to_ms(datetime.now() - self._playback_offset) + interval
            if last_load:
                delay = max(0, (last_load + interval/2) - end)
                if delay:
                    self._target.debug_message("Reloading too fast. Waiting...")
                time.sleep(delay / 1000)
                end = EventPlayback._to_ms(datetime.now() - self._playback_offset) + interval
            else:
                last_load = end - interval
            events = self._event_source.events(last_load, end)
            for event in events:
                self._events_queue.put(event)
            self._target.debug_message("Loaded {:d} events.".format(len(events)))
            last_load = end
            self._load_lock.clear()

    def start(self):
        """Start the playback thread."""
        thread = threading.Thread(target=self._load_events)
        thread.daemon = True
        thread.start()

    def tick(self):
        """Call this periodically from your animation thread to deliver events."""
        # if the buffer is low, fill it up
        if self._events_queue.qsize() < self._min_buffer_size and not self._load_lock.is_set():
            self._load_lock.set()
        if not self._event:
            try:
                self._event = self._events_queue.get(block=False)
                self._offset_event_time = self._event_source.get_date(self._event) + self._playback_offset
            except queue.Empty:
                self._event = None
        if self._event and self._offset_event_time <= datetime.now(tz=pytz.utc):
            print('.', end='', flush=True)
            self._target.on_event(self._event)
            self._event = None
        now = datetime.now()
        if not self._stats_last_publish or (
                now > (self._stats_last_publish + self._stats_interval)):
            self._publish_stats()
            self._stats_last_publish = now
