"""Dataclip test"""

import json
import time
import sys
from threading import Thread

from eventplayback import EventPlayback, PlaybackTarget
from dataclip_source import DataClipSource


class TestTarget(PlaybackTarget):
    def on_event(self, event):
        #print(json.dumps(event))
        pass


def main():
    if len(sys.argv) != 2:
        print("Usage: {} DATA_CLIP_CONFIG_FILE".format(sys.argv[0]))
        sys.exit(1)

    source = DataClipSource(sys.argv[1])
    event_playback = EventPlayback(TestTarget(), source)
    event_playback.start()
    while True:
        event_playback.tick()
        time.sleep(0.001)


if __name__ == "__main__":
    main()
