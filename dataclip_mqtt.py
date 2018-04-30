"""Dataclip -> MQTT"""

import json
import time
import sys
from threading import Thread

from mqtt_base import MQTTBase
from eventplayback import EventPlayback, PlaybackTarget
from dataclip_source import DataClipSource


class MQTTTarget(MQTTBase, PlaybackTarget):
    def __init__(self, config_file):
        MQTTBase.__init__(self, config_file=config_file)
        self._topic = 'event'

    def on_connect(self, client, userdata, flags, conn_result):
        print("Connected to MQTT server.")

    def on_event(self, event):
        self.mqtt.publish(self._topic, json.dumps(event))


def main():
    if len(sys.argv) != 3:
        print("Usage: {} MQTT_CONFIG_FILE DATA_CLIP_CONFIG_FILE".format(sys.argv[0]))
        sys.exit(1)

    target = MQTTTarget(sys.argv[1])
    target.connect()
    mqtt_thread = Thread(target=target.loop_forever)
    mqtt_thread.start()
    source = DataClipSource(sys.argv[2])
    event_playback = EventPlayback(target, source)
    event_playback.start()
    while True:
        event_playback.tick()
        time.sleep(0.001)


if __name__ == "__main__":
    main()
