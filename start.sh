#!/bin/bash

export DISPLAY=:99

# Start PulseAudio daemon in user mode (not system mode for Docker compatibility)
pulseaudio --start --exit-idle-time=-1

# Create virtual audio sink and source for Chrome to use
pactl load-module module-null-sink sink_name=DummyOutput sink_properties=device.description="Virtual_Sink"
pactl load-module module-virtual-source source_name=VirtualMic master=DummyOutput.monitor

# Set default audio devices
pacmd set-default-sink DummyOutput
pacmd set-default-source VirtualMic

# Give PulseAudio a moment to initialize
sleep 1

xvfb-run --server-num=99 --server-args='-screen 0 1280x720x24' npm run start
