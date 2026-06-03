# 1. Project Plan: Edge Audio Subsystem (Raspberry Pi Only)



This plan targets the execution tasks restricted entirely to the **Raspberry Pi client-side repository boundaries**.

[Hardware Mic Audio] ➔ [PyAudio Buffer Stream] ➔ [WebRTC VAD] ➔ [MQTT Outbound (QoS 0)]



[Physical Strobe/Buzzer] ⬅ [GPIO Signal High] ⬅ [MQTT Alert Callback (QoS 1)]

# 📍 Phase 1: Edge Audio Acquisition & VAD

Objective: Access hardware mic arrays, isolate voice/distress audio frames in system RAM to conserve network bandwidth, and slice data into transit blocks.



- [x] Task 1.1: Audio Hardware Interface (edge_audio.py)

Inputs: Hardware device index configuration parameter for the microphone.

Process: Instantiate a background, non-blocking PyAudio capture loop. Hardware stream parameters must strictly lock to: 16000Hz sample rate, Mono channel, 16-bit signed PCM format.

Outputs: Continuous chunk byte-array stream forwarded directly to the VAD layer.

- [x] Task 1.2: Voice Activity Detection (VAD) & Memory Buffering (edge_audio.py)

Inputs: Raw byte stream from Task 1.1.

Process: Run a lightweight VAD framework (e.g., silero-vad or webrtcvad) on the local CPU cores. Upon detecting audio passing above the environmental noise floor, open a rolling RAM buffer. Package the active window into discrete lengths of 2 to 3 seconds.

Outputs: Raw, uncompressed PCM .wav byte payloads of active audio frames.

## 🎯 Phase 1 Success Criteria

RAM Processing Only: No disk writing or local storage. Do not call local filesystem storage writes (.wav or .mp3 exports), avoiding SD card write overhead.

Resource Optimization: Total processing pipeline execution must remain <= 15% CPU consumption on a baseline Raspberry Pi 4.

# 📍 Phase 2: Lightweight Transport Pipeline

Objective: Maintain connection protocols to send outbound audio streams to the host PC and activate local physical alerts from incoming emergency payloads.



- [x] Task 2.1: Outbound Audio Publishing (edge_audio.py)

Inputs: 2-to-3 second uncompressed audio byte arrays from Task 1.2.

Process: Initialize a persistent asynchronous TCP network socket connection using paho-mqtt (leveraging the v2.0+ Callback API structure). Publish the binary payloads to the topic: aquatic_safety/edge_1/audio.

Network Tuning: Use QoS 0 (At most once / Fire-and-forget) to minimize payload latency over local Wi-Fi.

- [x] Task 2.2: Inbound Emergency Alert Listener (edge_audio.py)

Inputs: Subscription callback stream tracking the topic: aquatic_safety/edge_1/alerts.

Process: Attach a persistent listener thread to wait for emergency triggers. Upon receipt of a valid JSON emergency packet, decode the payload data and toggle hardware pins (GPIO HIGH).

Hardware Output: Actuate physical local emergency indicator relays (e.g., electronic siren, LED warning strobe, or system alert buzzer).

Network Tuning: Use QoS 1 (At least once / Guaranteed delivery) to enforce warning system communication stability.

## 🎯 Phase 2 Success Criteria

Edge Transport Latency: Outbound processing to socket output execution speed must hit targets under < 50 milliseconds.

Network Resiliency: Implement auto-reconnection try/except loops to catch Wi-Fi dropouts or temporary network broker loss without generating script crashes or execution halts.
