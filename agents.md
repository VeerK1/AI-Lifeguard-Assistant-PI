# Agent Context & System Rules: Audio Subsystem

## High-Level Objective
You are an expert AI Engineer executing tasks for the Audio Processing & Distress Detection subsystem of an AI Lifeguard Assistant. This system captures raw audio on a Raspberry Pi (Edge), streams it over MQTT to a Windows PC (Inference Server), and analyzes it locally and offline using Machine Learning to detect swimmers in distress.

---

## System Architecture & File Tree
Do not modify the directory structure without explicit permission.

---

## Current Project
Right now this project is on a windows computer, HOWEVER, the code and project is meant for the edge device -- a raspberry PI 4B. 

---

## Critical Constraints & Rules (Token-Savers)
API Constraints: You MUST use Paho-MQTT v2.0+. Traditional v1.x configurations will break the system. 
All clients must initialize with ```callback_api_version=mqtt.CallbackAPIVersion.VERSION2```.
Callback Signatures: Your on_connect callbacks must use five arguments: (client, userdata, flags, reason_code, properties).
Zero Disk I/O: Audio data must never be saved as .wav or .mp3 files to disk on either the Pi or Windows server. You must stream and decode payloads entirely in volatile memory utilizing io.BytesIO().
Dependencies: Keep dependencies minimal. Use PyAudio and paho-mqtt on the edge. Use faster-whisper, torch, and paho-mqtt on the server.

---

## Network Topology Configuration

If your changing networking configurations or need more info revolving networking check: [Networking.md](Networking.md)

---

## Performance Benchmarks (Success Criteria)

When writing or optimizing code, ensure your scripts meet or exceed these metrics:
    One-Way Transport Latency: $< 50\text{ms}$ (Pi to Server MQTT transmission).
    Server Inference Bound: $< 400\text{ms}$ processing execution window for a 3-second audio block.
    Total Loop Response Speed: $< 1.5\text{ seconds}$ total time elapsed from the vocalized pool scream to the hardware GPIO trigger on the Pi.


---

## Instructions for Code Generation
When asked to write or modify code under this architecture:Avoid placeholder code, comments like # TODO, or partial scripts. Write complete, functional files.
Build local, defensive try/except blocks to catch network dropouts without halting the script execution loop.
Validate that your proposed changes do not violate the Zero Disk I/O rule.

## Tasks

look at [PiPlan.md](PiPlan.md)
If your task has checkboxes track which tasks are completed by updating them in [PiPlan.md](PiPlan.md)