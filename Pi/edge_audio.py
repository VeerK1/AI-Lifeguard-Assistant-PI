import time
import os
import sys
import io
import wave
import collections
import threading
import struct
import math
import json
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

# Safe optional imports for Raspberry Pi hardware
try:
    import webrtcvad
except ImportError:
    webrtcvad = None

import pyaudio

# Hardware Pins & Networking Defaults
MQTT_BROKER = os.environ.get("MQTT_BROKER", "192.168.1.156")
AUDIO_TOPIC = "aquatic_safety/edge_1/audio"
ALERT_TOPIC = "aquatic_safety/edge_1/alerts"
GPIO_BUZZER_PIN = 18

# Audio Processing Configuration
SAMPLE_RATE = 16000
CHANNELS = 1
SAMPLE_WIDTH = 2  # 16-bit signed PCM is 2 bytes
FRAME_DURATION_MS = 30  # WebRTC VAD requires 10, 20, or 30ms
CHUNK_SIZE = int(SAMPLE_RATE * FRAME_DURATION_MS / 1000)  # 480 samples
FRAME_BYTES = CHUNK_SIZE * SAMPLE_WIDTH  # 960 bytes

# VAD State Machine Thresholds
VAD_MODE = int(os.environ.get("VAD_MODE", "2"))  # 0 to 3 aggressiveness
PRE_TRIGGER_MS = 500  # Keep 500ms pre-speech audio
PRE_TRIGGER_FRAMES = int(PRE_TRIGGER_MS / FRAME_DURATION_MS)
MAX_SILENCE_MS = 1000  # Stop recording after 1 second of silence hangover
MAX_RECORDING_MS = 3000  # Stream in discrete 2-3 second packages max
MIN_RECORDING_MS = 1000  # Avoid sending sub-1 second audio fragments

# Microphone Index (None selects standard default)
MICROPHONE_INDEX = os.environ.get("MICROPHONE_INDEX")
if MICROPHONE_INDEX is not None:
    MICROPHONE_INDEX = int(MICROPHONE_INDEX)

# Safe GPIO Setup
GPIO_AVAILABLE = False
try:
    import RPi.GPIO as GPIO
    GPIO.setmode(GPIO.BCM)
    GPIO.setup(GPIO_BUZZER_PIN, GPIO.OUT)
    GPIO_AVAILABLE = True
    print(f"[SUCCESS] Hardware GPIO buzzer successfully configured on BCM pin {GPIO_BUZZER_PIN}.")
except ImportError:
    print("[INFO] RPi.GPIO module not found. Hardware alerting will operate in SIMULATION mode.")
except Exception as e:
    print(f"[WARN] Failed to initialize GPIO hardware pin: {e}. Operating in SIMULATION mode.")


def trigger_hardware_alert(duration=1.5):
    """
    Actuates a local warning sequence (e.g. flashing/chirping buzzer) when a swimmer is in distress.
    This runs safely using a mock fallback if physical GPIO libraries or hardware is absent.
    """
    print("\n[ALERT] DISTRESS ALERT RECEIVED: ACTUATING WARNING DEVICE AT THE EDGE!")
    if GPIO_AVAILABLE:
        try:
            end_time = time.time() + duration
            while time.time() < end_time:
                GPIO.output(GPIO_BUZZER_PIN, GPIO.HIGH)
                time.sleep(0.1)
                GPIO.output(GPIO_BUZZER_PIN, GPIO.LOW)
                time.sleep(0.1)
            print("[SUCCESS] Physical alerting cycle completed successfully.")
        except Exception as e:
            print(f"[WARN] Error actuating GPIO warning hardware: {e}")
    else:
        # Standard simulation alert
        print("[BUZZER SIMULATION] BEEP-BEEP-BEEP-BEEP-BEEP!")
        time.sleep(duration)


# Paho MQTT Callback Setup (Strictly CallbackAPIVersion.VERSION2 for Paho v2.0+)
def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print(f"[SUCCESS] Connected to MQTT Broker successfully at {MQTT_BROKER}!")
        # Subscribe to alerts at QoS 1 to guarantee emergency packet ingestion
        client.subscribe(ALERT_TOPIC, qos=1)
        print(f"[INFO] Subscribed to alerts topic: {ALERT_TOPIC} (QoS 1)")
    else:
        print(f"[ERROR] Connection failed with reason code: {reason_code}")


def on_message(client, userdata, msg):
    """
    Invoked when a distress alert JSON payload is published from the inference server.
    """
    try:
        alert_payload = msg.payload.decode()
        print(f"\n[ALERT] ALERT INGESTED FROM INFERENCE SERVER: {alert_payload}")
        
        # Parse the JSON payload
        try:
            alert_data = json.loads(alert_payload)
            if alert_data.get("status") == "CRITICAL_DISTRESS":
                # Start hardware actuation thread to avoid blocking MQTT reception loop
                alert_thread = threading.Thread(target=trigger_hardware_alert, args=(2.0,))
                alert_thread.daemon = True
                alert_thread.start()
        except Exception as json_err:
            print(f"[WARN] Non-JSON alert or parsing error: {json_err}. Triggering safety alert anyway.")
            # Trigger alert as a defensive safety fallback
            alert_thread = threading.Thread(target=trigger_hardware_alert, args=(2.0,))
            alert_thread.daemon = True
            alert_thread.start()
            
    except Exception as e:
        print(f"[WARN] Error in on_message processing loop: {e}")


def on_disconnect(client, userdata, flags, reason_code, properties):
    print(f"[WARN] MQTT connection dropped (Reason: {reason_code}). Reconnection scheduled automatically...")


# In-Memory RAM Buffer & Voice Activity Detection (VAD) Pipeline
class AudioBufferPipeline:
    def __init__(self, publish_callback):
        self.publish_callback = publish_callback
        
        # Initialize WebRTC VAD if available
        self.vad = None
        if webrtcvad is not None:
            try:
                self.vad = webrtcvad.Vad(VAD_MODE)
                print(f"[SUCCESS] WebRTC VAD initialized in Mode {VAD_MODE}")
            except Exception as e:
                print(f"[WARN] Failed to instantiate WebRTC VAD: {e}. Using RMS fallback.")
        else:
            print("[INFO] WebRTC VAD module not installed. Defaulting to RMS energy thresholding VAD.")

        # Circular buffer for pre-speech capture to capture start transients
        self.pre_trigger_buffer = collections.deque(maxlen=PRE_TRIGGER_FRAMES)
        
        # Active streaming state machine variables
        self.is_recording = False
        self.recording_buffer = []
        self.silence_frames = 0
        
        self.max_silence_frames = int(MAX_SILENCE_MS / FRAME_DURATION_MS)
        self.max_recording_frames = int(MAX_RECORDING_MS / FRAME_DURATION_MS)
        self.min_recording_frames = int(MIN_RECORDING_MS / FRAME_DURATION_MS)

    def process_frame(self, frame_bytes):
        """
        Processes a single 30ms PCM frame. Integrates VAD filtering and RAM buffering.
        """
        # Determine if speech is present in this window
        is_speech = False
        if self.vad is not None:
            try:
                is_speech = self.vad.is_speech(frame_bytes, SAMPLE_RATE)
            except Exception:
                # WebRTC VAD raises errors on invalid sizes. Defensively fallback to RMS
                is_speech = self._energy_is_speech(frame_bytes)
        else:
            is_speech = self._energy_is_speech(frame_bytes)

        if not self.is_recording:
            # We are currently in silence. Fill the circular pre-trigger sliding window.
            self.pre_trigger_buffer.append(frame_bytes)
            if is_speech:
                # Transition to recording: pull pre-trigger frames to avoid truncating speech start
                self.is_recording = True
                self.recording_buffer = list(self.pre_trigger_buffer)
                self.silence_frames = 0
                print("\n[VAD] ACTIVE SOUND DETECTED. Activating RAM record buffer...")
        else:
            # We are actively recording a speech segment
            self.recording_buffer.append(frame_bytes)
            
            # Feed pre-trigger buffer continuously to keep history valid
            self.pre_trigger_buffer.append(frame_bytes)

            if is_speech:
                self.silence_frames = 0
            else:
                self.silence_frames += 1

            # Decision metrics for stopping/chunking the window
            reached_max = len(self.recording_buffer) >= self.max_recording_frames
            reached_silence = self.silence_frames >= self.max_silence_frames

            if reached_max or reached_silence:
                # Validate minimum length before formatting and transmitting
                if len(self.recording_buffer) >= self.min_recording_frames:
                    self._package_and_publish()
                else:
                    print("[WARN] Active window dismissed (too short to verify).")
                
                # Revert state machine back to scanning
                self.is_recording = False
                self.recording_buffer = []
                self.silence_frames = 0

    def _energy_is_speech(self, frame_bytes, threshold=350):
        """
        Fallback RMS Amplitude VAD algorithm if WebRTC VAD fails or is not compiled.
        """
        count = len(frame_bytes) // 2
        if count == 0:
            return False
        shorts = struct.unpack(f"<{count}h", frame_bytes)
        sum_squares = sum(x * x for x in shorts)
        rms = (sum_squares / count) ** 0.5
        return rms > threshold

    def _package_and_publish(self):
        """
        Fulfill Zero Disk I/O rule: Compile raw frames straight into an in-memory
        WAV buffer via BytesIO and publish over the network.
        """
        raw_pcm = b"".join(self.recording_buffer)
        duration = len(raw_pcm) / (SAMPLE_RATE * SAMPLE_WIDTH)
        print(f"[INFO] Packaging {duration:.2f}s active audio segment entirely in RAM...")
        
        try:
            # Create a WAV file completely in virtual RAM space
            wav_io = io.BytesIO()
            with wave.open(wav_io, "wb") as wav_file:
                wav_file.setnchannels(CHANNELS)
                wav_file.setsampwidth(SAMPLE_WIDTH)
                wav_file.setframerate(SAMPLE_RATE)
                wav_file.writeframes(raw_pcm)
            
            wav_bytes = wav_io.getvalue()
            wav_io.close()
            
            # Send to network callback
            self.publish_callback(wav_bytes)
        except Exception as e:
            print(f"[ERROR] RAM serialization error: {e}")


# Real-time Background Hardware/Mock Capture Thread
class AudioCaptureThread(threading.Thread):
    def __init__(self, pipeline, device_index=None):
        super().__init__()
        self.pipeline = pipeline
        self.device_index = device_index
        self.daemon = True
        self.running = False
        self.pa = None
        self.stream = None

    def run(self):
        self.running = True
        
        # Safe initialization of PyAudio
        pyaudio_available = pyaudio is not None
        if not pyaudio_available:
            print("[INFO] PyAudio module not imported. Initiating developer mock playback loop.")

        if pyaudio_available:
            try:
                self.pa = pyaudio.PyAudio()
            except Exception as e:
                print(f"[WARN] PyAudio failed to load host audio subsystems: {e}. Using mock engine.")
                pyaudio_available = False

        # Attempt to open real microphone hardware
        if pyaudio_available:
            try:
                self.stream = self.pa.open(
                    format=pyaudio.paInt16,
                    channels=CHANNELS,
                    rate=SAMPLE_RATE,
                    input=True,
                    input_device_index=self.device_index,
                    frames_per_buffer=CHUNK_SIZE
                )
                print(f"[INFO] PyAudio hardware interface bound to input device index: {self.device_index or 'Default'}")
            except Exception as e:
                print(f"[WARN] Failed to bind input device index {self.device_index}: {e}. Fallback to mock.")
                pyaudio_available = False
                if self.pa:
                    self.pa.terminate()

        # Branch execution loops based on device availability
        if pyaudio_available and self.stream:
            self._run_hardware_loop()
        else:
            self._run_mock_loop()

    def _run_hardware_loop(self):
        print("[INFO] Real-time hardware stream capture thread actively running.")
        while self.running:
            try:
                # Capture a single 30ms frame from the hardware microphone
                frame_bytes = self.stream.read(CHUNK_SIZE, exception_on_overflow=False)
                if frame_bytes:
                    self.pipeline.process_frame(frame_bytes)
            except Exception as e:
                print(f"[WARN] PyAudio device read exception: {e}")
                time.sleep(0.01)
        
        # Secure cleanup on closure
        try:
            self.stream.stop_stream()
            self.stream.close()
            self.pa.terminate()
            print("[INFO] PyAudio stream cleaned up successfully.")
        except Exception:
            pass

    def _run_mock_loop(self):
        """
        Cross-platform developer stream simulator. 
        Reads from test.wav if present to mimic real microphone data in real-time,
        otherwise synthesizes sound waves.
        """
        test_wav_path = os.path.join(os.path.dirname(__file__), "test.wav")
        
        if os.path.exists(test_wav_path):
            print(f"[INFO] Found developer test audio at: {test_wav_path}")
            print("[INFO] Streaming from file in real-time to test VAD and MQTT loops...")
            try:
                with wave.open(test_wav_path, "rb") as wf:
                    # Stream chunks continuously
                    while self.running:
                        frame_bytes = wf.readframes(CHUNK_SIZE)
                        if not frame_bytes:
                            # Loop audio file infinitely
                            wf.setpos(0)
                            frame_bytes = wf.readframes(CHUNK_SIZE)
                        
                        self.pipeline.process_frame(frame_bytes)
                        # Throttle loop to match 30ms real-time chunk speed
                        time.sleep(FRAME_DURATION_MS / 1000.0)
                return
            except Exception as e:
                print(f"[WARN] Error streaming mock test.wav: {e}. Falling back to synthetic tone synthesis.")
        
        # Synthetic Soundwave loop (silence interspersed with distress screams)
        print("[INFO] Initiating synthetic tone synthesizer...")
        phase = 0.0
        cycle_counter = 0
        while self.running:
            # Generate simulated scream blocks (3 seconds of tone, then 12 seconds of silence)
            time_sec = (cycle_counter * FRAME_DURATION_MS) / 1000.0
            cycle_counter = (cycle_counter + 1) % 500  # Reset pattern every 15s

            is_screaming = (4.0 <= (time_sec % 15.0) <= 7.0)
            
            samples = []
            for _ in range(CHUNK_SIZE):
                if is_screaming:
                    # Synthesize an organic high-pitched squeal: 800Hz base modulated with harmonic
                    val = 0.7 * math.sin(phase) + 0.3 * math.sin(2.0 * phase)
                    phase += 2.0 * math.pi * 800.0 / SAMPLE_RATE
                    samples.append(int(val * 24000))
                else:
                    # Synthetic ambient system hiss
                    samples.append(int(0.005 * math.sin(phase) * 100))
                    phase += 2.0 * math.pi * 50.0 / SAMPLE_RATE
            
            frame_bytes = struct.pack(f"<{CHUNK_SIZE}h", *samples)
            self.pipeline.process_frame(frame_bytes)
            time.sleep(FRAME_DURATION_MS / 1000.0)

    def stop(self):
        self.running = False


# MQTT Thread Controller & Global Initialization
client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect

def publish_audio_bytes(wav_bytes):
    """
    Defensive network boundary wrapper to publish data using QoS 0 (At most once).
    """
    try:
        client.publish(AUDIO_TOPIC, payload=wav_bytes, qos=0)
        print(f"[MQTT] Sent WAV package payload via MQTT on {AUDIO_TOPIC} (QoS 0)")
    except Exception as e:
        print(f"[WARN] MQTT publisher encounter error: {e}")


def main():
    print("=========================================================")
    print("     [INFO] AI Lifeguard Assistant - Edge Audio Node     ")
    print("=========================================================")
    
    # Establish MQTT connection
    try:
        print(f"[INFO] Attempting MQTT connection to broker: {MQTT_BROKER}:1883...")
        client.connect(host=MQTT_BROKER, port=1883, keepalive=60)
        client.loop_start()  # Launch background thread handling MQTT sockets
    except Exception as e:
        print(f"[WARN] Failed to connect to broker ({e}). Retrying will proceed asynchronously in background thread.")
        try:
            client.connect_async(host=MQTT_BROKER, port=1883, keepalive=60)
            client.loop_start()
        except Exception:
            pass

    # Create the Processing VAD Buffer Pipeline
    pipeline = AudioBufferPipeline(publish_callback=publish_audio_bytes)
    
    # Launch continuous background acquisition thread
    capture_thread = AudioCaptureThread(pipeline, device_index=MICROPHONE_INDEX)
    capture_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[INFO] Keyboard interrupt detected. Disconnecting edge node safely...")
    finally:
        capture_thread.stop()
        capture_thread.join(timeout=2.0)
        client.loop_stop()
        client.disconnect()
        
        # Safe GPIO release on close
        if GPIO_AVAILABLE:
            try:
                GPIO.cleanup()
                print("[INFO] GPIO channels released successfully.")
            except Exception:
                pass
        print("[INFO] Edge audio system shut down completed gracefully.")


if __name__ == "__main__":
    main()