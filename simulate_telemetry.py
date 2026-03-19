import json
import time
import uuid
import random
import os
from datetime import datetime

LANDING_ZONE = "landing_zone"

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def generate_telemetry():
    """Simulates realistic mechatronics telemetry data."""
    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "device_id": f"sensor-{random.randint(100, 199)}",
        "temperature": round(random.uniform(20.0, 85.0), 2),
        "vibration_freq": round(random.uniform(10.0, 500.0), 2),
        "status": random.choice(["OK", "WARNING", "ERROR"]) if random.random() > 0.05 else None # Simulate occasional missing status
    }

def main():
    ensure_dir(LANDING_ZONE)
    print(f"Starting telemetry simulator. Writing to ./{LANDING_ZONE}...")
    
    try:
        while True:
            data = generate_telemetry()
            filename = f"telemetry_{int(time.time()*1000)}.json"
            filepath = os.path.join(LANDING_ZONE, filename)
            
            with open(filepath, "w") as f:
                json.dump(data, f)
            
            print(f"Generated {filename}")
            time.sleep(1) # Simulate real-time streaming
    except KeyboardInterrupt:
        print("\nSimulation stopped.")

if __name__ == "__main__":
    main()
