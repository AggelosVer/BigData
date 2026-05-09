import time
import json
import argparse
from kafka import KafkaProducer
import numpy as np
from uxsim import World
import random

def convert(obj):
    if isinstance(obj, (np.integer,)): return int(obj)
    if isinstance(obj, (np.floating,)): return float(obj)
    if isinstance(obj, (np.ndarray,)): return obj.tolist()
    return obj

def simulate_traffic():
    seed = None

    W = World(
        name="",
        deltan=5,
        tmax=3600,  # 1 hour simulation
        print_mode=1, save_mode=0, show_mode=0,
        random_seed=seed,
        duo_update_time=600
    )
    random.seed(seed)

    signal_time = 20
    sf_1 = 1
    sf_2 = 2
    I1 = W.addNode("I1", 1, 0, signal=[signal_time * sf_1, signal_time * sf_2])
    I2 = W.addNode("I2", 2, 0, signal=[signal_time * sf_1, signal_time * sf_2])
    I3 = W.addNode("I3", 3, 0, signal=[signal_time * sf_1, signal_time * sf_2])
    I4 = W.addNode("I4", 4, 0, signal=[signal_time * sf_1, signal_time * sf_2])
    W1 = W.addNode("W1", 0, 0)
    E1 = W.addNode("E1", 5, 0)
    N1 = W.addNode("N1", 1, 1)
    N2 = W.addNode("N2", 2, 1)
    N3 = W.addNode("N3", 3, 1)
    N4 = W.addNode("N4", 4, 1)
    S1 = W.addNode("S1", 1, -1)
    S2 = W.addNode("S2", 2, -1)
    S3 = W.addNode("S3", 3, -1)
    S4 = W.addNode("S4", 4, -1)

    for n1, n2 in [[W1, I1], [I1, I2], [I2, I3], [I3, I4], [I4, E1]]:
        W.addLink(n2.name + n1.name, n2, n1, length=500, free_flow_speed=50, jam_density=0.2, number_of_lanes=3, signal_group=0)

    for n1, n2 in [[N1, I1], [I1, S1], [N3, I3], [I3, S3]]:
        W.addLink(n1.name + n2.name, n1, n2, length=500, free_flow_speed=30, jam_density=0.2, signal_group=1)

    for n1, n2 in [[N2, I2], [I2, S2], [N4, I4], [I4, S4]]:
        W.addLink(n2.name + n1.name, n2, n1, length=500, free_flow_speed=30, jam_density=0.2, signal_group=1)

    dt = 30
    demand = 2
    for t in range(0, 3600, dt):
        dem = random.uniform(0, demand)
        for n1, n2 in [[N1, S1], [S2, N2], [N3, S3], [S4, N4]]:
            W.adddemand(n1, n2, t, t + dt, dem * 0.25)
        for n1, n2 in [[E1, W1], [N1, W1], [S2, W1], [N3, W1], [S4, W1]]:
            W.adddemand(n1, n2, t, t + dt, dem * 0.75)
            
    return W

def kafka_producer_loop(W, N, topic="vehicle_positions"):
    # Βασικός χειρισμός σφαλμάτων για τη σύνδεση στον Kafka
    try:
        producer = KafkaProducer(
            bootstrap_servers="localhost:19092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5
        )
        print("Επιτυχής σύνδεση στον Kafka/Redpanda broker!")
    except Exception as e:
        print(f"Σφάλμα σύνδεσης στον Kafka broker: {e}")
        return

    print("Εκτέλεση προσομοίωσης UXSIM...")
    W.exec_simulation()
    
    # Εξαγωγή δεδομένων οχημάτων σε DataFrame
    df = W.analyzer.vehicles_to_pandas()
    
    # Χρονικά βήματα της προσομοίωσης
    time_col = 't'
    times = sorted(df[time_col].unique())

    print(f"Έναρξη αποστολής δεδομένων ανά {N} δευτερόλεπτα...")
    
    for t in times:
        snapshot = df[df[time_col] == t]
        
        # Φιλτράρισμα: Κρατάμε μόνο όσα οχήματα βρίσκονται σε κίνηση (speed > 0)
        # Στο output του UXSIM, η ταχύτητα είναι η στήλη 'v'
        moving_vehicles = snapshot[snapshot['v'] > 0]
        
        count = 0
        for _, row in moving_vehicles.iterrows():
            record = {k: convert(v) for k, v in row.to_dict().items()}
            # Αποστολή στο Redpanda
            producer.send(topic, record)
            count += 1
            
        producer.flush()
        print(f"Χρόνος προσομοίωσης t={t}: Στάλθηκαν {count} κινούμενα οχήματα στο topic '{topic}'")
        
        # Αναμονή N δευτερόλεπτα (παραμετροποιημένο)
        time.sleep(N)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UXSIM Kafka Producer")
    parser.add_argument("-n", "--interval", type=float, default=1.0, help="Διάστημα αποστολής δεδομένων σε δευτερόλεπτα (N)")
    parser.add_argument("-t", "--topic", type=str, default="vehicle_positions", help="Το Kafka topic")
    args = parser.parse_args()

    W = simulate_traffic()
    kafka_producer_loop(W, N=args.interval, topic=args.topic)
    print("Η διαδικασία ολοκληρώθηκε.")
