from faker import Faker
from datetime import datetime, timedelta
import random
import json
import sys

if len(sys.argv) < 2:
    print("Usage: python data_generator.py [output_dir]")
    exit(0)

# output_dir = "dbfs:/Volumes/dev/default/events_stream/"
# output_dir = "/home/syssec/faker/data"
output_dir = sys.argv[1]
fake = Faker()

visit_values = ["true", "false"]
visit_w = [0.2, 0.8]

reaction_values = ["NA", "Thumbs up", "Heart", "Laughing", "Clapping Hands"]
reaction_w = [0.7, 0.1, 0.05, 0.03, 0.12]

current_time = datetime.utcnow()
batch_records = []
for _ in range(30):
    current_time += timedelta(seconds=random.randint(0,59))
    record = {
        "view_duration": round(random.random()*20, 2),
        "country": fake.country_code(),
        "timestamp": current_time.isoformat(),
        "visit_website": random.choices(visit_values, weights=visit_w, k=1)[0],
        "reaction": random.choices(reaction_values, weights=reaction_w, k=1)[0]
    }
    batch_records.append(record)

# print(json.dumps(batch_records))
current_time_string = datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')
filename = f"{output_dir}/event-{current_time_string}.json"
with open(filename, "w+") as f:
    json.dump(batch_records, f)
