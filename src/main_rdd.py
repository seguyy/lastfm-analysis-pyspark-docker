from pyspark.sql import SparkSession
from datetime import datetime
from collections import Counter
import os

spark = (
    SparkSession.builder
    .appName("LastFM RDD Session Analysis")
    .master("local[1]")
    .getOrCreate()
)

sc = spark.sparkContext

input_path = "/opt/spark-data"
output_dir = "/opt/spark-output"
output_file = f"{output_dir}/top_10_songs.tsv"

os.makedirs(output_dir, exist_ok=True)

# this function is created to parse each row into corresponding columns in the dataset
def parse_line(line):
    parts = line.rstrip("\n").split("\t")
    if len(parts) != 6:
        return None

    user_id, timestamp, _artist_id, artist_name, _track_id, track_name = parts

    # skip the invalid rows
    if not user_id or not timestamp or not track_name:
        return None

    # convert timestamp into Unix seconds for easier time-gap comparison
    try:
        event_time = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
        event_ts = int(event_time.timestamp())
    except Exception:
        return None

    return user_id, (event_ts, artist_name, track_name)

# create a new session when the gap between consecutive plays is greater than 20 minutes
def build_sessions(user_events):
    user_id, events = user_events
    events = sorted(events, key=lambda x: x[0])
    session_num = 0
    prev_time = None
    current_tracks = []

    # count the number of tracks in each session
    for event_ts, artist_name, track_name in events:
        if prev_time is None or event_ts - prev_time > 20 * 60:
            if current_tracks:
                session_id = f"{user_id}_{session_num}"
                yield session_id, len(current_tracks), dict(Counter(current_tracks))

            session_num += 1
            current_tracks = []

        current_tracks.append((track_name, artist_name))
        prev_time = event_ts

    if current_tracks:
        session_id = f"{user_id}_{session_num}"
        yield session_id, len(current_tracks), dict(Counter(current_tracks))


lines = sc.textFile(input_path, minPartitions=256)

parsed = (
    lines
    .map(parse_line)
    .filter(lambda x: x is not None)
)

# group all listening events by user so sessions can be built per user
sessions = (
    parsed
    .groupByKey(numPartitions=64)
    .flatMap(build_sessions)
)

# select the top 50 longest sessions
top_50_sessions = sessions.takeOrdered(50, key=lambda x: -x[1])

song_counter = Counter()

# count the songs inside the top 50 sessions
for session_id, track_count, song_counts in top_50_sessions:
    song_counter.update(song_counts)

# get the top 10 songs
top_10_songs = song_counter.most_common(10)

# write them into a file
with open(output_file, "w", encoding="utf-8") as f:
    f.write("track_name\tartist_name\tplay_count\n")
    for (track_name, artist_name), play_count in top_10_songs:
        f.write(f"{track_name}\t{artist_name}\t{play_count}\n")

print("Top 10 songs:")
for (track_name, artist_name), play_count in top_10_songs:
    print(track_name, artist_name, play_count)

print(f"Output written to: {output_file}")

spark.stop()