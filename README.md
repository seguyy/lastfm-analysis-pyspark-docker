# Last.fm Session Analysis with PySpark and Docker

## Overview

This project analyzes the Last.fm 1K listening history dataset using PySpark and Docker.

The goal is to answer the following question:

> What are the top 10 songs played in the top 50 longest user sessions by track count?

A listening session is defined as consecutive songs played by the same user where the time difference between two songs is not greater than 20 minutes.

The project was implemented using PySpark and containerized with Docker to provide a reproducible execution environment.

---

## Dataset

Dataset source: [Last.fm 1K Dataset](http://ocelma.net/MusicRecommendationDataset/lastfm-1K.html)

Dataset attribution: This dataset requires referencing the official Last.fm webpage: [Last.fm](https://www.last.fm/)

Main file used:

```text
userid-timestamp-artid-artname-traid-traname.tsv
```

Dataset size:

```text
~2.5 GB
~17.5 million listening events
```

The user profile dataset was not used because the task only requires listening event data.

The dataset is not included in this repository because of its size.

You can put the dataset locally inside:

```text
lastfm/userid-timestamp-artid-artname-traid-traname.tsv
```

---

## Project Structure

```text
lastfm-session-analysis-pyspark/
├── README.md
├── Dockerfile
├── docker-compose.yml
├── .gitignore
├── src/
│   └── main_rdd.py
├── output/
│   └── top_10_songs.tsv
├── lastfm/
└── lastfm_split/
```

Note: lastfm/ and lastfm_split/ are local-only folders and are not committed to the repository.

---

## Architecture

```text
Raw Last.fm TSV
        ↓
Split raw input into smaller files using Linux commands
        ↓
Dockerized PySpark job
        ↓
Read split files with Spark RDD
        ↓
Parse and filter listening events
        ↓
Group events by user
        ↓
Sort each user's events by timestamp
        ↓
Create sessions using the 20-minute rule
        ↓
Select top 50 sessions by track count
        ↓
Count songs within those sessions
        ↓
Write final TSV output
```

---

## Technologies Used

- Docker
- Docker Compose
- PySpark
- Python
- Spark RDD API
- Linux commands: `mkdir`, `split`, `cat`, `ls`

---

## Implementation Details

The final solution is implemented in:

```text
src/main_rdd.py
```

The script performs the following steps:

1. Reads split TSV files using `sc.textFile`
2. Parses each row into:
   - user ID
   - timestamp
   - artist name
   - track name
3. Skips invalid or malformed rows
4. Groups listening events by user using `groupByKey`
5. Sorts each user's listening events by timestamp
6. Builds user sessions using a 20-minute inactivity threshold
7. Counts the number of tracks in each session
8. Selects the top 50 longest sessions using `takeOrdered`
9. Counts songs only inside those top 50 sessions using Python `Counter`
10. Writes the top 10 songs to a TSV file

This implementation avoids the memory-heavy DataFrame window execution plan and avoids joining the full listening history back to the top sessions.

---

## Session Logic

For each user:

1. Events are sorted by timestamp
2. The first event starts a new session
3. If the gap between two consecutive events is greater than 20 minutes, a new session starts
4. Otherwise, the event belongs to the current session
5. The session length is measured by number of tracks
6. The top 50 sessions are selected by track count
7. Songs inside those sessions are counted

---

## Optimization Strategy

The raw TSV dataset is large and memory-intensive to process locally.

The following optimizations were used:

- The raw TSV file was split into smaller files before Spark processing
- Spark was run inside Docker with conservative memory and CPU settings
- `sc.textFile(..., minPartitions=256)` was used to increase input partitions
- `groupByKey(numPartitions=64)` was used to control shuffle partitioning
- The implementation uses the Spark RDD API to avoid a memory-heavy DataFrame window plan
- Large `collect()` operations on the full dataset were avoided
- Only the top 50 sessions were returned to the driver
- Final song counting was performed only on those top 50 sessions
- The final result was written directly to disk

---

## How to Run

### 1. Place the dataset

Place the Last.fm listening history file at:

```text
lastfm/userid-timestamp-artid-artname-traid-traname.tsv
```

### 2. Split the input file

Create smaller chunks from the raw TSV file:

```bash
mkdir -p lastfm_split

split -l 500000 \
lastfm/userid-timestamp-artid-artname-traid-traname.tsv \
lastfm_split/part_
```

This creates smaller files inside:

```text
lastfm_split/
```

### 3. Run with Docker Compose

```bash
docker compose up --build
```

The Docker Compose configuration mounts:

```text
./src          → /opt/spark-apps
./lastfm_split → /opt/spark-data
./output       → /opt/spark-output
```

The Spark job reads from:

```text
/opt/spark-data
```

and writes the final result to:

```text
/opt/spark-output/top_10_songs.tsv
```

---

## Output

The final result is written to:

```text
output/top_10_songs.tsv
```

Format:

```text
track_name	artist_name	play_count
```

Example result:

```text
Love Lockdown	Kanye West	1473
Heartless	Kanye West	1470
See You In My Nightmares	Kanye West	1470
Say You Will	Kanye West	1466
Pinocchio Story (Freestyle Live From Singapore)	Kanye West	1466
Paranoid (Feat. Mr. Hudson)	Kanye West	1464
Welcome To Heartbreak (Feat. Kid Cudi)	Kanye West	1463
Amazing (Feat. Young Jeezy)	Kanye West	1462
Coldest Winter	Kanye West	1461
Bad News	Kanye West	1448
```

---

## Assumptions

- Sessions are calculated separately for each user
- Events are processed in timestamp order
- A new session starts when the gap between two plays exceeds 20 minutes
- Longest session means highest number of tracks, not longest duration in minutes
- Songs are identified by `track_name` and `artist_name`
- Invalid or malformed rows are skipped during parsing

---

## Resource Notes

The full dataset requires substantial memory because sessionization requires grouping and ordering millions of listening events by user.

The project was successfully executed locally using Docker on an 8 GB machine by:

- splitting the input file into smaller chunks
- using conservative Docker resource settings
- running Spark with one CPU
- using an RDD-based implementation

---

## Result

The generated result file:

```text
output/top_10_songs.tsv
```

contains the top 10 songs played in the top 50 longest sessions.

A successful Docker execution produced:

```text
Job 0 finished
Output written to: /opt/spark-output/top_10_songs.tsv
```
