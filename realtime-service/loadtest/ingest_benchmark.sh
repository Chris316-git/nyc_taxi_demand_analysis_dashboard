#!/usr/bin/env bash
# Measures end-to-end ingestion throughput: replays N trips at full speed and times how long
# the consumer takes to apply them (Kafka -> dedup -> Postgres upsert).
#   ./loadtest/ingest_benchmark.sh data/trips_2025_01.csv.gz 1000000
set -euo pipefail
FILE=${1:?prepared trips csv.gz}
LIMIT=${2:-1000000}
API=${API:-http://localhost:8080}
JAR=${JAR:-target/taxi-demand-service-1.0.0.jar}

# Events consumed so far = counted + skipped as duplicates + rejected as late.
applied() { curl -s "$API/api/v1/stats" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["tripsApplied"]+d["duplicatesSkipped"]+d["lateRejected"])'; }

before=$(applied)
start=$(date +%s.%N)
SPRING_PROFILES_ACTIVE=replayer REPLAY_FILE="$FILE" REPLAY_SPEEDUP=0 REPLAY_LIMIT="$LIMIT" \
  java -jar "$JAR" > /tmp/replayer.log 2>&1 &
target=$((before + LIMIT))
while [ "$(applied)" -lt "$target" ]; do sleep 0.5; done
end=$(date +%s.%N)
python3 -c "d=$end-$start; print(f'applied {$LIMIT:,} trips in {d:.1f}s -> {$LIMIT/d:,.0f} trips/s end to end')"
