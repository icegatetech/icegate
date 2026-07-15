# Per-tenant task model
Data flow: Client -> Ingestor -> WAL -> Shifter (multiple tasks) -> Iceberg.

## Ingestor's tasks
- Minimize the number of requests to S3. Moreover, the priority is to minimize file write requests (in AWS, writing is 10 times more expensive than reading).
- Do not delay the response to the client and guarantee data recording (respond only after WAL recording).

## Core rules
- each Ingest request is appended to a single WAL file.
- Shifter creates one task per tenant (partition).
- Each task produces one Parquet file for its tenant (partition).
- A task reads only WAL files that contain data for its tenant (partition).

## Read/write example
- wrote 3 WAL files
- read WAL files 7 times (fan-out to multiple tenants)
- wrote 3 Parquet files

```mermaid
flowchart LR

subgraph Request_3
  e3_1[tenant3] --> wal3[wal3];
  e3_2[tenant1] --> wal3;
  e3_3[tenant1] --> wal3;
  e3_4[tenant1] --> wal3;
end

subgraph Request_2
  e2_1[tenant1] --> wal2[wal2];
  e2_2[tenant2] --> wal2;
  e2_3[tenant1] --> wal2;
end

subgraph Request_1
  e1_1[tenant1] --> wal1[wal1];
  e1_2[tenant2] --> wal1;
  e1_3[tenant3] --> wal1;
  e1_4[tenant2] --> wal1;
end

subgraph Shifter
  wal1 --> t_tenant1[task1];
  wal2 --> t_tenant1;
  wal3 --> t_tenant1;

  wal1 --> t_tenant2[task2];
  wal2 --> t_tenant2;

  wal1 --> t_tenant3[task3];
  wal3 --> t_tenant3;

  t_tenant1 --> p_tenant1[parquet1 - tenant1];
  t_tenant2 --> p_tenant2[parquet2 - tenant2];
  t_tenant3 --> p_tenant3[parquet3 - tenant3];
end

```
