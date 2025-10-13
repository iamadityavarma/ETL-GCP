flowchart LR
  subgraph Internet
    EXTAPI[External API]
  end

  subgraph GCP VPC (us-central1)
    direction LR
    NAT[Cloud NAT]
    SQL[(Cloud SQL Postgres<br/>Private IP)]
    BQ[(BigQuery)]

    subgraph Serverless
      VPCConn[Serverless VPC Access Connector]
      EXJ[Cloud Run Job<br/>data-extractor]
      LOJ[Cloud Run Job<br/>data-loader]
      CMP[Cloud Composer (Airflow)]
    end
  end

  CMP -->|2:00 AM trigger| EXJ
  EXJ -. uses .-> VPCConn
  LOJ -. uses .-> VPCConn

  EXJ -->|private IP| SQL
  LOJ -->|private IP| SQL
  LOJ -->|load/merge| BQ

  EXJ -->|outbound internet via VPCConn → NAT| EXTAPI
