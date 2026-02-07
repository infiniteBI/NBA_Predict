
NBA Analytics Pipeline
Production-grade ETL system processing 1,200+ games per season

Technical Highlights:
- Retry logic with exponential backoff for API resilience
- Idempotent operations - safely re-runnable without duplicates
- Date-partitioned Parquet storage for optimized queries
- Graceful degradation - continues on errors, logs failures

Architecture:
└── Extract: NBA API (game stats, player stats, standings)
└── Transform: Pandas dataframes with schema validation
└── Load: AWS S3 (Parquet, Snappy compression)
└── Schedule: AWS EventBridge (daily 6 AM EST)

Features:
✓ Traditional & advanced stats (ORtg, DRtg, USG%, PACE)
✓ Optional shot chart data (heat maps, zone efficiency)
✓ Player-team history (trade detection)
✓ Daily incremental updates

Stack: Python, Pandas, Boto3, AWS S3, Parquet, EventBridge

Performance:
- 50% storage reduction vs JSON (Parquet compression)
- 80% fewer API calls (smart skip logic)
- Hive-style partitioning for sub-second Athena queries
