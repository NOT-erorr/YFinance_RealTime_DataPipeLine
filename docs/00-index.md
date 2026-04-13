# Yahoo Finance Data Pipeline Docs

This folder contains operational and technical documents for the project.

## Document list

1. [01-architecture.md](01-architecture.md): System architecture and runtime flow.
2. [02-setup-and-run.md](02-setup-and-run.md): Local setup, Docker runbook, and validation commands.
3. [03-configuration-reference.md](03-configuration-reference.md): Environment variables and config behavior.
4. [04-operations-and-troubleshooting.md](04-operations-and-troubleshooting.md): Daily operations and common failure recovery.
5. [05-project-structure.md](05-project-structure.md): Current normalized project structure and responsibilities.

## Quick start

```bash
docker compose -f docker-compose.yaml up -d --build
```

Check ingestion:

```bash
docker compose -f docker-compose.yaml exec -T postgres psql -U admin -d kraf_db -c "SELECT COUNT(*) FROM stock_prices;"
```
