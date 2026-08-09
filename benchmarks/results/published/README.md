# Published benchmark results

The raw JSON behind every number quoted in the top-level README. Regenerate
with a running cluster:

```bash
make benchmark            # throughput: 8 MiB and 128 MiB objects, 8 clients
make benchmark-recovery   # kill-a-node recovery, 3 trials
```

Fresh results land in `benchmarks/results/`; the files here are the curated
set the README cites. Each file records the full environment (host, Docker
version, git commit, client location) alongside the measurements, so a number
can always be traced to the run that produced it.

Caveat that applies to every file: client, gateway, coordinator, PostgreSQL
and all five storage nodes ran on one laptop. These numbers measure the
software's behaviour, not a real network's.
