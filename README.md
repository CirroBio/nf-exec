# nf-exec

Run a user-supplied shell command in a user-chosen container, against one or more input
datasets staged from S3.

Built for the Cirro **Run Command** process, where the command text and the container image
come from a form rather than from this repository. The point is to leave a durable record:
the command, its log and its exit status are published alongside the results.

## Contract

| | |
|---|---|
| Input | Each dataset in `--input_datasets` is staged into `inputs/<n>/`, numbered from 1 in the order given, directory structure preserved |
| Output | Anything written to `output/` is published to `--output_dir` |
| Shell | The command runs under `bash` with `set -euo pipefail` |
| Failure | A non-zero exit still publishes the log, then fails the workflow |

The container image **must have `bash` and `ps`**. Nextflow's task wrapper calls `ps` to
collect metrics, and without it the task fails before the command runs — which rules out
most Debian `-slim` images.

## Parameters

| Parameter | Description |
|---|---|
| `--command` | Shell commands to run. Written to a file and executed, so nothing needs escaping |
| `--input_datasets` | Comma-separated dataset roots. `/data` is appended to each, and each is fetched with `aws s3 cp --recursive` |
| `--output_dir` | Where `output/` is published |
| `--container` | Image to run in |
| `--cpus` | CPUs for the task |
| `--memory_gb` | Memory for the task, in GB |
| `--timeout_hours` | Task time limit |

## Published files

Besides whatever the command writes, every run produces:

| File | Contents |
|---|---|
| `_cirro/command.sh` | The exact bytes executed |
| `_cirro/command.log` | stdout and stderr, interleaved |
| `_cirro/exit_code.txt` | The command's exit status |
| `_cirro/inputs.tsv` | Folder number and source URI for each staged dataset |

`_cirro/inputs.tsv` is the only record of which dataset landed in which numbered folder.

## Variables set for the command

`CIRRO_INPUTS`, `CIRRO_OUTPUT`, `CIRRO_OUTPUT_S3`, `CIRRO_CPUS`, `CIRRO_MEMORY_GB`.

`PIXI_CACHE_DIR` defaults to `/tmp/px`. conda packages embed a fixed-length prefix
placeholder that a Nextflow work directory is too deep to fit inside, so `pixi exec` fails
without a short cache path.

## Running it

```bash
nextflow run CirroBio/nf-exec \
    -with-docker \
    --input_datasets 's3://bucket/datasets/<uuid-a>,s3://bucket/datasets/<uuid-b>' \
    --output_dir ./results \
    --container 'ghcr.io/prefix-dev/pixi:0.80.0' \
    --cpus 2 --memory_gb 8 --timeout_hours 12 \
    --command 'wc -l inputs/1/*.csv > output/counts.txt'
```

Staging uses the AWS CLI at `aws.batch.cliPath` when that is configured — Cirro mounts its
own CLI into every task container, so the image does not need one — and falls back to `aws`
on `PATH` otherwise.

Cirro supplies its own Nextflow config at run time via `-config`, setting the executor, work
directory, queue and job role, so this repository intentionally ships no `nextflow.config`.
