#!/usr/bin/env nextflow

nextflow.enable.dsl = 2

// Cirro mounts the host's AWS CLI into every task container at the path it sets as
// aws.batch.cliPath, but does not put it on PATH. Read the configured value rather
// than hard-coding it; fall back to PATH for a local-agent environment, which has
// no such mount.
aws_cli = session.config.navigate('aws.batch.cliPath') ?: 'aws'

process EXEC {
    container params.container
    cpus     params.cpus
    memory   "${params.memory_gb}.GB"
    time     "${params.timeout_hours}.h"

    publishDir params.output_dir, mode: 'copy', overwrite: true,
               saveAs: { it.replaceFirst('^output/', '') }

    input:
        path 'inputs.txt'
        path 'command.sh'

    output:
        path 'output/**', optional: true
        path 'output/_cirro/exit_code.txt', emit: status

    script:
    """
    mkdir -p inputs output/_cirro
    cp command.sh output/_cirro/command.sh
    : > output/_cirro/inputs.tsv

    # Staging runs before the block that swallows the exit status, so a failed
    # transfer fails the task rather than handing the command an empty folder.
    # The folder number is the line number, and inputs.tsv is the only record of
    # which dataset landed where.
    i=0
    while read -r uri; do
        [ -n "\$uri" ] || continue
        i=\$((i + 1))
        mkdir -p "inputs/\$i"
        "${aws_cli}" s3 cp --recursive --quiet "\$uri/data" "inputs/\$i/"
        printf '%s\\t%s\\n' "\$i" "\$uri" >> output/_cirro/inputs.tsv
    done < inputs.txt

    # conda packages embed a fixed-length prefix placeholder, and a Nextflow work
    # directory is far too deep to fit inside it. Without this, 'pixi exec' fails
    # with "target prefix cannot be longer than the placeholder prefix".
    export PIXI_CACHE_DIR="\${PIXI_CACHE_DIR:-/tmp/px}"

    export CIRRO_INPUTS="\$PWD/inputs"
    export CIRRO_OUTPUT="\$PWD/output"
    export CIRRO_OUTPUT_S3="${params.output_dir}"
    export CIRRO_CPUS="${task.cpus}"
    export CIRRO_MEMORY_GB="${params.memory_gb}"

    # The task exits 0 whatever the command did, so publishDir still copies the log
    # and any partial output. The workflow raises the failure afterwards.
    set +e
    ( set -euo pipefail; bash ./command.sh ) 2>&1 | tee output/_cirro/command.log
    rc=\${PIPESTATUS[0]}
    set -e

    echo "\$rc" > output/_cirro/exit_code.txt
    """
}

workflow {

    if( !params.command )
        error("No command was provided.")

    if( !params.input_datasets )
        error("No input datasets were selected.")

    command_ch = Channel.of(params.command).collectFile(name: 'command.sh')

    // Cirro's dataset picker stores a full S3 root, but the read API an assistant has
    // exposes only dataset IDs, so a bare ID has to work too. Every dataset in a project
    // shares the output's bucket, so the prefix is recoverable from output_dir. A token
    // with no separator is an ID; anything else is already a path.
    def datasets_root = params.output_dir.toString().replaceFirst('/datasets/.*$', '') + '/datasets'
    def uris = params.input_datasets.toString().tokenize(',')
        .collect { it.contains('/') ? it : "${datasets_root}/${it}" }

    // One dataset per line, in the order the form recorded them. Emitted as a single
    // string so collectFile has nothing to interleave or re-sort.
    inputs_ch = Channel
        .of(uris.join('\n'))
        .collectFile(name: 'inputs.txt', newLine: true)

    EXEC(inputs_ch, command_ch)

    EXEC.out.status
        .map { it.text.trim() as Integer }
        .filter { it != 0 }
        .subscribe { rc ->
            error("The command exited with status ${rc}. See _cirro/command.log in the output dataset.")
        }
}
