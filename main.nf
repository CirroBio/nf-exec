#!/usr/bin/env nextflow

nextflow.enable.dsl = 2

process EXEC {
    container params.container
    cpus     params.cpus
    memory   "${params.memory_gb}.GB"
    time     "${params.timeout_hours}.h"

    publishDir params.output_dir, mode: 'copy', overwrite: true,
               saveAs: { it.replaceFirst('^output/', '') }

    // On Batch the inputs are downloaded, but a shared-filesystem executor would
    // symlink them, and then a command as ordinary as `find inputs -type f` finds
    // nothing. An arbitrary command must not see a different tree by executor.
    stageInMode 'copy'

    input:
        path staged, stageAs: 'staged/d*'
        path 'inputs.txt'
        path 'command.sh'

    // hidden: true is required. A path output glob silently drops dotfiles, so
    // without it the whole provenance record is published as nothing at all.
    output:
        path 'output/**', hidden: true, optional: true
        path 'output/.exitcode', hidden: true, emit: status

    script:
    def n = staged instanceof List ? staged.size() : 1
    """
    mkdir -p inputs output
    cp command.sh output/.command.sh

    # Nextflow names a single staged item 'd' and several 'd1'..'dN', so the layout
    # would differ between a one-dataset and a two-dataset run. Normalise to
    # inputs/1..N, and pair each with its source in the same order.
    if [ ${n} -eq 1 ]; then
        mv staged/d inputs/1
    else
        for i in \$(seq 1 ${n}); do mv "staged/d\$i" "inputs/\$i"; done
    fi
    awk '{ print NR "\\t" \$0 }' inputs.txt > output/.inputs.tsv

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
    ( set -euo pipefail; bash ./command.sh ) 2>&1 | tee output/.command.log
    rc=\${PIPESTATUS[0]}
    set -e

    echo "\$rc" > output/.exitcode
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

    // A list from preprocess.py or from an API caller; a comma-joined string when the
    // workflow is driven directly, which is what the form widget submits before
    // preprocess.py normalises it.
    def selected = params.input_datasets instanceof List
        ? params.input_datasets
        : params.input_datasets.toString().tokenize(',')

    def uris = selected
        .collect { it.toString().trim() }
        .findAll { it }
        .collect { it.contains('/') ? it : "${datasets_root}/${it}" }

    // Nextflow does the transfer, so a failed download is a Nextflow error with its
    // retries and reporting rather than shell of ours, and the task container needs
    // nothing of its own.
    staged_ch = Channel.of(uris.collect { file("${it}/data", type: 'dir') })

    // One dataset per line, in the same order, so the task can pair line N with the
    // folder it became. Emitted as a single string so collectFile has nothing to
    // interleave or re-sort.
    inputs_ch = Channel
        .of(uris.join('\n'))
        .collectFile(name: 'inputs.txt', newLine: true)

    EXEC(staged_ch, inputs_ch, command_ch)

    EXEC.out.status
        .map { it.text.trim() as Integer }
        .filter { it != 0 }
        .subscribe { rc ->
            error("The command exited with status ${rc}. See .command.log in the output dataset.")
        }
}
