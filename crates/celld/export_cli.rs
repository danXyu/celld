// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! `celld export`: the change-export operator commands
//! (`docs/design/change-export.md#configuration`).

const HELP: &str = "celld export <reconcile | verify | erase> [flags]

Run `celld export <subcommand> --help` for each subcommand's flags.";

pub async fn run(arguments: Vec<String>) -> anyhow::Result<()> {
    let mut arguments = arguments.into_iter();
    let Some(command) = arguments.next() else {
        return crate::cli_output::Output::new(crate::cli_output::Format::Text).help(HELP);
    };
    match command.as_str() {
        "reconcile" | "verify" | "erase" => {
            crate::export_audit::cli::run(&command, arguments.collect()).await
        }
        "help" | "--help" | "-h" => {
            crate::cli_output::Output::new(crate::cli_output::Format::Text).help(HELP)
        }
        other => anyhow::bail!("unknown `celld export` subcommand: {other}\n\n{HELP}"),
    }
}
