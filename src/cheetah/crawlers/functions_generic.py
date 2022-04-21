"""
Generic functions.

This module contains generic functions required by Cheetah GUI at different facilities.
"""
import subprocess


def kill_slurm_job(name: str) -> str:
    """
    Kill SLURM job.

    This function kills a SLURM job by name calling 'scancel -n {name}' command in a
    subprocess.

    Returns:

        Either an empty string if the job was killed successfully or the error message.
    """
    command: str = f"scancel -n {name}"
    return subprocess.run(command, shell=True, capture_output=True).stderr.decode(
        "utf-8"
    )
