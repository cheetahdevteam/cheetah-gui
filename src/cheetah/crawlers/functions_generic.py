"""
Generic functions.

This module contains generic functions required by Cheetah GUI at different facilities.
"""
import subprocess
import pathlib
import psutil


def kill_slurm_job(name: str, full_proc_dir: pathlib.Path) -> str:
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


def kill_local_job(name: str, full_proc_dir: pathlib.Path) -> str:
    """
    Kill local job.

    This function kills an OM process by name calling 'kill -9 {pid}' command in a
    subprocess.

    Returns:

        Either an empty string if the process was killed successfully or the error
        message.
    """
    proc: psutil.Process
    for proc in psutil.process_iter():
        if (
            str(full_proc_dir / "monitor.yaml") in proc.cmdline()
            and "om_monitor.py" in proc.cmdline()
        ):
            try:
                proc.kill()
                return ""
            except psutil.NoSuchProcess:
                pass
    return f"Process {name} does not exist."
