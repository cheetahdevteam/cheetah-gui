#!/usr/bin/env python

import click  # type: ignore
import h5py  # type: ignore
import psana  # type: ignore
import traceback

from typing import Any, Tuple, Optional

import numpy
from numpy.typing import NDArray


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))  # type: ignore
@click.option(  # type: ignore
    "--source", "-s", "source", type=str, required=True, help="psana source string"
)
@click.option(  # type: ignore
    "--detector", "-d", "detector", type=str, required=True, help="psana detector name"
)
@click.option(  # type: ignore
    "--output",
    "-o",
    "output",
    type=click.Path(dir_okay=False),
    required=True,
    help="output HDF5 file name",
)
@click.option(  # type: ignore
    "--calib",
    is_flag=True,
    show_default=True,
    default=False,
    help="on/off mask from calib directory",
)
@click.option(  # type: ignore
    "--status",
    is_flag=True,
    show_default=True,
    default=True,
    help="on/off mask generated from calib pixel_status",
)
@click.option(  # type: ignore
    "--edges",
    is_flag=True,
    show_default=True,
    default=True,
    help="on/off mask of edges",
)
@click.option(  # type: ignore
    "--central",
    is_flag=True,
    show_default=True,
    default=True,
    help="on/off mask of two central columns",
)
@click.option(  # type: ignore
    "--unbond",
    is_flag=True,
    show_default=True,
    default=False,
    help="on/off mask of unbonded pixels",
)
@click.option(  # type: ignore
    "--unbondnbrs",
    is_flag=True,
    show_default=True,
    default=False,
    help="on/off mask of unbonded pixel with four neighbors",
)
@click.option(  # type: ignore
    "--unbondnbrs8",
    is_flag=True,
    show_default=True,
    default=False,
    help="on/off mask of unbonded pixel with eight neighbors",
)
@click.option(  # type: ignore
    "--mode",
    type=click.Choice(["1", "2", "3"]),
    show_default=True,
    default="2",
    help="masks zero/four/eight neighbors around each bad pixel",
)
@click.option(  # type: ignore
    "--width",
    type=int,
    default=2,
    help="number of masked rows columns on each edge",
)
def main(
    source: str,
    detector: str,
    output: str,
    calib: bool,
    status: bool,
    edges: bool,
    central: bool,
    unbond: bool,
    unbondnbrs: bool,
    unbondnbrs8: bool,
    mode: int,
    width: int,
) -> None:
    """
    Extract mask from psana. This script extracts bad pixel mask from psana using
    psana.Detector.mask() function, converts the extracted mask to slab shape and saves
    it in the output HDF5 file (in the '/data/data' dataset).
    """
    try:
        ds: Any = psana.DataSource(source)
        det: Any = psana.Detector(detector)
        for evt in ds.events():
            break
        psana_mask: Optional[NDArray[numpy.int_]] = det.mask(
            evt,
            calib=calib,
            status=status,
            edges=edges,
            central=central,
            unbond=unbond,
            unbondnbrs=unbondnbrs,
            unbondnbrs8=unbondnbrs8,
            width=width,
            mode=int(mode),
        ).astype(numpy.int8)
    except Exception as e:
        print(traceback.format_exc())
        print(e)
        psana_mask = None

    if psana_mask is None:
        print("Couldn't extract mask from psana.")
        return

    shape: Tuple[int, ...] = psana_mask.shape
    if len(shape) == 2:
        slab_mask: NDArray[numpy.int_] = psana_mask
    elif len(shape) == 3:
        slab_mask = psana_mask.reshape(shape[0] * shape[1], shape[2])
    else:
        print(f"Couldn't convert mask with shape {shape} to slab.")
        return
    fh: Any
    with h5py.File(output, "w") as fh:
        print(f"Writing mask to {output}.")
        fh.create_dataset("/data/data", data=slab_mask)


if __name__ == "__main__":
    main()
