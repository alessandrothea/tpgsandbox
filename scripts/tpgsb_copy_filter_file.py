#!/usr/bin/env python
import click
import h5py
from rich import print

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])
@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('src_path',type=click.Path(file_okay=True, dir_okay=False, exists=True))
@click.argument('dest_path',type=click.Path(file_okay=True, dir_okay=False, exists=False))
@click.option('-k','--keep', type=int, multiple=True, help="List of trigger record ids to copy to the new file")
def cli(src_path, dest_path, keep):
    """
    Utility script to copy a subset of trigger records from a DUNE-DAQ raw-data file to another.


    \b
    SRC_PATH : Path of the original data file
    DEST_PATH : Path of the destination file
    """
    tr_list = [str(tr) for tr in keep]
    # return
    with h5py.File(src_path) as src, h5py.File(dest_path,'w') as dest:
        for a in src.attrs:
            dest.attrs[a] = src.attrs[a]

        for name, grp in src.items():
            if not any((tr in name) for tr in tr_list):
                continue
            dest.copy(grp,dest,name)
            print(f"Trigger record {name} copied")


if __name__ == '__main__':
    cli()