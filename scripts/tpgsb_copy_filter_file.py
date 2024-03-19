#!/usr/bin/env python
import click
import h5py
from rich import print

# tr_list = [2892, 2900]
# src_path = '/nfs/rscratch/thea/np02vdcoldbox_raw_run023740_0030_dataflow0_datawriter_0_20240112T164839.hdf5'
# dest_path = 'zzz.hdf5'

@click.command()
@click.argument('src_path',type=click.Path(file_okay=True, dir_okay=False, exists=True))
@click.argument('dest_path',type=click.Path(file_okay=True, dir_okay=False, exists=False))
@click.option('-k','--keep', type=int, multiple=True)
def main(src_path, dest_path, keep):
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
    main()