#!/usr/bin/env python


import logging
import pandas as pd
# import hdf5libs
import click

import tpgsandbox.utils.unpacker as unpacker
import tpgsandbox.utils.assembler as assembler
import tpgsandbox.utils.reader as recordreader

from rich import print
from rich.logging import RichHandler


CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])
@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('rawfile', type=click.Path(exists=True))
@click.option('-i', '--interactive', is_flag=True, default=False, help="Start IPython")
@click.option('-p', '--plot', type=str, default=None, help="Generate example ADC plots")
@click.option('-o', '--tr-offset', type=int, default=0, help="Offset of the first Trigger Record to process")
@click.option('-n', '--num-trs', type=int, default=1, help="Number of trigger records to process")
def cli(rawfile, interactive, plot, tr_offset, num_trs):
    """
    This is an example script to demonstrate the usage of the Record Reader utility to 
    demonstrate how to unpack, pre-process DUNE raw data for a selection of fragments


    \b
    rawfile : Path of the original raw data file

    The script demonstrates how to 
    - Create a record reader object 
    - Define unpacking porducts and how to pre-process (assemble)
    - Plot raw adc channels vs time maps as PDG files
    - Optionally: start an IPython shell to interactlvely play with the unpacket dataframes
    """

    rr = recordreader.RecordReader()
    rr.add_file(rawfile)


    rr.add_product('bde_eth', unpacker.WIBEthFragmentPandasUnpacker(), assembler.ADCJoiner())
    rr.add_product('tp', unpacker.TPFragmentPandasUnpacker(), assembler.TPConcatenator())

    trs = [ i for i in rr.iter_records()]
    # # process only the first TR
    trs = trs[tr_offset:tr_offset+num_trs]

    for run,tr in trs:
        print(f"--- Reading Trigger Record {run}:{tr} ---")

        data = rr.load_record(run,tr)
        
        df_tp = data.record['tp']
        df_tpc = data.record['bde_eth']


        if plot and not df_tpc.empty:
            import matplotlib.pyplot as plt

            print("Plotting all samples")

            fig, ax = plt.subplots(figsize=(10,8))
            pcm = ax.pcolormesh(df_tpc)
            fig.colorbar(pcm)
            ax.set_xticks(xpos, [df_tpc.columns[i] for i in xpos])
            ax.set_xlabel("channel id")
            ax.set_ylabel("Samples (since start of RO window)")
            # pdf.savefig()
            fig.savefig(f'wibeth_frame_{tr}_{plot}.png')


            print("Plotting all samples (baseline subtracted)")
            df_tpc_sub = df_tpc-df_tpc.mean()
            fig, ax = plt.subplots(figsize=(10,8))
            pcm = ax.pcolormesh(df_tpc_sub)
            fig.colorbar(pcm)
            ax.set_xticks(xpos, [df_tpc_sub.columns[i] for i in xpos])
            ax.set_xlabel("channel id")
            ax.set_ylabel("Samples (since start of RO window)")
            # pdf.savefig()
            fig.savefig(f'wibeth_frame_{tr}_{plot}_sub.png')


            print("Plotting done")


    if interactive:
        import IPython
        IPython.embed(colors='neutral')

if __name__ == "__main__":
        
	FORMAT = "%(message)s"
	logging.basicConfig(
    	level="INFO",
        format=FORMAT,
        datefmt="[%X]",
        handlers=[RichHandler()]
	)

	cli()
