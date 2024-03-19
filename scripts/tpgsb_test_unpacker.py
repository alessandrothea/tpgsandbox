#!/usr/bin/env python


import logging
import pandas as pd
import hdf5libs
import click
import tpgsandbox.utils.rawdataunpacker as rdu
import tpgsandbox.utils.assembler as asm
from rich import print
from rich.logging import RichHandler
import detchannelmaps



openv_2_chmap = {
    'np02vdcoldbox': 'VDColdboxChannelMap',
    'np02hd': 'PD2HDChannelMap'
}


CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])
@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('rawfile', type=click.Path(exists=True))
@click.option('-i', '--interactive', is_flag=True, default=False)
@click.option('-p', '--plot', type=str, default=None)
@click.option('-o', '--tr-offset', type=int, default=0)
@click.option('-n', '--num-trs', type=int, default=1)
def main(rawfile, interactive, plot, tr_offset, num_trs):

    print(f"Opening {rawfile}")
    rdf = hdf5libs.HDF5RawDataFile(rawfile)

    op_env = rdf.get_attribute('operational_environment')
    print(op_env)

    chmap_id = openv_2_chmap.get(op_env, None)
    if chmap_id is None:
        print(f"[red]Error: channel map not found for operational environment {op_env}![/red]")
        return


    chmap = detchannelmaps.make_map(chmap_id)


    # Unpackers
    wethf_up = rdu.WIBEthFragmentPandasUnpacker(chmap)
    daphne_up = rdu.DAPHNEStreamFragmentPandasUnpacker()
    tp_up = rdu.TPFragmentPandasUnpacker(chmap)

    up = rdu.UnpackerService()

    up.add_unpacker('bde_eth', wethf_up)
    up.add_unpacker('tp', tp_up)
    up.add_unpacker('pds', daphne_up)

    weth_asb = asm.ADCJoiner('bde_eth')
    tp_asb = asm.TPConcatenator('tp')
    
    # Assembler
    asm_svc = asm.AssemblerService()
    asm_svc.add('bde_eth', weth_asb)
    asm_svc.add('tp', tp_asb)
        
    trs = [ i for i,_ in rdf.get_all_trigger_record_ids()]
    # process only the first TR
    trs = trs[tr_offset:tr_offset+num_trs]

    df_tp = None
    df_tpc = None
    for tr in trs:
        print(f"--- Reading Trigger Record {tr} ---")

        unpacked_tr = up.unpack(rdf, tr)

        dfs = asm_svc.assemble(unpacked_tr)
        
        df_tp = dfs['tp']
        df_tpc = dfs['bde_eth']


        if plot and not df_tpc.empty:
            import matplotlib.pyplot as plt

            # with PdfPages('multipage_pdf.pdf') as pdf:

            print("Plotting the first 128 samples")
            # xticks = df_tpc.columns[::len(df_tpc.columns)//10]
            xpos = list(range(len(df_tpc.columns)))[::len(df_tpc.columns)//10]

            fig, ax = plt.subplots(figsize=(10,8))
            pcm = ax.pcolormesh(df_tpc.iloc[:128])
            fig.colorbar(pcm)
            ax.set_xticks(xpos, [df_tpc.columns[i] for i in xpos])
            ax.set_xlabel("channel id")
            ax.set_ylabel("Samples (since start of RO window)")
            # pdf.savefig()
            fig.savefig(f'wibeth_frame_{tr}_{plot}_0-127ticks.png')
            # fig.savefig('wibeth_frame_0-127ticks.pdf')

            print("Plotting the all samples")

            fig, ax = plt.subplots(figsize=(10,8))
            pcm = ax.pcolormesh(df_tpc)
            fig.colorbar(pcm)
            ax.set_xticks(xpos, [df_tpc.columns[i] for i in xpos])
            ax.set_xlabel("channel id")
            ax.set_ylabel("Samples (since start of RO window)")
            # pdf.savefig()
            fig.savefig(f'wibeth_frame_{tr}_{plot}.png')


            print("Plotting the all samples (baseline subtracted)")
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



            # # Assigning labels of x-axis 
            # # according to dataframe
            # # plt.xticks(range(len(df_tpc.columns)), df_tpc.columns)
            
            # # Assigning labels of y-axis 
            # # according to dataframe
            # # plt.yticks(range(len(df_tpc)), df_tpc.index)
            # plt.colorbar()
            # plt.savefig('wibeth_frame.png')




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

	main()
