#!/usr/bin/env python

import click
import pandas as pd
from rich import print

from tpgsandbox.utils.reader import RecordReader
import tpgsandbox.utils.unpacker as unpacker
import tpgsandbox.utils.assembler as assembler
from tpgsandbox.emulation.algos import emulate_ped, emulate_running_sum, generate_tps, dbscan_cluster

import detchannelmaps

from sklearn.cluster import DBSCAN

import click
import ast

class PythonLiteralOption(click.Option):

    def type_cast_value(self, ctx, value):
        try:
            return ast.literal_eval(value)
        except:
            raise click.BadParameter(value)


import plotly.express as px
from plotly.subplots import make_subplots
# from plotly.offline import plot
import plotly.io as pio
from pypdf import PdfWriter

@click.command()
@click.option('-l', '--list-records', is_flag=True, default=False)
@click.option('-n', '--num-records', default=1)
# @click.option('-r', '--records', cls=PythonLiteralOption, default=[])
@click.option('-r', '--records', type=(int, int), multiple=True)
@click.option('-c', '--channels', type=int, multiple=True)
@click.argument('raw_files', type=click.Path(exists=True, dir_okay=False), nargs=-1)
def cli(list_records, num_records, records, channels, raw_files):


    

    rr = RecordReader()
    for f in raw_files:
        print(f'Adding {f}')
        rr.add_file(f)

    rr.add_product('bde_eth', unpacker.WIBEthFragmentPandasUnpacker(), assembler.ADCJoiner())
    rr.add_product('tp', unpacker.TPFragmentPandasUnpacker(), assembler.TPConcatenator())

    if list_records:
        for i,(run,tr) in enumerate(rr.iter_records()):
            print(f"{i:04d}: ({run}, {tr})")
        return


    merger = PdfWriter()

    done=0

    for i,(run,tr) in enumerate(rr.iter_records()):
        if done >= num_records:
            break

        if records:
            if not (run,tr) in records:
                continue
        done += 1

        data = rr.load_record(run, tr)
        chmap = detchannelmaps.make_map(data.tpc_chan_map_id)

        # Prepare dataframes
        dfs = data.record

        df_tpc = dfs['bde_eth'].astype('int16')

        # Reindex from the start of the frame
        t0 = df_tpc.index.min()
        
        df_tpc.index = df_tpc.index.astype('int64')-t0

        # TODO: distinguish between readout TPs and Trigger TPs
        df_tps = data.frags['tp'][0]
        df_tps['time_start']=df_tps['time_start'].astype('int64')-t0
        df_tps['time_peak']=df_tps['time_peak'].astype('int64')-t0

        ## Processing starts here
        print("- [cyan]Emulating pedestal[/cyan]")
        df_ped, df_ped_var = emulate_ped(df_tpc, init_ped_range=100)
        df_adc = df_tpc-df_ped

        print("- [cyan]Running Sum[/cyan]")
        df_rs_adc = emulate_running_sum(df_adc, 0.98)

        print("- [cyan]Generating hits [/cyan]")
        df_emu_tps_100 = generate_tps(df_adc, 100, chmap)
        df_emu_tps_200 = generate_tps(df_adc, 200, chmap)

        print("- [cyan]Clustering[/cyan]")
        df_emu_tps_100_cluster = dbscan_cluster(df_emu_tps_100)
        df_emu_tps_200_cluster = dbscan_cluster(df_emu_tps_200)



        print("- [green]Plotting clusters[/green]")
        for p in [2,1,0]:
            ## Generate plosts
            tps = df_emu_tps_100_cluster[df_emu_tps_100_cluster['plane']==p]
            fig = px.scatter(x=tps['channel'],y=tps['time_peak'],color=tps['cluster_label'])
            # Save to a temp file
            pio.write_image(fig, f'tmp_img.pdf', format='pdf')
            # Append to the merger
            merger.append(f'tmp_img.pdf')

        for p in [2,1,0]:

            tps = df_emu_tps_200_cluster[df_emu_tps_200_cluster['plane']==p]
            fig = px.scatter(x=tps['channel'],y=tps['time_peak'],color=tps['cluster_label'])
            # Save to a temp file
            pio.write_image(fig, f'tmp_img.pdf', format='pdf')
            # Append to the merger
            merger.append(f'tmp_img.pdf')

        # # Plot emulated TPS
        # tps = df_emu_tps_100[df_emu_tps_100_cluster['plane']==2]

        # fig = px.scatter(x=df_emu_tps_100[df_emu_tps_100['plane']==2]['channel'],y=df_emu_tps_100[df_emu_tps_100['plane']==2]['time_peak'])
        # # Save to a temp file
        # pio.write_image(fig, f'tmp_img.pdf', format='pdf')
        # # Append to the merger
        # merger.append(f'tmp_img.pdf')

        # # Plot emulated TPS
        # fig = px.scatter(x=df_emu_tps_200[df_emu_tps_200['plane']==2]['channel'],y=df_emu_tps_200[df_emu_tps_200['plane']==2]['time_peak'])
        # # Save to a temp file
        # pio.write_image(fig, f'tmp_img.pdf', format='pdf')
        # # Append to the merger
        # merger.append(f'tmp_img.pdf')

        # plot a waveform
        for ch in channels:
            print(f"- [green]Plotting channel {ch}[/green]")

            wf = pd.DataFrame(
                {
                    'adc_raw':df_tpc[ch],
                    'ped': df_ped[ch], 
                    'ped_var': df_ped_var[ch],
                    'adc': df_adc[ch],
                    'rs_adc': df_rs_adc[ch],
                    'rs_adc_n': df_rs_adc[ch]/df_rs_adc[ch].std()*df_adc[ch].std()
                }
            )


            # cdm={'adc_raw': 'black', 'adc': 'black', 'ped': 'red', 'ped_var': 'red', 'rs_adc': 'orange', 'rs_adc_n': 'orange'}
            cdm={}
            figures = [
                px.line(wf,y=['adc_raw','ped'], color_discrete_map=cdm, title=f'channel {ch}'),
                px.line(wf,y=['ped_var'], color_discrete_map=cdm, title=f'channel {ch}'),
                px.line(wf,y=['adc'], color_discrete_map=cdm, title=f'channel {ch}'),
                px.line(wf,y=['adc', 'rs_adc'], color_discrete_map=cdm, title=f'channel {ch}'),
                px.line(wf,y=['adc', 'rs_adc_n', 'ped_var'], color_discrete_map=cdm, title=f'channel {ch}'),
            ]

            fig = make_subplots(rows=len(figures), cols=1) 

            for j, figure in enumerate(figures):
                for trace in range(len(figure["data"])):
                    fig.append_trace(figure["data"][trace], row=j+1, col=1)
            fig.update_layout(width=1600, height=1600, title_text=f"Channel {ch} [Run {run}, TR {tr}]")
            pio.write_image(fig, f'tmp_img.pdf', format='pdf')
            
            merger.append(f'tmp_img.pdf')

    # Write to an output PDF document
    with open("document-output.pdf", "wb") as output:
        merger.write(output)

        # Close file descriptors
        merger.close()
        output.close()
    return


if __name__ == '__main__':
    cli()