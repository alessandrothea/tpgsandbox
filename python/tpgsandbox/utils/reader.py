import pandas as pd
from dataclasses import dataclass
from rich import print
from typing import Generator, Any

import hdf5libs
from . import unpacker
from . import assembler
import detchannelmaps

openv_2_chmap = {
    'np04hd': 'PD2HDChannelMap',
    'np04hdcoldbox': 'HDColdboxChannelMap',
    'np02vd': 'PD2VDBottomTPCChannelMap',
    'np02vdcoldbox': 'VDColdboxChannelMap',
}

@dataclass
class RawdataFileInfo:
    '''
    Hold rawdata information
    '''
    path: str
    run_number: int
    tr_list : list[int]
    tpc_chan_map_id : str

@dataclass
class RecordData:

    frags : dict
    record : dict
    tpc_chan_map_id : str

    
class RecordReader():

    def __init__(self):
        self.raw_files = {}
        self.record_list = {}
        self.tpc_chan_map_cache = {}
        self.unpacker = unpacker.UnpackerService()
        self.assembler = assembler.AssemblerService()


    def get_tpc_channel_map(self, ch_map_id) -> detchannelmaps.TPCChannelMap:
        '''Get the channel map'''
        return self.tpc_chan_map_cache.setdefault(ch_map_id, detchannelmaps.make_map(ch_map_id))
            

    def add_file(self, path):
        '''
        Add new entry to the reader
        '''

        if path in self.raw_files:
            raise KeyError(f"file {path} already added")
        
        rdf = hdf5libs.HDF5RawDataFile(path)
        op_env = rdf.get_attribute('operational_environment')
        run_number=rdf.get_int_attribute('run_number')
        
        tpc_chan_map_id = openv_2_chmap.get(op_env, None)
        tr_list = [ i for i,_ in rdf.get_all_trigger_record_ids()]

        rfi = RawdataFileInfo(
            path,
            # rdf,
            run_number,
            tr_list,
            tpc_chan_map_id
        )

        self.raw_files[path] = rfi

        # Warning, missing protection against existing 
        self.record_list.setdefault(run_number,{}).update( { tr:rfi for tr in tr_list} )

    
    def remove_file(self, path):
        '''Remove file and associated trigger records'''
        if not path in self.raw_files:
            raise KeyError(f"file {path} not known")

        r = self.raw_files.pop(path)

        for i in r.tr_list:
            del self.record_list[r.run_number][i]

    def add_product(self, product, unpacker, assembler):
        self.unpacker.add(product, unpacker)
        self.assembler.add(product, product, assembler)

    def load_record(self, run, tr):
        '''Load a trigger record from a specific run'''

        if not run in self.record_list:
            raise KeyError(f"Run {run} not found")

        if not tr in self.record_list[run]:
            raise KeyError(f"Trigger record {tr} not found in run {run}")

        r = self.record_list[run][tr]

        # # TODO: make configurable
        # upk = unpacker.UnpackerService()
        # asm = assembler.AssemblerService()

        # # Unpackers
        # upk.add_unpacker('bde_eth', unpacker.WIBEthFragmentPandasUnpacker())
        # upk.add_unpacker('tp', unpacker.TPFragmentPandasUnpacker())

        # # Assemblers
        # weth_asb = assembler.ADCJoiner('bde_eth')
        # tp_asb = assembler.TPConcatenator('tp')
        
        # asm.add('bde_eth', weth_asb)
        # asm.add('tp', tp_asb)


        # Open the rawdata file
        # A little inefficient? Maybe keep a cache?
        print(f"Opening {r.path}")
        rdf = hdf5libs.HDF5RawDataFile(r.path)

        # Run unpackers
        print(f"Loading record {tr}")
        df_frags = self.unpacker.unpack(rdf, tr, tpc_chan_map_id=r.tpc_chan_map_id)

        # Assemble final products
        df_tr = self.assembler.assemble(df_frags)

        return RecordData(df_frags, df_tr, r.tpc_chan_map_id )

    def get_records(self) -> dict:
        '''Get the records known to the reader'''
        return { run:list(tr_infos.keys()) for run,tr_infos in self.record_list.items() }

    def iter_records(self) -> Generator[Any,Any,Any]:
        '''Iterate over all trigger records of all runs'''
        for run,tr_infos in self.record_list.items():
            for tr in tr_infos:
                yield run,tr