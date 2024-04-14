# import pathlib
import collections
import fddetdataformats
import trgdataformats
import daqdataformats
import detchannelmaps

import logging

import pandas as pd
import numpy as np

import rawdatautils.unpack.wibeth as wibeth_unpack
import rawdatautils.unpack.triggerprimitive as tp_unpack

from rich import print
from abc import ABC, abstractmethod
from typing import Any

class UnpakerContext:
    """Class representing the context in which a fragment is unpacked.

    tpc_chan_map: TPC channel map object
    """
    def __init__(self):
        self.tpc_chan_map = None

class FragmentUnpacker(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def match(self, frag: daqdataformats.Fragment, sid: daqdataformats.SourceID) -> bool:
        pass
    
    @abstractmethod
    def unpack(self, frag: daqdataformats.Fragment, ctx: UnpakerContext) -> Any:
        pass


class WIBEthFragmentNumpyUnpacker(FragmentUnpacker):
    
    def __init__(self):
        super().__init__()
    

    def match(self, frag: daqdataformats.Fragment, sid: daqdataformats.SourceID) -> bool:
        return (frag.get_fragment_type() == daqdataformats.FragmentType.kWIBEth) and (sid.subsystem == daqdataformats.SourceID.kDetectorReadout)
    

    def unpack(self, frag: daqdataformats.Fragment, ctx: UnpakerContext) -> tuple:
        # frag_hdr = frag.get_header()

        if not frag.get_data_size():
            return None, None
        
        if True:
            wf = fddetdataformats.WIBEthFrame(frag.get_data())
            dh = wf.get_daqheader()
            wh = wf.get_wibheader()
            ts, det_id, crate_no, slot_no, stream_no = (dh.timestamp, dh.det_id, dh.crate_id, dh.slot_id, dh.stream_id)

            logging.info(f"ts: 0x{ts:016x} (15 lsb: 0x{ts&0x7fff:04x}) cd_ts_0: 0x{wh.colddata_timestamp_0:04x} cd_ts_1: 0x{wh.colddata_timestamp_1:04x} crate: {crate_no}, slot: {slot_no}, stream: {stream_no}")

        ts = wibeth_unpack.np_array_timestamp(frag)
        adcs = wibeth_unpack.np_array_adc(frag)

        return ts, adcs
    
    
class WIBEthFragmentPandasUnpacker(WIBEthFragmentNumpyUnpacker):

    def __init__(self):
        super().__init__()

    def unpack(self, frag: daqdataformats.Fragment, ctx: UnpakerContext) -> pd.DataFrame:

        payload_size = frag.get_data_size()
        if not payload_size:
            return None
        
        wf = fddetdataformats.WIBEthFrame(frag.get_data())
        dh = wf.get_daqheader()
        wh = wf.get_wibheader()
        ts, det_id, crate_no, slot_no, stream_no = (dh.timestamp, dh.det_id, dh.crate_id, dh.slot_id, dh.stream_id)
        n_chan_per_stream = 64
        n_streams_per_link = 4

        logging.info(f"ts: 0x{ts:016x} (15 lsb: 0x{ts&0x7fff:04x}) cd_ts_0: 0x{wh.colddata_timestamp_0:04x} cd_ts_1: 0x{wh.colddata_timestamp_1:04x} crate: {crate_no}, slot: {slot_no}, stream: {stream_no}")

        if ctx.tpc_chan_map:
            off_chans = [ctx.tpc_chan_map.get_offline_channel_from_crate_slot_stream_chan(crate_no, slot_no, stream_no, c) for c in range(n_chan_per_stream)]
        else:
            first_chan += (stream_no >> 6)*n_chan_per_stream*n_streams_per_link
            off_chans = [c for c in range(first_chan,first_chan+n_chan_per_stream)]

        ts, adcs = super().unpack(frag, ctx)

        if ts is None or adcs is None:
            return None

        df = pd.DataFrame(collections.OrderedDict([('ts', ts)]+[(off_chans[c], adcs[:,c]) for c in range(64)]))
        df = df.set_index('ts')

        return df


class TPFragmentPandasUnpacker(FragmentUnpacker):

    def __init__(self):
        super().__init__()
    
    def match(self, frag: daqdataformats.Fragment, sid: daqdataformats.SourceID) -> bool:
        return (frag.get_fragment_type() == daqdataformats.FragmentType.kTriggerPrimitive) and (sid.subsystem == daqdataformats.SourceID.kTrigger)
    
    @classmethod
    def dtypes(cls):
        return [
                ('time_start', np.uint64), 
                ('time_peak', np.uint64), 
                ('time_over_threshold', np.uint64), 
                ('channel',np.uint32),
                ('adc_integral', np.uint32), 
                ('adc_peak', np.uint16), 
                ('flag', np.uint16),
                ('plane', np.uint8),
            ]

    @classmethod
    def empty(cls) -> pd.DataFrame:
        return pd.DataFrame(np.empty(0, cls.dtypes()))
    
    def unpack(self, frag: daqdataformats.Fragment, ctx: UnpakerContext) -> pd.DataFrame:
        # Convert fragment into a numpy record array
        arr = tp_unpack.get_tp_array(frag)
        # and into a dataframe
        df = pd.DataFrame(arr)
        # Filter extra fields
        df = df.filter(items=[ n for (n,_) in self.dtypes()])
        # add plane information
        df['plane'] = df['channel'].apply(lambda x: ctx.tpc_chan_map.get_plane_from_offline_channel(x)).astype(np.uint8)
        return df


# class TPFragmentPandasUnpackerOld(FragmentUnpacker):

#     def __init__(self):
#         super().__init__()
    
#     def match(self, frag: daqdataformats.Fragment, sid: daqdataformats.SourceID) -> bool:
#         return (frag.get_fragment_type() == daqdataformats.FragmentType.kTriggerPrimitive) and (sid.subsystem == daqdataformats.SourceID.kTrigger)
    
#     @classmethod
#     def dtypes(cls):
#         return [
#                 ('time_start', np.uint64), 
#                 ('time_peak', np.uint64), 
#                 ('time_over_threshold', np.uint64), 
#                 ('channel',np.uint32),
#                 ('adc_integral', np.uint32), 
#                 ('adc_peak', np.uint16), 
#                 ('flag', np.uint16),
#                 ('plane', np.uint8),
#             ]
    
#     @classmethod
#     def empty(cls) -> pd.DataFrame:
#         return pd.DataFrame(np.empty(0, cls.dtypes()))

#     def unpack(self, frag: daqdataformats.Fragment, ctx: UnpakerContext) -> pd.DataFrame:

#         tp_array = []
#         tp_size = trgdataformats.TriggerPrimitive.sizeof()

#         frag_hdr = frag.get_header()

#         n_frames = (frag.get_size()-frag_hdr.sizeof())//tp_size
#         logging.debug(f"Number of TP frames: {n_frames}")
        
#         # Initialize the TP array buffer
#         tp_array = np.zeros(
#             n_frames, 
#             dtype=self.dtypes()
#         )

        
#         # Populate the buffer
#         for i in range(n_frames):
#             tp = trgdataformats.TriggerPrimitive(frag.get_data(i*tp_size))
#             tp_array[i] = (
#                 tp.time_start,
#                 tp.time_peak,
#                 tp.time_over_threshold,
#                 tp.channel,
#                 tp.adc_integral,
#                 tp.adc_peak,
#                 tp.flag,
#                 9999 # placeholder, not set
#             )

#         # Create the dataframe
#         df = pd.DataFrame(tp_array)

#         logging.debug(f"TP Dataframe size {len(df)}")
#         # print(df)
#         # Add plane information (here or in user code?)
#         df['plane'] = df['channel'].apply(lambda x: ctx.tpc_chan_map.get_plane_from_offline_channel(x)).astype(np.uint8)
#         return df


class TAFragmentPandasUnpacker(FragmentUnpacker):

    def __init__(self):
        super().__init__()
    
    def match(self, frag: daqdataformats.Fragment, sid: daqdataformats.SourceID) -> bool:
        return (frag.get_fragment_type() == daqdataformats.FragmentType.kTriggerActivity) and (sid.subsystem == daqdataformats.SourceID.kTrigger)
    
    @classmethod
    def dtypes(cls):
        return [
            ('time_start', np.uint64), 
            ('time_end', np.uint64), 
            ('time_peak', np.uint64), 
            ('time_activity', np.uint64), 
            ('channel_start', np.uint32), 
            ('channel_end', np.uint32), 
            ('channel_peak', np.uint32), 
            ('adc_integral', np.uint32), 
            ('adc_peak', np.uint16),
            ('plane', np.uint8),
        ]

    @classmethod
    def empty(cls):
        return pd.DataFrame(np.empty(0, cls.dtypes()))


    def test_wrapper(self, frag, offset):
        offset=0

        b = frag.get_data_bytes(offset)
        ta = trgdataformats.TriggerActivity(b)

        import rich
        rich.print('>'*80)
        rich.print(len(ta))
        rich.print(f"""
time [
    start={ta.data.time_start},
    end={ta.data.time_end},
    peak={ta.data.time_peak},
    act={ta.data.time_activity}], 
chan: [
    start={ta.data.channel_start},
    end={ta.data.channel_end},
    peak={ta.data.channel_peak}] ,
adc: [
    int={ta.data.adc_integral},
    peak={ta.data.adc_peak}
]"""
        )
        
        for i in range(len(ta)):
            rich.print(ta[i].time_start, ta[i].time_peak, ta[i].time_over_threshold)

        rich.print('<'*80)
        

    def unpack(self, frag: daqdataformats.Fragment, ctx: UnpakerContext) -> pd.DataFrame:

        # self.test_wrapper(frag)
        data_size = frag.get_data_size()

        offset=0
        offsets = []
        n_entries = 0

        while(offset < data_size):
            offsets.append(offset)

            ta_o = trgdataformats.TriggerActivityOverlay(frag.get_data(offset))

            n_entries += 1
            offset += ta_o.sizeof()

        # Initialize the TA array buffer
        ta_array = np.zeros(
            n_entries, 
            dtype=self.dtypes()
        )

        offset=0
        for i, offset in enumerate(offsets):
            ta_o = trgdataformats.TriggerActivityOverlay(frag.get_data(offset))

            ta_array[i] = (
                ta_o.data.time_start,
                ta_o.data.time_end,
                ta_o.data.time_peak,
                ta_o.data.time_activity,
                ta_o.data.channel_start,
                ta_o.data.channel_end,
                ta_o.data.channel_peak,
                ta_o.data.adc_integral,
                ta_o.data.adc_peak,
                9999 # placeholder, not set
            )


        # Create the dataframe
        df = pd.DataFrame(ta_array)
        # logging.debug(f"TA Dataframe size {len(df)}")
        # print(df)
        # Add plane information (here or in user code?)
        df['plane'] = df['channel_peak'].apply(lambda x: ctx.tpc_chan_map.get_plane_from_offline_channel(x)).astype(np.uint8)
        return df


class TCFragmentPandasUnpacker(FragmentUnpacker):

    def __init__(self):
        super().__init__()

    def match(self, frag: daqdataformats.Fragment, sid: daqdataformats.SourceID) -> bool:
        return (frag.get_fragment_type() == daqdataformats.FragmentType.kTriggerCandidate) and (sid.subsystem == daqdataformats.SourceID.kTrigger)

    @classmethod
    def dtypes(cls):
        return [
                ('time_start', np.uint64), 
                ('time_end', np.uint64), 
                ('time_candidate', np.uint64), 
            ]

    @classmethod
    def empty(cls):
        return pd.DataFrame(np.empty(0, cls.dtypes()))

    def unpack(self, frag: daqdataformats.Fragment, ctx: UnpakerContext) -> pd.DataFrame:

        data_size = frag.get_data_size()

        offset=0
        offsets = []
        n_entries = 0

        while(offset < data_size):
            offsets.append(offset)

            tc = trgdataformats.TriggerCandidateOverlay(frag.get_data(offset))

            n_entries += 1
            offset += tc.sizeof()


        # Initialize the TA array buffer
        tc_array = np.zeros(
            n_entries, 
            self.dtypes()
        )

        offset=0
        for i, offset in enumerate(offsets):
            ta = trgdataformats.TriggerCandidateOverlay(frag.get_data(offset))

            tc_array[i] = (
                ta.data.time_start,
                ta.data.time_end,
                ta.data.time_candidate,
            )


        # Create the dataframe
        df = pd.DataFrame(tc_array)
        logging.debug(f"TC Dataframe size {len(df)}")
        return df
    

class DAPHNEStreamFragmentPandasUnpacker(FragmentUnpacker):
    
    def __init__(self):
        super().__init__()
    
    def match(self, frag: daqdataformats.Fragment, sid: daqdataformats.SourceID) -> bool:
        return (frag.get_fragment_type() == daqdataformats.FragmentType.kDAPHNE) and (sid.subsystem == daqdataformats.SourceID.kDetectorReadout)

    def unpack(self, frag: daqdataformats.Fragment, ctx: UnpakerContext) -> pd.DataFrame:
        frag_hdr = frag.get_header()

        payload_size = (frag.get_size()-frag_hdr.sizeof())
        if not payload_size:
            return None
        
        print(f"DAPHNE payload size {payload_size}")
        
        df = fddetdataformats.DAPHNEStreamFrame(frag.get_data())

        dh = df.get_daqheader()

        ts, det_id, crate_no, slot_no, stream_no = (dh.get_timestamp(), dh.det_id, dh.crate_id, dh.slot_id, dh.link_id)
        print(ts, det_id, crate_no, slot_no, stream_no)


        df = pd.DataFrame()
        return df

### 
# Unpacker Service
###
    

from concurrent.futures import ThreadPoolExecutor, as_completed


class UnpackerService:
    """Helper class to unpack Trigger Records"""
    
    _openv_2_chmap = {
        'np04hd': 'PD2HDChannelMap',
        'np04hdcoldbox': 'HDColdboxChannelMap',
        'np02vd': 'PD2VDBottomTPCChannelMap',
        'np02vdcoldbox': 'VDColdboxChannelMap',
    }

    def __init__(self):
        self.fragment_unpackers = {}
        self._tpc_chan_map_cache = {}

    def _get_tpc_channel_map(self, ch_map_id) -> detchannelmaps.TPCChannelMap:
        '''Get the channel map'''
        return self._tpc_chan_map_cache.setdefault(ch_map_id, detchannelmaps.make_map(ch_map_id))
        

    def add(self, prod_name, unpacker):

        if prod_name in self.fragment_unpackers:
            raise KeyError(f"Unpacker for product {prod_name} already registered")

        self.fragment_unpackers[prod_name] = unpacker

    def get(self, prod_name):
        return self.fragment_unpackers[prod_name]
        

    def unpack(self, raw_data_file, tr_id: int, seq_id: int=0, max_thread=10, tpc_chan_map_id=None) -> dict:
        """Unpack trigger record

        Args:   
            raw_data_file (_type_): _description_
            tr_id (int): _description_
            seq_id (int, optional): _description_. Defaults to 0.
            max_thread (int, optional): _description_. Defaults to 10.
            op_env (str, optional): _description_. Defaults to None.

        Returns:
            dict: _description_
        """

        res = {}
        print('SSSS')

        # Recover the tpc_chan_map_id from the operational environment from file if not specified
        if tpc_chan_map_id is None:
            op_env = raw_data_file.get_attribute('operational_environment')
            tpc_chan_map_id = self._openv_2_chmap.get(op_env, None)

        ctx = UnpakerContext()
        ctx.tpc_chan_map_id = tpc_chan_map_id
        ctx.tpc_chan_map = self._get_tpc_channel_map(tpc_chan_map_id)

        
        tr_source_ids = raw_data_file.get_source_ids((tr_id, seq_id))

        unpack_list = []
        for sid in tr_source_ids:
            # Get the fragment
            frag = raw_data_file.get_frag((tr_id, seq_id),sid)

            for prod,upk in self.fragment_unpackers.items():
                if not upk.match(frag, sid):
                    continue

                unpack_list.append((prod, upk, ctx, sid, frag))


        with ThreadPoolExecutor(max_workers=max_thread) as xtor:
            futures = {xtor.submit(upk.unpack, frag, ctx):(sid, prod) for prod, upk, ctx, sid, frag in unpack_list}
            for f in as_completed(futures):
                sid, prod = futures[f]
                # logging.debug(f"[{n}] Unpacking Subsys={sid.subsystem}, id={sid.id}")                
                r = f.result()
                logging.debug(f"[{prod}] Unpacked Subsys={sid.subsystem}, id={sid.id} ({len(r) if r is not None else 0})")
                res.setdefault(prod,{})[sid.id] = r

        return res