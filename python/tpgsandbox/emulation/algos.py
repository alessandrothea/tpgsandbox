from numba import njit
import numpy as np
import pandas as pd
from typing import Literal
from sklearn.cluster import DBSCAN

@njit
def frugal_pedestal( adcs, median_0 = 0, acc_0 = 0, limit=10):
    """_summary_

    Args:
        adcs (_type_): _description_
        median_0 (int, optional): _description_. Defaults to 0.
        acc_0 (int, optional): _description_. Defaults to 0.
        limit (int, optional): _description_. Defaults to 10.

    Returns:
        _type_: _description_
    """

    peds = np.zeros_like(adcs)
    
    median = median_0
    acc = acc_0
    
    for i, adc in enumerate(adcs):

        if adc > median:
            acc+=1
        elif adc < median:
            acc-=1

        if acc == limit:
            acc = 0
            median +=1
        elif acc == -limit:
            acc = 0
            median-=1
        # peds[i] = adc[i]
        peds[i] = median
        
    # A somewhat trivial example
    return peds

@njit
def running_sum(adcs, r=1):

    rs_adcs = np.zeros_like(adcs)
    s = 0
    for i in range(len(adcs)):
        s = r*s+ adcs[i]
        rs_adcs[i] = s 
    return rs_adcs

from numba.types import List,Tuple,int64,uint16,int16,int32

@njit('Tuple((int64,uint64[:],uint64[:],uint16[:],uint16[:],uint32[:]))(int64[:],int16[:],int64)')
def find_hits(ts: np.array, adcs: np.array, threshold=100):
    """Find his on a waveform by applying a threshold

    Args:
        ts (np.array): numpy array containing the timestamps at which the data is sampled
        adcs (np.array): _description_
        threshold (int, optional): _description_. Defaults to 100.

    Returns:
        _type_: _description_
    """
    npts = len(adcs)//2

    num_hits = 0 # store number of found hits
    # list of hits parameters to be returned by this numba function (see dc.hits)
    v_time_start          = np.zeros(npts,dtype=np.uint64)
    v_time_peak           = np.zeros(npts,dtype=np.uint64)
    v_time_over_threshold = np.zeros(npts,dtype=np.uint16)
    v_adc_peak            = np.zeros(npts,dtype=np.uint16)
    v_adc_integral        = np.zeros(npts,dtype=np.uint32)

    in_hit=False
    time_start=None
    time_peak=None
    adc_peak=None
    time_over_threshold=None
    adc_integral=None

    for i,adc in enumerate(adcs):
        
        if in_hit:
            if adc >= threshold:
                in_hit=True
                adc_integral += adc
                if adc > adc_peak:
                    adc_peak = adc
                    time_peak = ts[i]
            else:
                in_hit=False
                time_over_threshold = ts[i]-time_start

                v_time_start[num_hits] = time_start
                v_time_peak[num_hits] = time_peak
                v_time_over_threshold[num_hits] = time_over_threshold
                v_adc_peak[num_hits] = adc_peak
                v_adc_integral[num_hits] = adc_integral

                num_hits+=1
                
                time_start=None
                time_peak=None
                adc_peak=None
                time_over_threshold=None
                adc_integral=None
            
        else:
            if adc >= threshold:
                in_hit=True
                time_start = ts[i]
                time_peak = ts[i]
                adc_peak = adc
                adc_integral = adc
            else:
                in_hit=False
    
    return (num_hits, v_time_start[:num_hits], v_time_peak[:num_hits], v_time_over_threshold[:num_hits], v_adc_peak[:num_hits], v_adc_integral[:num_hits])
    

InitialPedestalEstimatorAlgo = Literal["mode", "mean"]

def emulate_ped(df_rawadc: pd.DataFrame, limit: int=10, init_ped_algo: InitialPedestalEstimatorAlgo='mode', init_ped_range: int = None) -> tuple[pd.DataFrame, pd.DataFrame]:

    match init_ped_algo:
        case 'mode':
            # Calculate the initial pedestal value using the mode over 0:init_ped_range
            adc_modes = df_rawadc[:init_ped_range].mode().iloc[0].astype('int16')
        case 'mean':
            # Calculate the initial pedestal value using the mean over 0:init_ped_range
            adc_modes = df_rawadc[:init_ped_range].mean().astype('int16')
        case _:
            raise ValueError(f"Pedestal estimator algorithm '{init_ped_algo}' not recognised")

    df_ped = pd.DataFrame().reindex_like(df_rawadc)
    for c,s in df_rawadc.items():
        df_ped[c] = frugal_pedestal(s.to_numpy(), median_0=adc_modes[c], limit=limit)
    df_ped_var = df_ped-adc_modes
    return df_ped, df_ped_var


def emulate_running_sum(df_adc: pd.DataFrame, r: float=0.98):
    df_rs_adc = pd.DataFrame().reindex_like(df_adc)
    
    for c,s in df_adc.items():
        df_rs_adc[c] = running_sum(s.to_numpy(), r=r)
    return df_rs_adc


def generate_tps(df_adc: pd.DataFrame, threshold: int, chmap):
    dtypes = [
                ('time_start', np.uint64), 
                ('time_peak', np.uint64), 
                ('time_over_threshold', np.uint64), 
                ('channel',np.uint32),
                ('adc_integral', np.uint32), 
                ('adc_peak', np.uint16), 
                ('flag', np.uint16),
                ('plane', np.uint8),
        ]

    empty_tps = pd.DataFrame(np.empty(0, dtypes))

    dfs_tp = []
    ts = df_adc.index.to_numpy()
    for c,s in df_adc.items():
        num_hits, v_time_start, v_time_peak, v_time_over_threshold, v_adc_peak, v_adc_integral = find_hits(ts, s.to_numpy(), threshold)
        if num_hits > 0: 
            
            chan_tps = empty_tps.copy()
            chan_tps['channel']=[c]*num_hits
            chan_tps['flag']=[0]*num_hits
            chan_tps['plane']=[chmap.get_plane_from_offline_channel(c)]*num_hits
            chan_tps['time_start']=v_time_start
            chan_tps['time_peak']=v_time_peak
            chan_tps['time_over_threshold']=v_time_over_threshold
            chan_tps['adc_peak']=v_adc_peak
            chan_tps['adc_integral']=v_adc_integral

            dfs_tp.append(chan_tps)
    return pd.concat(dfs_tp)


def dbscan_cluster(df_tps, eps=40, min_samples=5):
    """
    Cluster trigger primitives using the dbscan algorithm

    The `dbscan_cluster` 

    Args:
        df_tps (_type_): _description_
        eps (int, optional): _description_. Defaults to 40.
        min_samples (int, optional): _description_. Defaults to 5.

    Returns:
        _type_: _description_
    """
    points = df_tps[['time_peak','channel']].copy()
    points['time_peak'] = points['time_peak']/32
    clustering =  DBSCAN(eps=eps, min_samples=min_samples).fit(points.to_numpy())
    res = df_tps.copy()
    res['cluster_label'] = clustering.labels_
    return res