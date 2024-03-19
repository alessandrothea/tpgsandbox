import pandas as pd
import logging


from abc import ABC, abstractmethod

class Assembler(ABC):

    @abstractmethod
    def match(self, name) -> bool:
        pass

    @abstractmethod
    def assemble(self, dataframes : dict ) -> pd.DataFrame:
        pass

class ADCJoiner(Assembler):

    def __init__(self) -> None:
        pass

    def match(self, sid: int) -> bool:
        return True
    
    def assemble(self, dataframes: dict) -> pd.DataFrame:
        dfs = {k:v for k,v in dataframes.items() if not v is None}
        logging.info(f"Assembling ADC Frames {len(dfs)}")


        idx = pd.Index([], dtype='uint64')
        for df in dfs.values():
            idx = idx.union(df.index)

        df_adc = pd.DataFrame(index=idx, dtype='uint16')

        for df in dfs.values():
            df_adc = df_adc.join(df)

        df_adc = df_adc.reindex(sorted(df_adc.columns), axis=1)
        
        logging.info(f"Adcs dataframe assembled {len(df_adc)}x{len(df_adc.columns)}")

        return df_adc

class TPConcatenator(Assembler):

    def __init__(self) -> None:
        pass

    def match(self, sid: int) -> bool:
        return True
    
    def assemble(self, dataframes: dict) -> pd.DataFrame:
        logging.info("Assembling TPs")
        df_tp = pd.concat( dataframes.values() )
        df_tp = df_tp.sort_values(by=['time_start', 'channel'])
        logging.info(f"TPs dataframe concatenated {len(df_tp)}")
        return df_tp
    
class AssemblerService:
    def __init__(self) -> None:
        self.assemblers = {}

    def add(self, product_id: str, data_id: str, assembler: Assembler):
        """Add an assembler for frament data of name 'data_id' and produce 'product_id"

        Args:
            product_id (str): name of the assembler product
            data_id (str): name of the input data
            assembler (Assembler): assembler object

        Raises:
            KeyError: _description_
        """
        
        if product_id in self.assemblers:
            raise KeyError(f"Assembler {product_id} already registered")

        self.assemblers[product_id] = (data_id, assembler)
    
    def get(self, product_id) -> Assembler:
        return self.assemblers[product_id]
    
    def assemble(self, fragments: dict):

        res = {}
        # for id, dfs in dataframe_dict.items():
        for prod_id,(data_id, asm) in self.assemblers.items():
            if not data_id in fragments:
                continue
            
            
            dfs = { k:v for k,v in fragments[data_id].items() if asm.match(v)}

            r = asm.assemble(dfs)
            res[prod_id] = r
        return res