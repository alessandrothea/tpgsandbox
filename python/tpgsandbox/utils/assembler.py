import pandas as pd


from abc import ABC, abstractmethod

class Assembler(ABC):

    @abstractmethod
    def match(self, name) -> bool:
        pass

    @abstractmethod
    def assemble(self, dataframes : dict ) -> pd.DataFrame:
        pass

class ADCJoiner(Assembler):

    def __init__(self, id: str) -> None:
        self.id = id

    def match(self, name) -> bool:
        return name == self.id
    
    def assemble(self, dataframes: dict) -> pd.DataFrame:
        dfs = {k:v for k,v in dataframes.items() if not v is None}
        print(f"Assembling ADC Frames {len(dfs)}")


        idx = pd.Index([], dtype='uint64')
        for df in dfs.values():
            idx = idx.union(df.index)

        df_adc = pd.DataFrame(index=idx, dtype='uint16')

        for df in dfs.values():
            df_adc = df_adc.join(df)

        df_adc = df_adc.reindex(sorted(df_adc.columns), axis=1)
        
        print(f"Adcs dataframe assembled {len(df_adc)}x{len(df_adc.columns)}")

        return df_adc

class TPConcatenator(Assembler):

    def __init__(self, id: str) -> None:
        self.id = id

    def match(self, name) -> bool:
        return name == self.id
    
    def assemble(self, dataframes: dict) -> pd.DataFrame:
        print("Assembling TPs")
        df_tp = pd.concat(dataframes.values())
        df_tp = df_tp.sort_values(by=['time_start', 'channel'])
        print(f"TPs dataframe assembled {len(df_tp)}")
        return df_tp
    
class AssemblerService:
    def __init__(self) -> None:
        self.assemblers = {}

    def add(self, name: str, assembler: Assembler):

        if name in self.assemblers:
            raise KeyError(f"Assembler {name} already registered")

        self.assemblers[name] = assembler
    
    def get(self, name) -> Assembler:
        return self.assemblers[name]
    
    def assemble(self, dataframe_dict):

        res = {}
        for id, dfs in dataframe_dict.items():
            for n,asm in self.assemblers.items():
                if not asm.match(id):
                    continue

                r = asm.assemble(dfs)
                res[n] = r
        return res