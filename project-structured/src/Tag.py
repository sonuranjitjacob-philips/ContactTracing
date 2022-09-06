import pandas as pd
import datetime as dt
import numpy as np
import os
import matplotlib.pyplot as plt

class Tag():

    """
    Assuming the entire dataframe is passed and we extract the properties
    
    """
    
    def __init__(self, df, tag):
        self.tagId = df.iloc[0]['TAG_ID']
        self.tagType = df.iloc[0]['TAG_TYPE']
        self.data = df.loc[df['TAG_ID'] == tag]
        self.compressed_data = self.compress(self.data.copy())
        
    #history based on timeframe specified
    def getHistory(self, startTime, endTime):
        return self.data.loc[(self.data['LOCAL_TIME'] > startTime) & (self.data['LOCAL_TIME'] < endTime)]
    
    def compress(self, df):
        """
        Removes subsequent reports of identical locations; 
        Calculates TIME_IN_ZONE calculated as timestamp first report - timestap last report
        
        Parameters: 
        df (pandas dataframe)
        
        Output:
        temp (pandas dataframe): compressed dataframe
                       
        """
        temp = df.copy()
        if not df.empty:
            temp = temp.sort_values(by=['TAG_ID', 'EPOCHTIME'])  
            temp = temp[temp.MONITOR_ID != 0]
            #keep monitor id if it is not equal to the previous or next one and keep tag id if it is equal to previous or next one
            if not temp.empty:
                temp['keep'] = temp['keep'] = (((temp.TAG_ID == temp.TAG_ID.shift(1))|(temp.TAG_ID == temp.TAG_ID.shift(-1))) & ((temp.MONITOR_ID != temp["MONITOR_ID"].shift(1)) | (temp.MONITOR_ID != temp["MONITOR_ID"].shift(-1))))
                temp['keep'].iloc[0] = True
                temp = temp[temp.keep == True]
                temp['TIME_LAST_REPORT_IN_ZONE'] = np.where((temp.MONITOR_ID == temp.MONITOR_ID.shift(-1)) & (temp.TAG_ID == temp.TAG_ID.shift(-1)) ,temp.EPOCHTIME.shift(-1),temp.EPOCHTIME)
                temp['LOCAL_TIME_LAST_REPORT_IN_ZONE'] = np.where((temp.MONITOR_ID == temp.MONITOR_ID.shift(-1)) & (temp.TAG_ID == temp.TAG_ID.shift(-1)) ,temp.LOCAL_TIME.shift(-1),temp.LOCAL_TIME)
                #remove duplicate monitor ids
                temp['remove'] = ((temp.TAG_ID == temp.TAG_ID.shift(1)) & (temp.MONITOR_ID == temp["MONITOR_ID"].shift(1)))
                temp = temp[temp.remove == False]
                #calculate time in zone
                temp['TIME_IN_ZONE'] = temp.TIME_LAST_REPORT_IN_ZONE - temp.EPOCHTIME
            else:
                print('Compressed dataframe is empty, check your data!')
                return None
        return(temp)
    
    def getLocation(self):
        #connectedmonitor at a particular timeframe
        pass
    
    def getDropZoneLocation(self):
        #dropzonelocation at a particular timeframe
        pass
    def checkOverlap(self, row):
       
        assert len(row) == 1
        #converting to series
        r_series = row.squeeze()
        try:
            a = pd.Interval(r_series['LOCAL_TIME_a'], r_series['LOCAL_TIME_LAST_REPORT_IN_ZONE_a'])
            b = pd.Interval(r_series['LOCAL_TIME_b'], r_series['LOCAL_TIME_LAST_REPORT_IN_ZONE_b'])
            if a.overlaps(b):
                return row
            # emit two rows instead
            cols = ['LOCAL_TIME_a', 'LOCAL_TIME_LAST_REPORT_IN_ZONE_a', 'MONITOR_ID', 'LOCAL_TIME_b', 'LOCAL_TIME_LAST_REPORT_IN_ZONE_b']
            sa, ea, ev, sb, eb = r_series[cols]
            return pd.DataFrame([
                [pd.NaT, pd.NaT, ev, sb, eb],
                [sa, ea, ev, pd.NaT, pd.NaT],
            ], columns=cols)
        except ValueError:
            return row
        
    def getIntersection(self, tag2):    
        for df in [self.compressed_data, tag2.compressed_data]:
            for k in ['LOCAL_TIME', 'LOCAL_TIME_LAST_REPORT_IN_ZONE']:
                df[k] = pd.to_datetime(df[k])
        # next, merge on eventname, regardless of interval overlapping
        zo = self.compressed_data.merge(tag2.compressed_data, on='MONITOR_ID', suffixes=['_a', '_b'], how='outer')
        out = zo.groupby(level=0).apply(self.checkOverlap).reset_index(drop=True)      
        out['start_time'] = out.apply(lambda row: max(row['LOCAL_TIME_a'], row['LOCAL_TIME_b']), axis=1)
        out['end_time'] = out.apply(lambda row: min(row['LOCAL_TIME_LAST_REPORT_IN_ZONE_a'], row['LOCAL_TIME_LAST_REPORT_IN_ZONE_b']), axis=1)
        out['overlap_minutes'] = (out['end_time'] - out['start_time'])
        out['overlap_minutes'] = out['overlap_minutes'].dt.total_seconds()/60
        out = out.sort_values(by='overlap_minutes')

        out = out[['TAG_ID_a', 'TAG_ID_b','MONITOR_ID', 'LOCAL_TIME_a', 'LOCAL_TIME_LAST_REPORT_IN_ZONE_a', 'LOCAL_TIME_b', 'LOCAL_TIME_LAST_REPORT_IN_ZONE_b', 'ZONE_NAME_a', 'ZONE_NAME_b', 'start_time', 'end_time', 'overlap_minutes']].dropna()

        out = out[['TAG_ID_a', 'TAG_ID_b', 'start_time', 'end_time', 'overlap_minutes', 'MONITOR_ID', 'ZONE_NAME_a', 'ZONE_NAME_b',  'LOCAL_TIME_a', 'LOCAL_TIME_LAST_REPORT_IN_ZONE_a', 'LOCAL_TIME_b', 'LOCAL_TIME_LAST_REPORT_IN_ZONE_b', ]]

        if out.empty:
            print('There is no contact between the assets!')
        return out
            
    def displayHistory(self):
        fig, ax = plt.subplots()
        ax.plot(self.data['EPOCHTIME'],self.data['AREA_NAME'],marker='o',linestyle="--",)
        ax.set_xlabel('EPOCH TIME')
        ax.set_ylabel('MONITOR ID')
        ax.set_title(f"Time vs AREA NAME for {self.data.iloc[0]['TAG_ID']}")
        fig.set_size_inches(16.5, 8.5)
        plt.show()
        
