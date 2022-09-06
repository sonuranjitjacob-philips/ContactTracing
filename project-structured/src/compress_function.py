def compress(df):
    """
    Function:           Removing subsequent reports of identical locatons;  
                        Calculating TIME_IN_ZONE calculated as timestamp first report - timestap last report
    
    Input requirements: MONITOR_ID
                        
                        
    """
    
    
    temp = df
    if not df.empty:
        temp = temp.sort_values(by=['TAG_ID', 'EPOCHTIME'])  
        temp = temp[temp.MONITOR_ID != 0]
        temp['keep'] = temp['keep'] = (((temp.TAG_ID == temp.TAG_ID.shift(1))|(temp.TAG_ID == temp.TAG_ID.shift(-1))) & ((temp.MONITOR_ID != temp["MONITOR_ID"].shift(1)) | (temp.MONITOR_ID != temp["MONITOR_ID"].shift(-1))))
        temp['keep'].iloc[0] = True
        temp = temp[temp.keep == True]
        temp['TIME_LAST_REPORT_IN_ZONE'] = np.where((temp.MONITOR_ID == temp.MONITOR_ID.shift(-1)) & (temp.TAG_ID == temp.TAG_ID.shift(-1)) ,temp.EPOCHTIME.shift(-1),temp.EPOCHTIME)
        temp['LOCAL_TIME_LAST_REPORT_IN_ZONE'] = np.where((temp.MONITOR_ID == temp.MONITOR_ID.shift(-1)) & (temp.TAG_ID == temp.TAG_ID.shift(-1)) ,temp.LOCAL_TIME.shift(-1),temp.LOCAL_TIME)
        temp['remove'] = ((temp.TAG_ID == temp.TAG_ID.shift(1)) & (temp.MONITOR_ID == temp["MONITOR_ID"].shift(1)))
        temp = temp[temp.remove == False]
        temp['TIME_IN_ZONE'] = temp.TIME_LAST_REPORT_IN_ZONE - temp.EPOCHTIME
    else:
        temp['TIME_IN_ZONE'] = np.nan
        temp['TIME_LAST_REPORT_IN_ZONE'] = np.nan
        temp['LOCAL_TIME_LAST_REPORT_IN_ZONE'] = np.nan
    return(temp)