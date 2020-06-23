import xarray as xr
import numpy as np
from datetime import datetime

### External Functions
def time_tag():
    '''just a date tag for now to not save so many files'''
    now = datetime.now()
    now_str = now.strftime("%Y-%m-%d")
    return now_str

def external_save(da_list):
        q = input('Save? (y/n)')
        if q == 'y':
            for da in da_list.da_list:
                filename = da.name+'.'+time_tag()+'.nc'
                path = './'+da.name+'/'+filename
                recent_path = './'+da.name+'/'+da.name+'.nc'
                da = da.astype(str)
                da.to_netcdf(path)#, mode='w') # specific dated instance
                print(path, 'saved')
                da.to_netcdf(recent_path)#, mode='w') # generic "current" file overwrite
                print(recent_path, 'saved')

def pprocess(N):
    N = round(N, 1)
    if N < 10 and N > 0:
        Nprint = '0'+str(N)
    else:
        Nprint = str(N)
    return Nprint

### Classes

class dalist():
    def __init__(self, locs, tag=''):
        self.locs = locs
        self.tag = tag
        self.da_list = self.load()
        self.original = self.da_list.copy() # to aid in recovery if needed, and to check as we go
    
    def load(self):  
        '''Load files corresponding to each location tag "locs" into a list.'''
        da_list = []
        tag = self.tag
        for s in self.locs:
            with xr.open_dataarray(s+'/'+s+tag+'.nc').load() as f:
                da_list.append(f)
        return da_list
    
    def index(self, name):
        i = self.locs.index(name)
        return i
    
    def view(self, name=None):
        '''Formatted printout of all dataarrays'''
        if name in self.locs:
            i = self.index(name)
            view_me = self.da_list[i]
            print(view_me.T)
            print('')
        else:
            for da in self.da_list:
                print(da.T)
                print('')

    def save(self):
        q = input('Save? (y/n)')
        if q == 'y':
            for da in self.da_list:
                da['itime'] = np.arange(len(da.itime))
                filename = da.name+'.'+time_tag()+'.nc'
                path = './'+da.name+'/'+filename
                recent_path = './'+da.name+'/'+da.name+'.nc'
                da = da.astype(str)
                print('saving',path)
                da.to_netcdf(path)#, mode='w') # specific dated instance
                print('saving', recent_path)  
                da.to_netcdf(recent_path)#, mode='w') # generic "current" file overwrite
               
                
    ## Make new dataarray for a location
    def new_da(self, name, lows, out_dates, highs, notes=None):
        # doesn't actually use self, but need it to prevent name from being self
        N = len(lows)
        if type(highs) == type(None):
            highs = np.zeros(N)
        if notes == None:
            notes = np.array(['']*N)
        returned = np.array(['False']*N)
        da = xr.DataArray(np.array([lows,
                                    highs,
                                    out_dates,
                                    notes,
                                    returned]), name=name, dims=['dt', 'itime'],
                          coords=[['low', 'high', 'outdate', 'notes','returned'],np.arange(N)])
        return da

    def ship(self, name, low, date, high=None, notes=None):
        '''Add an entry to da_list for a new flask pair that was shipped'''
        low = str(low) # change type for consistency
        i = self.locs.index(name)
        da = self.da_list[i]
        # check if that flask is already at that station (consistent?)
        for flask in self.original[i].sel(dt='low').values:
            if low==flask:
                print('Flask number {} is already at {}'.format(flask, da.name))
                print('')
                print(da.T)
                return
            
        new_entry = self.new_da(name, [low], [date], [high], notes=[notes])
        da1 = xr.concat([da, new_entry], dim='itime')
        self.da_list[i] = da1
        print(da1.T)
        self.save() # ask if you want to save
        
    def receive(self, name, low, notes=None):
        low = str(low) # change type for consistency
        i = self.locs.index(name)
        da = self.da_list[i]
        if low not in da.sel(dt='low'):
            print('------------Flask {} is not currently at {}------------'.format(low, name))
            print('')
            print(da.T)
            print('')
            return
        BL = da.sel(dt='low')!=low
        print(BL.T)
        print(da.T)
        print(da.where(BL, drop=True).T)
        print('')
        self.da_list[i] = da.where(BL, drop=True)
        # save
        self.save()
    
    def ask_receive(self): 
        name = input('station tag: ')
        low = input('low flask number: ')
        notes = input('notes: ')
        for var in [name, low, notes]:
            if var == 'abort':
                self.view(name)
                print('aborted')
                return
           
        self.receive(name, low, notes)
    
    def ask_ship(self):
        name = input('station tag: ')
        low = input('low flask number: ')
        high = input('high flask number: ')
        date = input('Date Shipped: ')
        notes = input('notes: ')
        for var in [name, low, high, date, notes]:
            if var == 'abort':
                return 'aborted'
        self.ship(name, low, date, high, notes)
    
    def search_all_stations(self, n):
        '''Search all stations for a low-flask number.'''
        n = str(n)
        for da in self.da_list:
            lows = da.sel(dt='low')
            if n in lows:
                print(da)
        
    def overview(self, verbose=False):
        if verbose == True:
            print('Station  ', 'pairs  ', 'oldest  ----------', 'newest  ----------','weeks  ', 'flags')
        else:
            print('Station  ', 'pairs  ', 'weeks  ', 'flags')
        for da in self.da_list:
            N = len(da.T)
            if N > 0:
                oldest_flask = str(da.sel(dt='low')[0].values)
                oldest_day = str(da.sel(dt='outdate')[0].values)
                newest_flask = str(da.sel(dt='low')[-1].values)
                newest_day = str(da.sel(dt='outdate')[-1].values)
                now = datetime.now()
                date = datetime.strptime(newest_day, '%Y-%m-%d')
                diff = now-date
                days = diff.days
                weeks = days/7.0
                flag = ' '
            else:
                oldest_flask = ' n/a '
                oldest_day = ' n/a '
                newest_flask = ' n/a '
                newest_day = ' n/a '
                days = 999
                flag = 'no flasks, '
            if N < 3:
                flag += 'under 3, '
            if N < weeks:
                flag += 'N < weeks, '
            N_print = pprocess(N)
            weeks_print = pprocess(weeks)
            if verbose==True:
                print(da.name,'     ', N_print,'    ', oldest_flask, oldest_day, '   ',
                      newest_flask, newest_day, '   ', weeks_print, '   ', flag)
            else:
                print(da.name,'     ', N_print,'    ', weeks_print, '   ', flag)
        if verbose == True:
            print(
                  '''Flask numbers are all low-valued members of a pair.
                     weeks = weeks since a box was last shipped there.
                     ''')
            

                
                


    def dalist(self):
        '''An alias for overview()'''
        self.overview()


