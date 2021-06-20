import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import dask.dataframe as dd


#reads and prints the setting out points for tunnel geometry 
IMP = pd.read_csv('geom.csv', header=0, nrows=14)
#convert imported data to dataframe
dfSOP = pd.DataFrame(IMP)
#calculated functions
EDif = dfSOP.East_B - dfSOP.East_A
NDif = dfSOP.North_B - dfSOP.North_A
ELVDif = dfSOP.Elev_B - dfSOP.Elev_A
Radius = dfSOP.Rad
HorDif = np.sqrt(EDif ** 2 + NDif ** 2)
CirDis = (2 * np.pi) * Radius * np.degrees (np.arccos (((Radius ** 2 + Radius ** 2) - HorDif ** 2) / (2 * Radius ** 2)) / 360)
Bearing = (np.degrees(np.arctan2(EDif,NDif)) + 360) % 360
#insert calculated func
dfSOP.insert(13, "EDif", EDif, True)
dfSOP.insert(13, "NDif", NDif, True)
dfSOP.insert(13, "ELVDif", ELVDif, True)
dfSOP.insert(13, "HorDif", HorDif, True)
dfSOP.insert(13, "CirDis", CirDis, True)
dfSOP.insert(13, "Bearing", Bearing, True)
dfSOP = dfSOP.replace(np.nan, '', regex=True)
dist = np.where(dfSOP['CirDis'] == '' , dfSOP['HorDif'], dfSOP['CirDis'] )
dfSOP.insert(13,"dist",pd.to_numeric(dist), True)

chainage_B = dfSOP['dist'].cumsum()
dfSOP.insert(4, "chainage_B",chainage_B, True)
chainage_A = chainage_B - dist
dfSOP.insert(4, "chainage_A",chainage_A, True)

col_ch = dfSOP["chainage_B"]
max_ch = col_ch.max()
max_rdu = 400

#round(max_ch)
#CHANGE TO max_ch to allow input for definition (1m, 0.1m etc)

dfCALC = pd.DataFrame(np.random.randint(0, 10, size=(max_rdu, 0)))

Calc_chain = range(max_rdu)
dfCALC.insert(0,'row', Calc_chain, True)
dfCALC.insert(0,'max', (max_rdu-1), True)
dfCALC['ch_max'] = (max_ch)

dfCALC['chainage'] = dfCALC['ch_max'] * ( dfCALC['row'] / dfCALC['max'] )

#index the range of chainages between setting out points 
dfSOP.index = pd.IntervalIndex.from_arrays(dfSOP['chainage_A'],dfSOP['chainage_B'],closed='both')

# create a new column populated with values for chainage in dfCALC
dfCALC['SOP'] = dfCALC['chainage'].apply(lambda x : dfSOP.iloc[dfSOP.index.get_loc(x)]['SOP'])
dfCALC['East_A'] = dfCALC['chainage'].apply(lambda x : dfSOP.iloc[dfSOP.index.get_loc(x)]['East_A'])
dfCALC['North_A'] = dfCALC['chainage'].apply(lambda x : dfSOP.iloc[dfSOP.index.get_loc(x)]['North_A'])
dfCALC['Elev_A'] = dfCALC['chainage'].apply(lambda x : dfSOP.iloc[dfSOP.index.get_loc(x)]['Elev_A'])
dfCALC['chainage_A'] = dfCALC['chainage'].apply(lambda x : dfSOP.iloc[dfSOP.index.get_loc(x)]['chainage_A'])
dfCALC['East_B'] = dfCALC['chainage'].apply(lambda x : dfSOP.iloc[dfSOP.index.get_loc(x)]['East_B'])
dfCALC['North_B'] = dfCALC['chainage'].apply(lambda x : dfSOP.iloc[dfSOP.index.get_loc(x)]['North_B'])
dfCALC['Elev_B'] = dfCALC['chainage'].apply(lambda x : dfSOP.iloc[dfSOP.index.get_loc(x)]['Elev_B'])
dfCALC['chainage_B'] = dfCALC['chainage'].apply(lambda x : dfSOP.iloc[dfSOP.index.get_loc(x)]['chainage_B'])
dfCALC['Dia'] = dfCALC['chainage'].apply(lambda x : dfSOP.iloc[dfSOP.index.get_loc(x)]['Dia'])
dfCALC['GL'] = dfCALC['chainage'].apply(lambda x : dfSOP.iloc[dfSOP.index.get_loc(x)]['GL'])
dfCALC['Rad'] = dfCALC['chainage'].apply(lambda x : dfSOP.iloc[dfSOP.index.get_loc(x)]['Rad'])
dfCALC['East_CP'] = dfCALC['chainage'].apply(lambda x : dfSOP.iloc[dfSOP.index.get_loc(x)]['East_CP'])
dfCALC['North_CP'] = dfCALC['chainage'].apply(lambda x : dfSOP.iloc[dfSOP.index.get_loc(x)]['North_CP'])
dfCALC['Side'] = dfCALC['chainage'].apply(lambda x : dfSOP.iloc[dfSOP.index.get_loc(x)]['Side'])
dfCALC['dist'] = dfCALC['chainage'].apply(lambda x : dfSOP.iloc[dfSOP.index.get_loc(x)]['dist'])
dfCALC['Bearing'] = dfCALC['chainage'].apply(lambda x : dfSOP.iloc[dfSOP.index.get_loc(x)]['Bearing'])
dfCALC['CirDis'] = dfCALC['chainage'].apply(lambda x : dfSOP.iloc[dfSOP.index.get_loc(x)]['CirDis'])
dfCALC['HorDif'] = dfCALC['chainage'].apply(lambda x : dfSOP.iloc[dfSOP.index.get_loc(x)]['HorDif'])
dfCALC['ELVDif'] = dfCALC['chainage'].apply(lambda x : dfSOP.iloc[dfSOP.index.get_loc(x)]['ELVDif'])
dfCALC['NDif'] = dfCALC['chainage'].apply(lambda x : dfSOP.iloc[dfSOP.index.get_loc(x)]['NDif'])
dfCALC['EDif'] = dfCALC['chainage'].apply(lambda x : dfSOP.iloc[dfSOP.index.get_loc(x)]['EDif'])
dfCALC['K'] = dfCALC['chainage'].apply(lambda x : dfSOP.iloc[dfSOP.index.get_loc(x)]['K'])
dfCALC['VL'] = dfCALC['chainage'].apply(lambda x : dfSOP.iloc[dfSOP.index.get_loc(x)]['VL'])

#distance of ref chainage from previous SOP
dfCALC['ch_DistSOP'] = dfCALC['chainage']-dfCALC['chainage_A']

#East dif from SOP
dfCALC['ch_EastDif'] = np.where(dfCALC['Bearing']<90.001,(np.sin(np.radians(dfCALC['Bearing']))*dfCALC['ch_DistSOP']),
                       np.where(dfCALC['Bearing']<180.001,(np.cos(np.radians(dfCALC['Bearing']-90))*dfCALC['ch_DistSOP']),
                       np.where(dfCALC['Bearing']<270.001,(np.sin(np.radians(dfCALC['Bearing']-180))*dfCALC['ch_DistSOP'])*-1,
                       np.where(dfCALC['Bearing']<361,(np.cos(np.radians(dfCALC['Bearing']-270))*dfCALC['ch_DistSOP'])*-1,""))))

#North dif from SOP
dfCALC['ch_NorthDif'] = np.where(dfCALC['Bearing']<90.001,(np.cos(np.radians(dfCALC['Bearing']))*dfCALC['ch_DistSOP']),
                        np.where(dfCALC['Bearing']<180.001,(np.sin(np.radians(dfCALC['Bearing']-90))*dfCALC['ch_DistSOP'])*-1,
                        np.where(dfCALC['Bearing']<270.001,(np.cos(np.radians(dfCALC['Bearing']-180))*dfCALC['ch_DistSOP'])*-1,
                        np.where(dfCALC['Bearing']<361,(np.sin(np.radians(dfCALC['Bearing']-270))*dfCALC['ch_DistSOP']),"")))) 

#angle between SOP points on a curve 
Rad = pd.to_numeric(dfCALC.Rad)
Dis1 = pd.to_numeric(dfCALC.HorDif)
Dis2 = pd.to_numeric(dfCALC.ch_DistSOP)

dfCALC['AngDifSOP'] = np.where(Rad=="",0,
                      np.degrees(np.arccos( ( (Rad**2)+(Rad**2)-(Dis2**2) ) / (2.0*Rad**2) ) ) )

dfCALC['Angle1'] = np.degrees(np.tan(Dis1/Rad))

dfCALC['Angle2'] = np.where(dfCALC['Side'] == "R",dfCALC.Bearing - 90 - (dfCALC.Angle1/2),
                   np.where(dfCALC['Side'] == "L",dfCALC.Bearing - 90 - (dfCALC.Angle1/2),""))

Angle2 = pd.to_numeric(dfCALC.Angle2)

dfCALC['Angle3'] = np.where(dfCALC['Side'] == "R",Angle2 + dfCALC.AngDifSOP,
                   np.where(dfCALC['Side'] == "L",Angle2 + dfCALC.Angle1 - dfCALC.AngDifSOP,""))

# x and y offset 
Angle3 = pd.to_numeric(dfCALC.Angle3)
dfCALC['CirXDif'] =  np.where(dfCALC['Side'] == "R",(Rad*(np.sin(np.radians(Angle3)))),
                     np.where(dfCALC['Side'] == "L",(Rad*(np.sin(np.radians(Angle3))))*-1,""))   
dfCALC['CirYDif'] =  np.where(dfCALC['Side'] == "R",(Rad*(np.cos(np.radians(Angle3)))),
                     np.where(dfCALC['Side'] == "L",(Rad*(np.cos(np.radians(Angle3))))*-1,""))

#Caclculate eastings and northings for points on the curve 
E = pd.to_numeric(dfCALC.East_CP)
N = pd.to_numeric(dfCALC.North_CP)
X = pd.to_numeric(dfCALC.CirXDif)
Y = pd.to_numeric(dfCALC.CirYDif)

dfCALC['CirEast'] = np.where(dfCALC['Side'] == "R", E + X,
                    np.where(dfCALC['Side'] == "L", E + X,""))
dfCALC['CirNorth'] = np.where(dfCALC['Side'] == "R", N + Y,
                     np.where(dfCALC['Side'] == "L", N + Y,""))

#final eastings and northings
East_A1 = pd.to_numeric(dfCALC['East_A'])
ch_EastDif1 = pd.to_numeric(dfCALC['ch_EastDif'])
Eastings = np.where(dfCALC['CirEast'] == "",ch_EastDif1 + East_A1, dfCALC['CirEast'] )
Eastings = pd.to_numeric(Eastings)
Eastings = np.round(Eastings, decimals=2)

North_A1 = pd.to_numeric(dfCALC['North_A'])
ch_NorthDif1 = pd.to_numeric(dfCALC['ch_NorthDif'])
Northings = np.where(dfCALC['CirNorth'] == "",ch_NorthDif1 + North_A1, dfCALC['CirNorth'] )
Northings = pd.to_numeric(Northings)
Northings = np.round(Northings, decimals=2)

dfCALC.insert(5,'Eastings',Eastings,True)
dfCALC.insert(6,'Northings',Northings,True)

#Settlement Module

# calculation for settlement based on defined contour SQRT(-1*(2*i^2)*LN(contour/SvMax))
# incline = np.degrees(np.arctan(dfCALC.ELVDif/dfCALC.HorDif))
# distance from SOP *(SIN(RADIANS(incline)))
#Z0 = dfCALC.GL - dfCALC.tunnel_axis
Elevation = dfCALC.Elev_A + (dfCALC.ch_DistSOP * (np.sin (np.radians (np.degrees (np.arctan (dfCALC.ELVDif / dfCALC.HorDif))))))
Elevation = pd.to_numeric(Elevation)
Elevation = np.round(Elevation, decimals=2)

Z0 = dfCALC.GL - Elevation
i = abs(Z0) * dfCALC.K

#SvMax +(Vl*PI()*(Dia/2)^2)/(i*SQRT(2*PI()))
SvMax = (abs(dfCALC.VL*np.pi)*((dfCALC.Dia/2)**2))/(i*np.sqrt(2*np.pi))

dfCALC.insert(7,'Elevation',Elevation,True)
dfCALC.insert(8,'Z0',Z0,True)
dfCALC.insert(9,'i',i,True)
dfCALC.insert(10,'SvMax',SvMax,True)

#buffer around tunnel 
Buff = 50

#define size of numpy array (round coords to 2 decimal places (mm) and convert to mm)
XMax = (np.round(np.max(dfCALC.Eastings) - np.min(dfCALC.Eastings), decimals=2) + (2*Buff)) 
YMax = (np.round(np.max(dfCALC.Northings) - np.min(dfCALC.Northings), decimals=2) + (2*Buff)) 
ZMax = (np.round(np.max(dfCALC.GL) - np.min(dfCALC.Elevation), decimals=2) + Buff) 
#xyz index location for dfCALC dataframe
dfCALC['XIndex'] = ((dfCALC.Eastings) - np.min(dfCALC.Eastings) + Buff) 
dfCALC['YIndex'] = ((dfCALC.Northings) - np.min(dfCALC.Northings) + Buff) 
dfCALC['ZIndex'] = ((dfCALC.Elevation) - np.min(dfCALC.Elevation) + Buff)

XMax = XMax.round(0).astype(int)
YMax = YMax.round(0).astype(int)

#create array to encompas the data points 
arr = np.empty((XMax,YMax))
arr[:] = 1
#create array of x,y coords
arrXY = np.where(arr[:])

#create pandas dataframe of the XY coords from dataframe, issue that this results in massive dataframe that results in memory crash 
dfXY = pd.DataFrame(arrXY)
dfXY.T
dfXY = pd.DataFrame(arrXY)
dfXY = dfXY.T
dfXY.columns = ['X','Y']

from scipy.spatial.distance import cdist
dfSett = pd.DataFrame(data = cdist(dfXY[["X", "Y"]], dfCALC[["XIndex", "YIndex"]], 'euclidean'), 
                      index = pd.MultiIndex.from_frame(dfXY[["X","Y"]]),
                      columns = [f"Point{i+1}" for i in range(len(dfCALC.index))]).reset_index()

dfSettTemp = dfSett.drop(columns = ['X','Y'], axis = 1)
dfSett['Dist'] = dfSettTemp.min(axis = 1)
arrSett = np.zeros((dfSett['X'].max() + 1, dfSett['Y'].max() + 1))
arrSett[dfSett.X,dfSett.Y] = dfSett.Dist
arrSett[dfSett.X,dfSett.Y] = 0.08 * np.exp( -(dfSett.Dist**2)/(2 * 7.9**2 )) 


fig = plt.figure(figsize = (6, 3.2))
ax = fig.add_subplot(111)
ax.set_title('colorMap')
plt.imshow(arrSett)
ax.set_aspect('equal')
cax = fig.add_axes([0.12, 0.1, 0.78, 0.8])
cax.get_xaxis().set_visible(False)
cax.get_yaxis().set_visible(False)
cax.patch.set_alpha(0)
cax.set_frame_on(False)
plt.colorbar(orientation='vertical')
plt.show()
#retun x,y coords then determine dustance to closest alignment coords and use that to calculate settlement 

print(dfCALC)