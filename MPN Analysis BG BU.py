# MPN-Analysis-BG-BU-Level
import pandas as pd

output_path = input('Please type in the desired file path to store exported data:')
print('')
output_path = output_path.split('\\')
for i in range(len(output_path)):
    if i == 0:
        new_path = output_path[i] + '\\\\'
    elif i != (len(output_path)-1) and i != 0:
        new_path += output_path[i] + '\\\\'
    else:
        new_path += output_path[i]
output_path = new_path

path = input('Please paste the path of files above:')

# 調整資料路徑名稱
path = path.split('\\')
for i in range(len(path)):
    if i == 0:
        new_path = path[i] + '\\\\'
    elif i != (len(path)-1) and i != 0:
        new_path += path[i] + '\\\\'
    else:
        new_path += path[i]
path = new_path

# 資料表代號
g_cus_fcst = pd.read_excel(path + '\\G_FcstTmp_export.xlsx') # G 次 Customer Fcst
f_cus_fcst = pd.read_excel(path + '\\F_FcstTmp_export.xlsx') # F 次 Customer Fcst
g_p830 = pd.read_excel(path + '\\G_P830_CUS.xlsx') # 上周 G 次 Customer Fcst 
f_p830 = pd.read_excel(path + '\\F_P830_CUS.xlsx') # 上周 F 次 Customer Fcst
sloc = pd.read_excel(path + '\\SLOC.xlsx') # 本周 SLOC 對照表
g_830r = pd.read_excel(path + '\\G_830R.xlsx') # 本周 G 次 830R
f_830r = pd.read_excel(path + '\\F_830R.xlsx') # 本周 F 次 830R
g_830r_p = pd.read_excel(path + '\\G_830R_P.xlsx') # 上周 G 次 830R
f_830r_p = pd.read_excel(path + '\\F_830R_P.xlsx') # 上周 F 次 830R

# 資料疊合，欄位名稱調整
cus_fcst = g_cus_fcst.append(f_cus_fcst, ignore_index = True)
p830 = g_p830.append(f_p830,ignore_index = True)
c_new_indx = dict() 
for item in cus_fcst.keys():
    try:
        tmp = int(item[0])
        indx = item.split('-')
        c_new_indx[item] = 'N' + indx[0] + '_' + indx[1] + '_' + indx[2]
    except:
        pass


p_new_indx = dict()
for item in p830.keys():
    try:
        tmp = int(item[0])
        indx = item.split('-')
        p_new_indx[item] = 'N' + indx[0] + '_' + indx[1] + '_' + indx[2]
    except:
        pass
r830_p = pd.concat([g_830r_p,f_830r_p],ignore_index = True)
r830_p.rename(columns = {'BuyerName':'CSR','SUM FCST':'SUM'},inplace=True)
r830_p.rename(columns = p_new_indx,inplace = True)

r830 = pd.concat([g_830r,f_830r],ignore_index = True)
r830.rename(columns = {'BuyerName':'CSR','SUM FCST':'SUM'},inplace=True)
r830.rename(columns = c_new_indx,inplace = True)

p830.rename(columns = p_new_indx,inplace = True)
cus_fcst.rename(columns = c_new_indx, inplace = True)
cus_fcst.rename(columns = {'APN/CPN': 'CPN', 'On-Hand INV': 'INV', 'Plant Site': 'PLANT'}, inplace = True)
p830.rename(columns = {'APN/CPN': 'CPN','SUM FCST':'SUM_FCST','On-Hand Inv':'INV'},inplace = True)

# MPN analysis in BG level
mpnBG = cus_fcst.copy()
drop_bg = []
for key in mpnBG.keys():
    if key != 'BG' and key != 'MPN' and key[0] != 'N':
        drop_bg.append(key)
for item in drop_bg:
    del mpnBG[item]

mpnBG = mpnBG.groupby(['MPN','BG']).sum().reset_index().sort_values(by=['MPN','BG'])

mpnBG_p = p830.copy()
drop_bg = []
for key in mpnBG_p.keys():
    if key != 'BG' and key != 'MPN' and key[0] != 'N':
        drop_bg.append(key)
for item in drop_bg:
    del mpnBG_p[item]

mpnBG_p = mpnBG_p.groupby(['MPN','BG']).sum().reset_index().sort_values(by=['MPN','BG'])

c_keys = []
for key in mpnBG.keys():
    if key[0] == 'N':
        c_keys.append(key)
p_keys = []
for key in mpnBG_p.keys():
    if key[0] == 'N':
        p_keys.append(key)
    
del mpnBG[c_keys[-1]] # 本周 Customer Fcst 取前 51 周
del mpnBG_p[p_keys[0]] # 上周 Customer Fcst 取後 51 周
c_keys.pop(-1)
p_keys.pop(0)

mpnBG['C_DEMAND'] = 0 # 本周 Customer Fcst 前 51 周總和
for key in c_keys:
    mpnBG['C_DEMAND'] += mpnBG[key]

mpnBG_p['P_DEMAND'] = 0 # 上周 Customer Fcst 後 51 周總和
for key in p_keys:
    mpnBG_p['P_DEMAND'] += mpnBG_p[key]
    
for key in c_keys:
    del mpnBG[key]
    del mpnBG_p[key]
    
mpnBG = pd.merge(mpnBG, mpnBG_p, how='inner',on = ['MPN','BG'])
mpnBG['DIF'] = mpnBG['C_DEMAND']-mpnBG['P_DEMAND']
mpnBG.sort_values(by = 'C_DEMAND', inplace = True, ascending = False) # 依照本周 Customer Fcst 前 51 周總和由大到小排序

## 分析新增以及流失的 MPN, BG, CSR(IS) 組合
cus_fcst = pd.merge(cus_fcst, sloc, how = 'left', on = 'SLOC')
cus_fcst['CSR'] = cus_fcst['CSR'].astype(str)
for i in range(len(cus_fcst)):
    if cus_fcst['CSR'][i] == 'nan':
        cus_fcst['CSR'][i] = cus_fcst['SLOC'][i].split('-')[0]
        
p830 = pd.merge(p830, sloc, how = 'left', on = 'SLOC')
p830['CSR'] = p830['CSR'].astype(str)
for i in range(len(p830)):
    if p830['CSR'][i] == 'nan':
        p830['CSR'][i] = p830['SLOC'][i].split('-')[0]

c_match = [] # 本周 MPN, BG, CSR(IS) 組合
for i in range(len(cus_fcst)):
    c_match.append((cus_fcst['MPN'][i], cus_fcst['BG'][i], cus_fcst['CSR'][i]))
c_match = list(set(c_match))
p_match = [] # 上周 MPN, BG, CSR(IS) 組合
for i in range(len(p830)):
    p_match.append((p830['MPN'][i], p830['BG'][i], p830['CSR'][i]))
p_match = list(set(p_match))

    
new = []
for match in c_match:
    if match not in p_match:
        new.append(match)
loss = []
for match in p_match:
    if match not in c_match:
        loss.append(match)
        
new = pd.DataFrame(new, columns = ['MPN','BG','CSR'])
loss = pd.DataFrame(loss, columns = ['MPN','BG','CSR'])

lossAMT = p830.copy()
lossAMT = pd.merge(lossAMT, loss, how = 'inner', on = ['MPN','BG','CSR'])
for key in lossAMT.keys():
    if key[0] != 'N':
        if key not in ['MPN','BG','CSR']:
            del lossAMT[key]
loss = lossAMT.groupby(['MPN','BG','CSR']).sum().reset_index().sort_values(by = ['MPN','BG','CSR'])
loss['P_DEMAND'] = 0 # 本州流失的 MPN 的上周 Customer Fcst 後 51 周總和
for key in p_keys:
    loss['P_DEMAND'] += loss[key]

for key in loss.keys():
    if key not in ['MPN','BG','CSR','P_DEMAND']:
        del loss[key]
loss.sort_values(by='P_DEMAND', inplace = True, ascending = False) # 依照上周 Customer Fcst 後 51 周總和由大到小排序
loss.drop(loss[loss['P_DEMAND'] == 0].index, inplace = True) # 刪除上周 Customer Fcst 後 51 周總和為零的 MPN

# MPN analysis in BU level
mpnBU = cus_fcst.copy()
drop_bu = []
for key in mpnBU.keys():
    if key != 'BU' and key != 'MPN' and key != 'BG' and key[0] != 'N':
        drop_bu.append(key)
for item in drop_bu:
    del mpnBU[item]

mpnBU = mpnBU.groupby(['MPN','BG', 'BU']).sum().reset_index().sort_values(by=['MPN','BG','BU'])

mpnBU_p = p830.copy()
drop_bu = []
for key in mpnBU_p.keys():
    if key != 'BU' and key != 'MPN' and key != 'BG' and key[0] != 'N':
        drop_bu.append(key)
for item in drop_bu:
    del mpnBU_p[item]

mpnBU_p = mpnBU_p.groupby(['MPN','BG','BU']).sum().reset_index().sort_values(by=['MPN','BG','BU'])


c_keys = []
for key in mpnBU.keys():
    if key[0] == 'N':
        c_keys.append(key)
p_keys = []
for key in mpnBU_p.keys():
    if key[0] == 'N':
        p_keys.append(key)
    
del mpnBU[c_keys[-1]] # 本周 Customer Fcst 取前 51 周
del mpnBU_p[p_keys[0]] # 上周 Customer Fcst 取後 51 周
c_keys.pop(-1)
p_keys.pop(0)

mpnBU['C_DEMAND'] = 0 # 本周 Customer Fcst 前 51 周總和
for key in c_keys:
    mpnBU['C_DEMAND'] += mpnBU[key]

mpnBU_p['P_DEMAND'] = 0 # 上周 Customer Fcst 後 51 周總和
for key in p_keys:
    mpnBU_p['P_DEMAND'] += mpnBU_p[key]
    
for key in c_keys:
    del mpnBU[key]
    del mpnBU_p[key]
    
mpnBU = pd.merge(mpnBU, mpnBU_p, how='inner',on = ['MPN','BG','BU'])
mpnBU['DIF'] = mpnBU['C_DEMAND']-mpnBU['P_DEMAND']
mpnBU.sort_values(by=['C_DEMAND','BG', 'BU'], inplace = True, ascending = False) # 依照本周 Customer Fcst 前 51 周總和、BG、BU 由大到小排序

## 分析新增以及流失的 MPN, BU, CSR(IS) 組合
c_match = []
for i in range(len(cus_fcst)):
    c_match.append((cus_fcst['MPN'][i],cus_fcst['BU'][i],cus_fcst['CSR'][i]))
c_match = list(set(c_match))
p_match = []
for i in range(len(p830)):
    p_match.append((p830['MPN'][i],p830['BU'][i],p830['CSR'][i]))
p_match = list(set(p_match))

new_bu = []
for match in c_match:
    if match not in p_match:
        new_bu.append(match)
loss_bu = []
for match in p_match:
    if match not in c_match:
        loss_bu.append(match)
        
new_bu = pd.DataFrame(new_bu, columns = ['MPN','BU','CSR'])
loss_bu = pd.DataFrame(loss_bu, columns = ['MPN','BU','CSR'])

lossBUAMT = p830.copy()
lossBUAMT = pd.merge(lossBUAMT, loss_bu, how = 'inner', on = ['MPN','BU','CSR'])
for key in lossBUAMT.keys():
    if key[0] != 'N':
        if key not in ['MPN','BU','CSR']:
            del lossBUAMT[key]
loss_bu = lossBUAMT.groupby(['MPN','BU','CSR']).sum().reset_index().sort_values(by = ['MPN','BU','CSR'])
loss_bu['P_DEMAND'] = 0
for key in p_keys:
    loss_bu['P_DEMAND'] += loss_bu[key]

for key in loss_bu.keys():
    if key not in ['MPN','BU','CSR','P_DEMAND']:
        del loss_bu[key]
        
loss_bu.sort_values(by='P_DEMAND', inplace = True, ascending = False) # 依照上周 Customer Fcst 後 51 周總和由大到小排序
loss_bu.drop(loss_bu[loss_bu['P_DEMAND'] == 0].index, inplace = True) # 刪除上周 Customer Fcst 後 51 周總和為零的 MPN

# MPN, CPN amount of IS

from pandasql import sqldf
sql = lambda q: sqldf(q, globals())

isBase = cus_fcst.copy()
isBase_p = p830.copy()

q = 'SELECT DISTINCT CSR, COUNT(DISTINCT MPN) AS MPN_AMT, COUNT(DISTINCT CPN) AS CPN_AMT FROM isBase GROUP BY CSR;'
c_amt = sql(q) # 本周 MPN, CPN 數量
q = 'SELECT DISTINCT CSR, COUNT(DISTINCT MPN) AS MPN_AMT, COUNT(DISTINCT CPN) AS CPN_AMT FROM isBase_p GROUP BY CSR;'
p_amt = sql(q) # 上周 MPN, CPN 數量

q = 'SELECT c_amt.CSR,c_amt.MPN_AMT, c_amt.MPN_AMT - p_amt.MPN_AMT AS MPN_AMT_DIF, c_amt.CPN_AMT, c_amt.CPN_AMT - p_amt.CPN_AMT AS CPN_AMT_DIF FROM c_amt INNER JOIN p_amt ON c_amt.CSR = p_amt.CSR GROUP BY c_amt.CSR;'
amt = sql(q)
amt.sort_values(by=['MPN_AMT','CPN_AMT'],inplace = True, ascending = False) # 依據 MPN, CPN 數量由大到小排序 CSR(IS)
amt.reset_index(inplace = True)
del amt['index']
amt['RANK'] = amt.index + 1
q = 'select RANK, CSR, MPN_AMT, MPN_AMT_DIF, CPN_AMT, CPN_AMT_DIF from amt;'
amt = sql(q)

## 計算 CSR(IS) 業績差額
for key in isBase.keys():
    if key != 'CSR' and key[0] != 'N':
        del isBase[key]

isBase = isBase.groupby('CSR').sum().reset_index()


for key in isBase_p.keys():
    if key != 'CSR' and key[0] != 'N':
        del isBase_p[key]
        
isBase_p = isBase_p.groupby('CSR').sum().reset_index()

isBase['C_DEMAND'] = 0 # CSR(IS) 本周 Customer Fcst 前 51 周總和
for key in c_keys:
    isBase['C_DEMAND'] += isBase[key]
isBase_p['P_DEMAND'] = 0 # CSR(IS) 上周 Customer Fcst 後 51 周總和
for key in p_keys:
    isBase_p['P_DEMAND'] += isBase_p[key]
    
for key in isBase.keys():
    if key != 'CSR' and key != 'C_DEMAND':
        del isBase[key]
for key in isBase_p.keys():
    if key != 'CSR' and key != 'P_DEMAND':
        del isBase_p[key]
    
isBase = pd.merge(isBase,isBase_p,how='inner',on='CSR')
isBase['DIF'] = isBase['C_DEMAND'] - isBase['P_DEMAND']
isBase.sort_values(by='DIF', ascending = True, inplace = True) # 依據 CSR(IS) 之兩周需求總和之差額由小到大排序，負得越多排越前面
isBase.reset_index(inplace = True)
del isBase['index']
isBase['DIF_RANK'] = isBase.index + 1
q = 'select DIF_RANK, CSR, C_DEMAND, P_DEMAND, DIF from isBase;'
isBase = sql(q)


# BG supply analysis

s_mpnBG = r830.copy()
for key in s_mpnBG.keys():
    if key != 'MPN' and key != 'BG' and key[0] != 'N':
        del s_mpnBG[key]
        
s_mpnBG = s_mpnBG.groupby(['MPN','BG']).sum().reset_index()

s_mpnBG['C_SUPPLY'] = 0 # 本周 830R 前 51 周總和
for key in c_keys:
    s_mpnBG['C_SUPPLY'] += s_mpnBG[key]
    
    
s_mpnBG_p = r830_p.copy()
for key in s_mpnBG_p.keys():
    if key != 'MPN' and key != 'BG' and key[0] != 'N':
        del s_mpnBG_p[key]
        
s_mpnBG_p = s_mpnBG_p.groupby(['MPN','BG']).sum().reset_index()

s_mpnBG_p['P_SUPPLY'] = 0 # 上周 830R 後 51 周總和
for key in p_keys:
    s_mpnBG_p['P_SUPPLY'] += s_mpnBG_p[key]
    
for key in s_mpnBG.keys():
    if key not in ['MPN','BG','C_SUPPLY']:
        del s_mpnBG[key]
        
for key in s_mpnBG_p.keys():
    if key not in ['MPN','BG','P_SUPPLY']:
        del s_mpnBG_p[key]
        
s_mpnBG = pd.merge(s_mpnBG,s_mpnBG_p,how='inner',on=['MPN','BG'])
s_mpnBG['DIF'] = s_mpnBG['C_SUPPLY']-s_mpnBG['P_SUPPLY']
s_mpnBG.sort_values(by = 'C_SUPPLY', ascending = False, inplace = True) # 依據本周 830R 前 51 周總和由大到小排序


# BU supply analysis
s_mpnBU = r830.copy()
for key in s_mpnBU.keys():
    if key != 'MPN' and key != 'BU' and key != 'BG' and key[0] != 'N':
        del s_mpnBU[key]
        
s_mpnBU = s_mpnBU.groupby(['MPN','BG', 'BU']).sum().reset_index()

s_mpnBU['C_SUPPLY'] = 0
for key in c_keys:
    s_mpnBU['C_SUPPLY'] += s_mpnBU[key]
    
    
s_mpnBU_p = r830_p.copy()
for key in s_mpnBU_p.keys():
    if key != 'MPN' and key != 'BU' and key != 'BG' and key[0] != 'N':
        del s_mpnBU_p[key]
        
s_mpnBU_p = s_mpnBU_p.groupby(['MPN','BG','BU']).sum().reset_index()

s_mpnBU_p['P_SUPPLY'] = 0
for key in p_keys:
    s_mpnBU_p['P_SUPPLY'] += s_mpnBU_p[key]
    
    
for key in s_mpnBU.keys():
    if key not in ['MPN','BG','BU','C_SUPPLY']:
        del s_mpnBU[key]
        
for key in s_mpnBU_p.keys():
    if key not in ['MPN','BG','BU','P_SUPPLY']:
        del s_mpnBU_p[key]
        
s_mpnBU = pd.merge(s_mpnBU,s_mpnBU_p,how='inner',on=['MPN','BG','BU'])
s_mpnBU['DIF'] = s_mpnBU['C_SUPPLY']-s_mpnBU['P_SUPPLY']
s_mpnBU.sort_values(by=['C_SUPPLY','BG','BU'], ascending = False, inplace = True)


# BG 供給需求差額分析
tmp1 = mpnBG.copy()
tmp2 = s_mpnBG.copy()
dsBG = pd.merge(tmp1,tmp2,how='inner',on=['MPN','BG'])
dsBG.rename(columns={'DIF_x':'DEMAND_DIF','DIF_y':'SUPPLY_DIF'},inplace = True)
dsBG['C_DS_GAP'] = dsBG['C_SUPPLY'] - dsBG['C_DEMAND']
dsBG['P_DS_GAP'] = dsBG['P_SUPPLY'] - dsBG['P_DEMAND']
dsBG.sort_values(by='C_DEMAND',inplace = True, ascending = False) # 依據本周 Customer Fcst 前 51 周總和

# BU 供給需求差額分析
tmp1 = mpnBU.copy()
tmp2 = s_mpnBU.copy()
dsBU = pd.merge(tmp1,tmp2,how='inner',on=['MPN','BG','BU'])
dsBU.rename(columns={'DIF_x':'DEMAND_DIF','DIF_y':'SUPPLY_DIF'},inplace = True)
dsBU['C_DS_GAP'] = dsBU['C_SUPPLY'] - dsBU['C_DEMAND']
dsBU['P_DS_GAP'] = dsBU['P_SUPPLY'] - dsBU['P_DEMAND']
dsBU.sort_values(by=['BG','BU','MPN'],inplace = True)

q = 'select BG, BU, MPN, C_DEMAND, P_DEMAND, DEMAND_DIF, C_SUPPLY, P_SUPPLY, SUPPLY_DIF, C_DS_GAP, P_DS_GAP from dsBU;'
dsBU = sql(q)

bg_list = sorted(list(set(dsBU['BG']))) # 排序順序:  BG → BU → MPN 依據本周 Customer Fcst 前 51 周總和由大到小排序
dsBU_af = pd.DataFrame()
for bg in bg_list:
    bu_list = sorted(list(set(dsBU[dsBU['BG']==bg]['BU'])))
    for bu in bu_list:
        df = dsBU[(dsBU['BG'] == bg)&(dsBU['BU'] == bu)]
        dsBU.drop(dsBU[(dsBU['BG'] == bg)&(dsBU['BU'] == bu)].index, inplace = True)
        df.sort_values(by='C_DEMAND', inplace = True, ascending = False)
        dsBU_af = pd.concat([dsBU_af, df], ignore_index = True)

# BG summary 
summaryBG = dsBG.copy()
summaryBG = summaryBG.groupby('BG').sum().reset_index() # 依據 BG 加總
summaryBG.sort_values(by='C_DEMAND',inplace = True, ascending = False) # 依據本周 Customer Fcst 前 51 周總和由大到小排序
q = 'SELECT BG, C_DEMAND, C_SUPPLY, C_DS_GAP, C_DS_GAP/C_DEMAND AS C_GAP_PCT,   P_DEMAND, P_SUPPLY, P_DS_GAP, P_DS_GAP/P_DEMAND AS P_GAP_PCT FROM summaryBG GROUP BY BG;'
summaryBG = sql(q)

# BU SUMMARY
summaryBU = dsBU_af.copy()
summaryBU = summaryBU.groupby(['BG','BU']).sum().reset_index()
summaryBU.sort_values(by='BG',inplace = True)
summaryBU_af = pd.DataFrame()
bg_list = sorted(list(set(summaryBU['BG'])))
for bg in bg_list: # 排序順序:  BG → BU → MPN 依據本周 Customer Fcst 前 51 周總和由大到小排序
    bu_list = sorted(list(set(summaryBU[summaryBU['BG'] == bg]['BU'])))
    for bu in bu_list:
        df = summaryBU[(summaryBU['BG'] == bg)&(summaryBU['BU'] == bu)]
        summaryBU.drop(summaryBU[(summaryBU['BG'] == bg)&(summaryBU['BU'] == bu)].index, inplace = True)
        df.sort_values(by='C_DEMAND',inplace = True, ascending=False)
        summaryBU_af = pd.concat([summaryBU_af, df], ignore_index = True)

        
# 輸出
new.to_excel(output_path+'\\new_mpnBG.xlsx',index = False)
loss.to_excel(output_path+'\\loss_mpnBG.xlsx',index = False)

new_bu.to_excel(output_path+'\\new_mpnBU.xlsx',index = False)
loss_bu.to_excel(output_path+'\\loss_mpnBU.xlsx',index = False)

amt.to_excel(output_path+'\\IS_mpnAMT.xlsx',index = False)
isBase.to_excel(output_path+'\\isBase.xlsx',index = False)

dsBG.to_excel(output_path+'\\Demand&Supply_BG.xlsx',index = False)
dsBU_af.to_excel(output_path+'\\Demand&Supply_BU.xlsx',index = False)

summaryBG.to_excel(output_path+'\\SummaryBG.xlsx',index = False)
summaryBU_af.to_excel(output_path+'\\SummaryBU.xlsx', index = False)
