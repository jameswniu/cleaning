import os
import time
import pandas as pd

from nltk.tokenize import word_tokenize


timestr = time.strftime("%Y%m%d_%H%M%S")
minutestr = timestr[:-2]

os.chdir('L:\\Auto_Opportunity_Analysis\\MLX_Cust_ID_538_Unbilled_Claims\\Report')
df1 = pd.read_excel('MLX_Cust_ID_538_Unbilled_Claims_20200428_v3.xlsx', sheet_name=0)
df1 = df1[['insurance_name']]
print(df1.head())

os.chdir('L:\\Auto_Opportunity_Analysis\\MLX_Decision_Management\\Jopari_Payer_List')
df2 = pd.read_csv('jopari_list_John.csv')
df2.columns = df2.columns.str.replace(' ', '_')
df2 = df2[['Payer_ID']].join(df2['Payer_Name'].str.upper())
print(df2.head())

########################################################################################################################

stop_list = word_tokenize(str.upper(
    """! @ # $ % ^ & * ( ) - _ + = { [ ] } \ | ' " ; : / ? . , > < ` ~  b c d e f g h i j k l n o p q r s t u v w \
    x y z 1 2 3 4 5 6 7 8 9 0 auto wc only n a except  district program part supp isd '' \
    construction homes mt or wa Â â € ™ s commercial vehicles acia association n.a. n.y. al	ak	az	ar	ca	co	ct	\
    de	fl	ga	hi	id	il	in	ia	ks	ky	la	me	md	ma	mi	mn	ms	mo	mt	ne	nv	nh	nj	nm	ny	nc	nd	oh	\
    ok	or	pa	ri	sc	sd	tn	tx	ut	vt	va	wa	wv	wi	wy and 's 51 12 cms for van tuyl companies sig pih\
    205 206 rem sse tab â € ™ “ work worker workers comp comps compensation compensations  \
    mobile motive first but this route is argent liab liability re canada cn c.n cn. usa u.s.a usa. us u.s. \
    us. of ohio wisconsin new york texas 1/1/2013 12/31/2017 dates doi before 7/1/2014 1850 njpliga jif paic corvel\
    exchange ins co corp company grp assoc  '' ''  insurance"""))
# other possible stopwords: pte llc inc ltd co automobile automotive a m casualty usaa benchmark

df1.insert(1, 'insurance_name_abbv', df1['insurance_name'].apply(lambda x: word_tokenize(x)))
df1['insurance_name_abbv'] = df1['insurance_name_abbv'].apply(lambda x: [word for word in x if word not in stop_list])
df1['insurance_name_abbv'] = df1['insurance_name_abbv'].apply(lambda x: (' ').join(x))
df1 = df1.sort_values(by='insurance_name_abbv', ascending=True, na_position='last')
print(df1.head())

df2.insert(2, 'payer_name_abbv', df2['Payer_Name'].apply(lambda x: word_tokenize(x)))
df2['payer_name_abbv'] = df2['payer_name_abbv'].apply(lambda x: [word for word in x if word not in stop_list])
df2['payer_name_abbv'] = df2['payer_name_abbv'].apply(lambda x: (' ').join(x))
df2 = df2.sort_values(by='payer_name_abbv', ascending=True, na_position='last')
cols = df2.columns.tolist()
cols = cols[1:] + cols[0:1]
df2 = df2[cols]
print(df2.head())

########################################################################################################################

os.chdir('L:\\Auto_Opportunity_Analysis\\MLX_Cust_ID_538_Unbilled_Claims\\Report')
with pd.ExcelWriter('Cust_ID_538_EDI_Carrier_Check_' + minutestr + '.xlsx') as writer:
    df1.to_excel(writer, sheet_name='set_carrier_info', index=False)
    df2.to_excel(writer, sheet_name='edi_carrier_info', index=False)