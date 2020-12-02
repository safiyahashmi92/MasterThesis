!pip install wfdb
import pandas as pd
from IPython.display import display
import matplotlib.pyplot as plt
%matplotlib inline
import numpy as np
import wfdb

signals,fields = wfdb.rdsamp( 'p042930-2190-07-28-20-30n',
							pn_dir='mimic3wdb/matched/p04/p042930/', 
							channel_names=['HR','ABP MEAN', 'ABP SYS','ABP DIAS'], 
							sampfrom=100, 
							sampto=120 )

wfdb.plot_items(signal=signals, fs=fields['fs'])

print('Printing signals')
display(signals)
print('Printing fields')
display(fields)

