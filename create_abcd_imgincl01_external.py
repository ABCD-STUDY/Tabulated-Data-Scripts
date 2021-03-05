#!/usr/bin/env python
# coding: utf-8

# In[1]:


import time
import os
import sys
from sqlalchemy import create_engine
import pandas as pd
import sqlalchemy
import numpy as np
import argparse

import warnings
warnings.filterwarnings("ignore")


# In[2]:


parser = argparse.ArgumentParser()

parser.add_argument('-p', '--path', help='Only neccessary when mode is txt. Default to current directory. /Path/to/csv/files/', default='.')
parser.add_argument('-o', '--output', help='Path to save abcd_imgincl01 file. /Path/to/save/file/', default='.')
parser.add_argument('-v', '--verbose', help='Verbosity. Default to False', action='store_true')


# In[3]:


args = parser.parse_args()

verbose = args.verbose
path = args.path
output_path = os.path.join(args.output, 'abcd_imgincl01.csv')

if 'abcd_mri01.txt' not in os.listdir(path):
    print('The directory does not contain needed txt files')
    print('Set the directory by -p /path/to/txt/files')
    sys.exit()


# ## Loading necessary tables

# In[4]:


tables = [('mriqcrp102', ['src_subject_id', 'eventname', 'iqc_t1_ok_ser', 'iqc_t2_ok_ser', 'iqc_dmri_ok_ser', 'iqc_dmri_ok_nreps', 'iqc_rsfmri_ok_ser']),
          ('mriqcrp202', ['src_subject_id', 'eventname', 'iqc_mid_ok_ser', 'iqc_nback_ok_ser', 'iqc_sst_ok_ser']), 
          ('mriqcrp302', ['src_subject_id', 'eventname', 'iqc_mid_ep_t_series_match', 'eprime_mismatch_ok_mid', 'iqc_nback_ep_t_series_match', 'eprime_mismatch_ok_nback', 'iqc_sst_ep_t_series_match', 'eprime_mismatch_ok_sst']), 
          ('freesqc01', ['src_subject_id', 'eventname', 'fsqc_qc']), 
          ('dmriqc01', ['src_subject_id', 'eventname', 'dmri_dti_postqc_visitid', 'dmri_dti_postqc_qc']), 
          ('abcd_mid02', ['src_subject_id', 'eventname', 'tfmri_mid_beh_performflag', 'tfmri_mid_all_beh_t_nt']), 
          ('midaparc03', ['src_subject_id', 'eventname', 'tfmri_mid_all_b_dof', 'tfmri_ma_acdn_b_scs_cbwmlh']),
          ('abcd_mrinback02', ['src_subject_id', 'eventname', 'tfmri_nback_beh_performflag']), 
          ('abcd_sst02', ['src_subject_id', 'eventname', 'tfmri_sst_beh_performflag', 'tfmri_sst_beh_glitchflag']), 
          ('fmriqc01', ['src_subject_id', 'eventname', 'fmri_postqc_qc']), 
          ('abcd_betnet02', ['src_subject_id', 'eventname', 'rsfmri_c_ngd_ntpoints', 'rsfmri_c_ngd_dt_ngd_sa']), 
          ('nback_bwroi02', ['src_subject_id', 'eventname', 'tfmri_nback_all_beta_dof', 'tfmri_nback_all_4']), 
          ('mrisst02', ['src_subject_id', 'eventname', 'tfmri_sa_beta_dof', 'tfmri_sacgvf_bscs_cbwmlh']), 
          ('abcd_auto_postqc01', ['src_subject_id', 'eventname', 'apqc_dmri_regt1_rigid', 'apqc_dmri_bounwarp_flag', 'apqc_dmri_fov_cutoff_dorsal', 
                                  'apqc_dmri_fov_cutoff_ventral', 'apqc_fmri_bounwarp_flag', 'apqc_smri_t2w_regt1_rigid', 'apqc_fmri_regt1_rigid', 
                                  'apqc_fmri_fov_cutoff_dorsal', 'apqc_fmri_fov_cutoff_ventral']),

          ('abcd_smrip201', ['src_subject_id', 'eventname', 'smri_t1w_scs_cbwmatterlh', 'smri_t2w_scs_cbwmatterlh']), 
          ('mri_rsi_p102', ['src_subject_id', 'eventname', 'dmri_rsind_fiberat_allfibers']), 
          ('abcd_mrfindings02', ['src_subject_id', 'eventname', 'mrif_score']),
          ('abcd_mri01', ['src_subject_id', 'eventname', 'mri_info_visitid', 'mri_info_manufacturer'])]

all_tables = {}

for table, columns in tables:

    df_path = os.path.join(path, table + '.txt')

    if verbose:

        print('Loading', df_path, columns)

    df = pd.read_csv(df_path, delim_whitespace=True)
    df = df.iloc[1: ]
    df = df.drop(columns=['subjectkey', 'interview_age', 'interview_date', 'sex'])

    df = df[columns]
    all_tables[table] = df


# ### General Inclusion Criteria (Recommended from Confluence page)
# Apply mrfindings to all modality
# 1. No serious MR Findings. abcd_mrfindings02:mrif_score!=3 && abcd_mrfindings01:mrif_score!=4 (Currently not added)

# In[5]:


on = ['src_subject_id', 'eventname']

mri = all_tables['abcd_mri01']

mrfindings = all_tables['abcd_mrfindings02'].copy()
mrfindings = mrfindings[['src_subject_id', 'eventname', 'mrif_score']]
mrfindings['mrif_score'] = pd.to_numeric(mrfindings['mrif_score'])

incl = mri.merge(mrfindings, how='left', on=['src_subject_id', 'eventname'])
incl = incl[['src_subject_id', 'eventname', 'mrif_score', 'mri_info_manufacturer', 'mri_info_visitid']]
    
incl = incl.rename(columns={'mri_info_visitid':'VisitID'})

# Re-order index
incl = incl.reset_index(drop=True).sort_values('VisitID')

incl.shape


# ### imgincl_t1w_include
# 
# 1. T1 series passed rawQC. mriqcrp102:iqc_t1_ok_ser>0
# 2. FreeSurfer QC not failed. freesqc01:fsqc_qc ~= 0
# 3. abcd_smrip201:smri_t1w_scs_cbwmatterlh is not NA

# In[6]:


df = incl.merge(all_tables['mriqcrp102'], how='left', on=on)         .merge(all_tables['freesqc01'], how='left', on=on)         .merge(all_tables['abcd_smrip201'], how='left', on=on)

# Convert columns to numeric
df[df.columns] = df[df.columns].apply(pd.to_numeric, errors='ignore')
    
df['imgincl_t1w_include'] = 0

df.loc[(df['iqc_t1_ok_ser']>0) &
       (df['fsqc_qc']!=0) &
       (~df['smri_t1w_scs_cbwmatterlh'].isna()), 'imgincl_t1w_include'] = 1
df.shape


# ### imgincl_t2w_include
# 1. T1 series passed rawQC. mriqcrp102:iqc_t1_ok_ser>0
# 2. T2 series passed rawQC. mriqcrp102:iqc_t2_ok_ser>0
# 3. FreeSurfer QC not failed. freesqc01:fsqc_qc ~= 0
# 4. sMRI T2w registration to T1w: abcd_auto_postqc01: apqc_smri_t2w_regt1_rigid < 10
# 5. sMRI T2w registration to T1w: abcd_auto_postqc01: apqc_smri_t2w_regt1_rigid != NA
# 6. smri_t2w_scs_cbwmatterlh != NA

# In[7]:


df = df.merge(all_tables['abcd_auto_postqc01'], how='left', on=on)

# Convert columns to numeric
df[df.columns] = df[df.columns].apply(pd.to_numeric, errors='ignore')

df['imgincl_t2w_include'] = 0

df.loc[(df['iqc_t1_ok_ser']>0) &
       (df['iqc_t2_ok_ser']>0) &
       (df['fsqc_qc']!=0) &
       (df['apqc_smri_t2w_regt1_rigid']<10) &
       (~df['apqc_smri_t2w_regt1_rigid'].isna()) &
       (~df['smri_t2w_scs_cbwmatterlh'].isna()), 'imgincl_t2w_include'] = 1
df.shape


# ### imgincl_dmri_include
# 
# 1. dMRI series passed rawQC. mriqcrp102:iqc_dmri_ok_ser>0
# 2. T1 series passed rawQC. mriqcrp102:iqc_t1_ok_ser>0
# 3. FreeSurfer QC not failed. freesqc01:fsqc_qc ~= 0
# 4. dMRI Total number of repetitions for all OK scans is 103 or more. mriqcrp102:(iqc_dmri_ok_nreps >= 103 OR (mri_info_manufacturer = Philips AND iqc_dmri_ok_ser >= 2 AND iqc_dmri_ok_nreps = 51))
# 5. dMRI Post Processing QC not failed. dmriqc01:dmri_dti_postqc_qc ~= 0
# 6. dMRI B0 Unwarp available. abcd_auto_postqc01:apqc_dmri_bounwarp_flag = 1 
# 7. dMRI registration to T1w: threshold of 17 or greater. abcd_auto_postqc01: apqc_dmri_regt1_rigid < 17
# 8. dMRI Maximum dorsal cutoff score: threshold of 47 or greater. abcd_auto_postqc01: apqc_dmri_fov_cutoff_dorsal < 47
# 9. dMRI Maximum ventral cutoff score: threshold of 54 or greater. abcd_auto_postqc01: apqc_dmri_fov_cutoff_ventral < 54
# 10. dmri_rsind_fiberat_allfibers != NA

# In[8]:


df = df.merge(all_tables['dmriqc01'], how='left', on=on)        .merge(all_tables['mri_rsi_p102'], how='left', on=on)    

df[df.columns] = df[df.columns].apply(pd.to_numeric, errors='ignore')
    

df['imgincl_dmri_include'] = 0

df.loc[(df['iqc_dmri_ok_ser'] > 0) & 
       (df['iqc_t1_ok_ser']>0) & 
       (df['fsqc_qc']!=0) & 
       ((df['iqc_dmri_ok_nreps']>=103) | 
                ((df['iqc_dmri_ok_ser']>=2) & (df['mri_info_manufacturer']=='Philips Medical Systems') & (df['iqc_dmri_ok_nreps']==51)) ) &
       (df['dmri_dti_postqc_qc']!=0) & 
       (df['apqc_dmri_bounwarp_flag']==1) &
       (df['apqc_dmri_regt1_rigid'] < 17)  &
       (df['apqc_dmri_fov_cutoff_dorsal'] < 47) &
       (df['apqc_dmri_fov_cutoff_ventral'] < 54) &
       (~df['dmri_rsind_fiberat_allfibers'].isna()), 'imgincl_dmri_include'] = 1

df.shape


# ### imgincl_rsfmri_include
# 
# 1. rsfMRI series passed rawQC. mriqcrp102:iqc_rsfmri_ok_ser>0
# 2. T1 series passed rawQC. mriqcrp102:iqc_t1_ok_ser>0
# 3. fMRI B0 Unwarp available. abcd_auto_postqc01:apqc_fmri_bounwarp_flag = 1
# 4. FreeSurfer QC not failed. freesqc01:fsqc_qc ~= 0
# 5. fMRI Manual Post-Processing QC not failed. fmriqc01:fmri_postqc_qc ~= 0
# 6. rsfMRI Number of frames in acquisition > 375. abcd_betnet02:rsfmri_c_ngd_ntpoints > 375 
# 7. fMRI registration to T1w: threshold of 19 or greater. abcd_auto_postqc01: apqc_fmri_regt1_rigid < 19
# 8. fMRI Maximum dorsal cutoff score: threshold of 65 or greater. abcd_auto_postqc01: apqc_fmri_fov_cutoff_dorsal < 65
# 9. fMRI Maximum ventral cutoff score: threshold of 60 or greater. abcd_auto_postqc01: apqc_fmri_fov_cutoff_ventral < 60
# 10. rsfmri_c_ngd_dt_ngd_sa != NA

# In[9]:


df = df.merge(all_tables['fmriqc01'], how='left', on=on)       .merge(all_tables['abcd_betnet02'], how='left', on=on)

df['iqc_rsfmri_ok_ser'] = pd.to_numeric(df['iqc_rsfmri_ok_ser'])
df['apqc_fmri_bounwarp_flag'] = pd.to_numeric(df['apqc_fmri_bounwarp_flag'])
df['fsqc_qc'] = pd.to_numeric(df['fsqc_qc'])
df['fmri_postqc_qc'] = pd.to_numeric(df['fmri_postqc_qc'])
df['rsfmri_c_ngd_ntpoints'] = pd.to_numeric(df['rsfmri_c_ngd_ntpoints'])
df['apqc_fmri_regt1_rigid'] = pd.to_numeric(df['apqc_fmri_regt1_rigid'])
df['apqc_fmri_fov_cutoff_dorsal'] = pd.to_numeric(df['apqc_fmri_fov_cutoff_dorsal'])
df['apqc_fmri_fov_cutoff_ventral'] = pd.to_numeric(df['apqc_fmri_fov_cutoff_ventral'])


df['imgincl_rsfmri_include'] = 0

df.loc[(df['iqc_rsfmri_ok_ser']>0) & 
       (df['iqc_t1_ok_ser']>0) & 
       (df['apqc_fmri_bounwarp_flag']==1) & 
       (df['fsqc_qc']!=0) & 
       (df['fmri_postqc_qc']!=0) & 
       (df['rsfmri_c_ngd_ntpoints']>375) & 
       (df['apqc_fmri_regt1_rigid'] < 19) &
       (df['apqc_fmri_fov_cutoff_dorsal'] < 65) &
       (df['apqc_fmri_fov_cutoff_ventral'] < 60) &
       (~df['rsfmri_c_ngd_dt_ngd_sa'].isna()), 'imgincl_rsfmri_include'] = 1

df.shape


# ### imgincl_nback_include
# 
# 1. T1 series passed rawQC. mriqcrp102:iqc_t1_ok_ser>0
# 2. FreeSurfer QC not failed. freesqc01:fsqc_qc ~= 0
# 3. fMRI Manual Post-Processing QC not failed. fmriqc01:fmri_postqc_qc ~= 0
# 4. fMRI B0 Unwarp available. abcd_auto_postqc01:apqc_fmri_bounwarp_flag = 1
# 5. nBack tfMRI series passed rawQC. mriqcrp102:iqc_nback_ok_ser>0
# 6. nBack Behavior passed. abcd_mrinback02:tfmri_nback_beh_performflag = 1 
# 7. nBack degrees of freedom > 200. nback_bwroi02:tfmri_nback_all_beta_dof > 200 
# 8. nBack ePrime timing match. mriqcrp302:iqc_nback_ep_t_series_match = 1
# 9. nBack ignore ePrime mismatch. mriqcrp302:eprime_mismatch_ok_nback = 1
# 10. fMRI registration to T1w: threshold of 19 or greater. abcd_auto_postqc01: apqc_fmri_regt1_rigid < 19
# 11. fMRI Maximum dorsal cutoff score: threshold of 65 or greater. abcd_auto_postqc01: apqc_fmri_fov_cutoff_dorsal < 65
# 12. fMRI Maximum ventral cutoff score: threshold of 60 or greater. abcd_auto_postqc01: apqc_fmri_fov_cutoff_ventral < 60
# 13. tfmri_nback_all_4 != Na

# In[10]:


df = df.merge(all_tables['abcd_mrinback02'], how='left', on=on)       .merge(all_tables['mriqcrp202'], how='left', on=on)       .merge(all_tables['nback_bwroi02'], how='left', on=on)       .merge(all_tables['mriqcrp302'], how='left', on=on)

df[df.columns] = df[df.columns].apply(pd.to_numeric, errors='ignore')
    
df['imgincl_nback_include'] = 0
df = df.drop_duplicates()

df.loc[(df['iqc_t1_ok_ser']>0) &
        (df['fsqc_qc']!=0) &
        (df['fmri_postqc_qc']!=0) &
        (df['apqc_fmri_bounwarp_flag']==1) &
        (df['iqc_nback_ok_ser']>0) &
        (df['tfmri_nback_beh_performflag']==1) &
        (df['tfmri_nback_all_beta_dof']>200) & 
        ((df['iqc_nback_ep_t_series_match']==1) | (df['eprime_mismatch_ok_nback']==1)) &
       (df['apqc_fmri_regt1_rigid'] < 19) &
       (df['apqc_fmri_fov_cutoff_dorsal'] < 65) &
       (df['apqc_fmri_fov_cutoff_ventral'] < 60) & 
       (~df['tfmri_nback_all_4'].isna()), 'imgincl_nback_include'] = 1


df.shape


# ### imgincl_sst_include
# 
# 1. T1 series passed rawQC. mriqcrp102:iqc_t1_ok_ser>0
# 2. FreeSurfer QC not failed. freesqc01:fsqc_qc ~= 0
# 3. fMRI Manual Post-Processing QC not failed. fmriqc01:fmri_postqc_qc ~= 0
# 4. fMRI B0 Unwarp available. abcd_auto_postqc01:apqc_fmri_bounwarp_flag = 1
# 5. SST tfMRI series passed rawQC. mriqcrp102:iqc_sst_ok_ser>0
# 6. SST Behavior passed. abcd_sst02:tfmri_sst_beh_performflag = 1
# 7. SST degrees of freedom > 200. mrisst02:tfmri_sa_beta_dof > 200 
# 8. SST ePrime timing match. mriqcrp302:iqc_sst_ep_t_series_match = 1 | SST ignore ePrime mismatch. mriqcrp302:eprime_mismatch_ok_sst = 1
# 10. fMRI registration to T1w: threshold of 19 or greater. abcd_auto_postqc01: apqc_fmri_regt1_rigid < 19
# 11. fMRI Maximum dorsal cutoff score: threshold of 65 or greater. abcd_auto_postqc01: apqc_fmri_fov_cutoff_dorsal < 65
# 12. fMRI Maximum ventral cutoff score: threshold of 60 or greater. abcd_auto_postqc01: apqc_fmri_fov_cutoff_ventral < 60
# 13. tfmri_sst_beh_glitchflag = 0

# In[11]:


df = df.merge(all_tables['abcd_sst02'], how='left', on=on)       .merge(all_tables['mrisst02'], how='left', on=on)

df[df.columns] = df[df.columns].apply(pd.to_numeric, errors='ignore')
    
df['imgincl_sst_include'] = 0
df = df.drop_duplicates()

df.loc[(df['iqc_t1_ok_ser']>0) &
       (df['fsqc_qc']!= 0) &
       (df['fmri_postqc_qc']!=0) &
       (df['apqc_fmri_bounwarp_flag']==1) &
       (df['iqc_sst_ok_ser']>0) &
       (df['tfmri_sst_beh_performflag']==1) &
       (df['tfmri_sa_beta_dof']>200) & 
       ((df['iqc_sst_ep_t_series_match']==1) | (df['eprime_mismatch_ok_sst']==1)) &
       (df['apqc_fmri_regt1_rigid'] < 19) &
       (df['apqc_fmri_fov_cutoff_dorsal'] < 65) &
       (df['apqc_fmri_fov_cutoff_ventral'] < 60) &
       (df['tfmri_sst_beh_glitchflag']==0) &
       (~df['tfmri_sacgvf_bscs_cbwmlh'].isna()), 'imgincl_sst_include'] = 1

df.shape


# ### imgincl_mid_include
# 
# 1. MID tfMRI series passed rawQC. mriqcrp102:iqc_mid_ok_ser>0
# 2. T1 series passed rawQC. mriqcrp102:iqc_t1_ok_ser>0
# 3. FreeSurfer QC not failed. freesqc01:fsqc_qc ~= 0
# 4. fMRI Manual Post-Processing QC not failed. fmriqc01:fmri_postqc_qc ~= 0
# 5. fMRI B0 Unwarp available. abcd_auto_postqc01:apqc_fmri_bounwarp_flag = 1 
# 6. MID Behavior passed. abcd_mid02:tfmri_mid_beh_performflag = 1
# 7. MID degrees of freedom > 200. midaparc03:tfmri_mid_all_b_dof > 200 
# 8. MID Total number of trials is 100. abcd_mid02:tfmri_mid_all_beh_t_nt = 100
# 9. MID ePrime timing match. mriqcrp302:iqc_mid_ep_t_series_match = 1 | mriqcrp302:eprime_mismatch_ok_mid = 1
# 10. fMRI registration to T1w: threshold of 19 or greater. abcd_auto_postqc01: apqc_fmri_regt1_rigid < 19
# 11. fMRI Maximum dorsal cutoff score: threshold of 65 or greater. abcd_auto_postqc01: apqc_fmri_fov_cutoff_dorsal < 65
# 12. fMRI Maximum ventral cutoff score: threshold of 60 or greater. abcd_auto_postqc01: apqc_fmri_fov_cutoff_ventral < 60
# 13. tfmri_ma_acdn_b_scs_cbwmlh != Na

# In[12]:


df = df.merge(all_tables['abcd_mid02'], how='left', on=on)       .merge(all_tables['midaparc03'], how='left', on=on)

df[df.columns] = df[df.columns].apply(pd.to_numeric, errors='ignore')
    
df['imgincl_mid_include'] = 0
df = df.drop_duplicates()

df.loc[(df['iqc_mid_ok_ser']>0) &
       (df['iqc_t1_ok_ser']>0) & 
       (df['fsqc_qc']!=0) &
       (df['fmri_postqc_qc']!=0) &
       (df['tfmri_mid_beh_performflag']==1) &
       (df['tfmri_mid_all_b_dof']>200) &
       (df['tfmri_mid_all_beh_t_nt']==100) &
       (df['apqc_fmri_bounwarp_flag']==1) & 
       ((df['iqc_mid_ep_t_series_match']==1) | (df['eprime_mismatch_ok_mid']==1)) &
       (df['apqc_fmri_regt1_rigid'] < 19) &
       (df['apqc_fmri_fov_cutoff_dorsal'] < 65) &
       (df['apqc_fmri_fov_cutoff_ventral'] < 60) & 
       (~df['tfmri_ma_acdn_b_scs_cbwmlh'].isna()), 'imgincl_mid_include'] = 1

df.shape


# ### Export Final DataFrame

# In[13]:


final_df = df[['VisitID', 'imgincl_t1w_include', 'imgincl_t2w_include', 'imgincl_dmri_include', 
               'imgincl_rsfmri_include', 'imgincl_mid_include', 'imgincl_nback_include', 'imgincl_sst_include']]

if verbose:
    print('\nSummary of Inclusion\n')
    print('There are total of {} records'.format(final_df.shape[0]))
    print(final_df.iloc[:, 1:].sum())
    print('\n')
    
final_df.to_csv(output_path, index=False)

print('The file is saved to', output_path)

