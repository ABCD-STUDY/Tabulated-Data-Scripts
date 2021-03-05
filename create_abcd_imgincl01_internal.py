#!/usr/bin/env python
# coding: utf-8

# In[43]:


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


# In[44]:


parser = argparse.ArgumentParser()

parser.add_argument('-o', '--output', help='Path to save abcd_imgincl01 file. /Path/to/save/file/', default='.')
parser.add_argument('-v', '--verbose', help='Verbosity. Default to False', action='store_true')


# In[45]:


args = parser.parse_args()

verbose = args.verbose
output_path = os.path.join(args.output, 'abcd_imgincl01.csv')


# In[46]:


def load_df(name, columns=None, verbose=True):
    
    """
        Import only necessary columns
    """
    
    if verbose:
        
        print('Loading', name)
    
    if columns is not None:
        
        columns = ', '.join(columns)
        
        df = pd.read_sql(f'select {columns} from {name}', con=prod_engine)
        
    else:
        df = pd.read_sql(f'select * from {name}', con=prod_engine)
        
    if verbose:
        print(f'Loaded {name} {df.shape}\n')
    
    return df


# In[47]:


from getpass import getpass

pwd = getpass('Enter password to imagetrack database : ')
prod_engine = create_engine('mysql+pymysql://imagetrack:{}@169.228.56.189:3306/imagetrack'.format(pwd), echo=False)

try:
    prod_engine.connect()
except sqlalchemy.exc.OperationalError:
    print('The password is incorrect')
    sys.exit()
    
if verbose:
    print('Start Loading all dataframes')

redcap = pd.read_sql('select pGUID, eventname from redcap_release_30', con=prod_engine)
dal_ra_checklist = pd.read_sql("""select id_redcap, redcap_event_name, ra_scan_not_scanned___1 from dal_ra_checklist""", con=prod_engine)
abcd_mrfindings02 = pd.read_sql("""select src_subject_id, eventname, mrif_score from abcd_mrfindings02""", con=prod_engine)

mriqcrp102 = load_df('mriqcrp102_30', ['id_redcap', 'redcap_event_name', 'iqc_t1_ok_ser', 'iqc_t2_ok_ser', 'iqc_dmri_ok_ser', 'iqc_dmri_ok_nreps', 'iqc_rsfmri_ok_ser'], verbose)
mriqcrp202 = load_df('mriqcrp202_30', ['id_redcap', 'redcap_event_name', 'iqc_mid_ok_ser', 'iqc_nback_ok_ser', 'iqc_sst_ok_ser'], verbose)
mriqcrp302 = load_df('mriqcrp302_30', ['id_redcap', 'redcap_event_name', 'iqc_mid_ep_t_series_match', 'eprime_mismatch_ok_mid', 'iqc_nback_ep_t_series_match', 'eprime_mismatch_ok_nback', 'iqc_sst_ep_t_series_match', 'eprime_mismatch_ok_sst'], verbose)

freesqc01 = load_df('freesqc01_30', verbose=verbose)
dmriqc01 = load_df('dmriqc01_30', ['VisitID', 'dmri_dti_postqc_visitid', 'dmri_dti_postqc_qc'], verbose=verbose)
abcd_mid02 = load_df('abcd_mid02_30', verbose=verbose)
midaparc03 = load_df('midaparc03_30', ['VisitID', 'tfmri_mid_all_b_dof', 'tfmri_ma_acdn_b_scs_cbwmlh'], verbose=verbose)
abcd_mrinback02 = load_df('abcd_mrinback02_30', ['src_subject_id', 'eventname', 'tfmri_nback_beh_performflag'], verbose=verbose)
abcd_sst02 = load_df('abcd_sst02_30', ['src_subject_id', 'eventname', 'tfmri_sst_beh_performflag', 'tfmri_sst_beh_glitchflag'], verbose=verbose)
fmriqc01 = load_df('fmriqc01_30', ['VisitID', 'fmri_postqc_qc'], verbose=verbose)
abcd_betnet02 = load_df('abcd_betnet02_30', ['VisitID', 'rsfmri_c_ngd_ntpoints', 'rsfmri_c_ngd_dt_ngd_sa'], verbose=verbose)
nback_bwroi02 = load_df('nback_bwroi02_30', ['VisitID', 'tfmri_nback_all_beta_dof', 'tfmri_nback_all_4'], verbose=verbose)
mrisst02 = load_df('mrisst02_30', ['VisitID', 'tfmri_sa_beta_dof', 'tfmri_sacgvf_bscs_cbwmlh'], verbose=verbose)
abcd_auto_postqc01 = load_df('abcd_auto_postqc01_30', verbose=verbose)
abcd_smrip201 = load_df('abcd_smrip201_30', verbose=verbose)
mri_rsi_p102 = load_df('mri_rsi_p102_30', verbose=verbose)
abcd_mri = load_df('abcd_mri01_30', ['src_subject_id', 'eventname', 'mri_info_visitid', 'mri_info_manufacturer'], verbose=verbose)


# ### General Inclusion Criteria (Recommended from Confluence page)
# Apply mrfindings to all modality
# 1. No serious MR Findings. abcd_mrfindings02:mrif_score!=3 && abcd_mrfindings01:mrif_score!=4 (Currently not added)

# In[48]:


left_on = ['pGUID', 'eventname']

redcap = pd.read_sql('select pGUID, eventname from redcap_release_30', con=prod_engine)

# Retrieve records that are in released table
# Filter out non-sharing records
incl = redcap.merge(abcd_mri, how='inner', left_on=['pGUID', 'eventname'], right_on=['src_subject_id', 'eventname']).drop(columns='src_subject_id')
incl = incl.merge(abcd_mrfindings02, how='left', left_on=left_on, right_on=['src_subject_id', 'eventname']).drop(columns='src_subject_id')

incl = incl.rename(columns={'mri_info_visitid':'VisitID'})

# Re-order index
incl = incl.reset_index(drop=True).sort_values('VisitID')


# ### imgincl_t1w_include
# 
# 1. T1 series passed rawQC. mriqcrp102:iqc_t1_ok_ser>0
# 2. FreeSurfer QC not failed. freesqc01:fsqc_qc ~= 0
# 3. abcd_smrip201:smri_t1w_scs_cbwmatterlh is not NA

# In[49]:


freesqc01 = freesqc01[['VisitID', 'fsqc_qc']]
abcd_smrip201 = abcd_smrip201[['VisitID', 'smri_t1w_scs_cbwmatterlh', 'smri_t2w_scs_cbwmatterlh']]

df = incl.merge(mriqcrp102, how='left', left_on=left_on, right_on=['id_redcap', 'redcap_event_name'])         .merge(freesqc01, how='left', on='VisitID').drop(columns=['id_redcap', 'redcap_event_name'])         .merge(abcd_smrip201, how='left', left_on='VisitID', right_on='VisitID')

df['imgincl_t1w_include'] = 0

df.loc[(df['iqc_t1_ok_ser']>0) &
       (df['fsqc_qc']!=0) &
       (~df['smri_t1w_scs_cbwmatterlh'].isna()), 'imgincl_t1w_include'] = 1


# ### imgincl_t2w_include
# 1. T1 series passed rawQC. mriqcrp102:iqc_t1_ok_ser>0
# 2. T2 series passed rawQC. mriqcrp102:iqc_t2_ok_ser>0
# 3. FreeSurfer QC not failed. freesqc01:fsqc_qc ~= 0
# 4. sMRI T2w registration to T1w: abcd_auto_postqc01: apqc_smri_t2w_regt1_rigid < 10
# 5. sMRI T2w registration to T1w: abcd_auto_postqc01: apqc_smri_t2w_regt1_rigid != NA
# 6. smri_t2w_scs_cbwmatterlh != NA

# In[50]:


df = df.merge(abcd_auto_postqc01, how='left', on='VisitID')
df['apqc_smri_t2w_regt1_rigid'] = df['apqc_smri_t2w_regt1_rigid'].astype(float)

df['imgincl_t2w_include'] = 0

df.loc[(df['iqc_t1_ok_ser']>0) &
       (df['iqc_t2_ok_ser']>0) &
       (df['fsqc_qc']!=0) &
       (df['apqc_smri_t2w_regt1_rigid']<10) &
       (~df['apqc_smri_t2w_regt1_rigid'].isna()) &
       (~df['smri_t2w_scs_cbwmatterlh'].isna()), 'imgincl_t2w_include'] = 1


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

# In[51]:


mri_rsi_p102 = mri_rsi_p102[['VisitID', 'dmri_rsind_fiberat_allfibers']]

df = df.merge(dmriqc01, how='left', on='VisitID')        .merge(mri_rsi_p102, how='left', on='VisitID')

df['apqc_dmri_regt1_rigid'] = df['apqc_dmri_regt1_rigid'].str.replace('_NaN_', '')
df['apqc_dmri_regt1_rigid'] = pd.to_numeric(df['apqc_dmri_regt1_rigid'])

df['apqc_dmri_fov_cutoff_dorsal'] = df['apqc_dmri_fov_cutoff_dorsal'].str.replace('_NaN_', '')
df['apqc_dmri_fov_cutoff_dorsal'] = pd.to_numeric(df['apqc_dmri_fov_cutoff_dorsal'])

df['apqc_dmri_fov_cutoff_ventral'] = df['apqc_dmri_fov_cutoff_ventral'].str.replace('_NaN_', '')
df['apqc_dmri_fov_cutoff_ventral'] = pd.to_numeric(df['apqc_dmri_fov_cutoff_ventral'])

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

# In[52]:


df = df.merge(fmriqc01, how='left', on='VisitID')       .merge(abcd_betnet02, how='left', on='VisitID')

df['apqc_fmri_regt1_rigid'] = df['apqc_fmri_regt1_rigid'].str.replace('_NaN_', '')
df['apqc_fmri_regt1_rigid'] = pd.to_numeric(df['apqc_fmri_regt1_rigid'])

df['apqc_fmri_fov_cutoff_dorsal'] = df['apqc_fmri_fov_cutoff_dorsal'].str.replace('_NaN_', '')
df['apqc_fmri_fov_cutoff_dorsal'] = pd.to_numeric(df['apqc_fmri_fov_cutoff_dorsal'])

df['apqc_fmri_fov_cutoff_ventral'] = df['apqc_fmri_fov_cutoff_ventral'].str.replace('_NaN_', '')
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

# In[53]:


for col in ['subjectkey', 'id_redcap', 'redcap_event_name']:
    try:
        df = df.drop(columns=col)
    except:
        pass

df = df.merge(abcd_mrinback02, how='left', left_on=left_on, right_on=['src_subject_id', 'eventname'])       .merge(mriqcrp202, how='left', left_on=left_on, right_on=['id_redcap', 'redcap_event_name']).drop(columns=['id_redcap', 'src_subject_id', 'redcap_event_name'])       .merge(nback_bwroi02, how='left', on='VisitID')       .merge(mriqcrp302, how='left', left_on=left_on, right_on=['id_redcap', 'redcap_event_name']).drop(columns=['id_redcap', 'redcap_event_name'])

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

# In[54]:


for col in ['src_subject_id', 'id_redcap', 'redcap_event_name']:
    try:
        df = df.drop(columns=col)
    except:
        pass

df = df.merge(abcd_sst02, how='left', left_on=left_on, right_on=['src_subject_id', 'eventname'])       .merge(mrisst02, how='left', on='VisitID')

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

# In[55]:


for col in ['src_subject_id', 'id_redcap', 'redcap_event_name']:
    try:
        df = df.drop(columns=col)
    except:
        pass
abcd_mid02 = abcd_mid02[['subjectkey', 'eventname', 'tfmri_mid_beh_performflag','tfmri_mid_all_beh_t_nt']]

df = df.merge(abcd_mid02, how='left', left_on=left_on, right_on=['subjectkey', 'eventname'])       .merge(midaparc03, how='left', on=['VisitID'])

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


# In[56]:


final_df = df[['VisitID', 'imgincl_t1w_include', 'imgincl_t2w_include', 'imgincl_dmri_include', 
               'imgincl_rsfmri_include', 'imgincl_mid_include', 'imgincl_nback_include', 'imgincl_sst_include']]

if verbose:
    print(final_df.iloc[:, 1:].sum())
    
final_df.to_csv(output_path, index=False)

print('There are {} records'.format(final_df.shape[0]))
print('The file is saved to', output_path)

