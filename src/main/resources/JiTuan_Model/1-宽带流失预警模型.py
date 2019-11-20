
# coding: utf-8

# # 一、训练过程

# ### 1、读入数据

# In[26]:


import pandas as pd
dir_base = 'E:\JiTuan_Model/'
df = pd.read_csv( dir_base + 'kd_train.txt', sep=',', header=0, index_col='SERV_ID')
# print(df.shape)


# ### 2、数据转换

# In[27]:


df = df.replace({'PROD_INST_STATE':'2HA'},1)


# In[28]:


# print(df['RM_MODE_CD'].value_counts())


# In[29]:


df = df.replace({'RM_MODE_CD':'FTTH接入'},1)
df = df.replace({'RM_MODE_CD':'普通接入'},2)
df = df.replace({'RM_MODE_CD':'LAN接入'},3)
df = df.replace({'RM_MODE_CD':'VDSL接入'},4)


# In[30]:


# print(df['OFFER_NAME_LEV1'].value_counts())


# In[31]:


df = df.replace({'OFFER_NAME_LEV1':'宽带'},1)
df = df.replace({'OFFER_NAME_LEV1':'一般套餐'},2)


# In[32]:


# 空值填充
df = df.fillna({
    'GENDER':0, 
    'AGE':df['AGE'].mean(),
    'STAR_LEVEL':0,
    'RM_MODE_CD':0,
    'OFFER_NAME_LEV1':0,
    'ZHU_OFFER_VALUE':df['ZHU_OFFER_VALUE'].mean()
})


# ### 3、特征衍生

# In[33]:


# 衍生字段 - 均值
df['BRD_CHARGE_AVG'  ] = df.loc[:,['BRD_CHARGE_11'  , 'BRD_CHARGE_10'  , 'BRD_CHARGE_09'  ]].mean(axis=1)
df['BRD_MBL_FLUX_AVG'] = df.loc[:,['BRD_MBL_FLUX_11', 'BRD_MBL_FLUX_10', 'BRD_MBL_FLUX_09']].mean(axis=1)
df['BRD_CNT_AVG'     ] = df.loc[:,['BRD_CNT_11'     , 'BRD_CNT_10'     , 'BRD_CNT_09'     ]].mean(axis=1)
df['ACTIVE_DAY_AVG'  ] = df.loc[:,['ACTIVE_DAY_11'  , 'ACTIVE_DAY_10'  , 'ACTIVE_DAY_09'  ]].mean(axis=1)


# In[34]:


# 衍生字段 - 趋势
df['BRD_CHARGE_TRD'  ] = (df['BRD_CHARGE_11'  ] - df['BRD_CHARGE_09'  ])/df['BRD_CHARGE_AVG'  ]
df['BRD_MBL_FLUX_TRD'] = (df['BRD_MBL_FLUX_11'] - df['BRD_MBL_FLUX_09'])/df['BRD_MBL_FLUX_AVG']
df['BRD_CNT_TRD'     ] = (df['BRD_CNT_11'     ] - df['BRD_CNT_09'     ])/df['BRD_CNT_AVG'     ]
df['ACTIVE_DAY_TRD'  ] = (df['ACTIVE_DAY_11'  ] - df['ACTIVE_DAY_09'  ])/df['ACTIVE_DAY_AVG'  ]


# In[35]:


# 衍生字段 - 波动
df['BRD_CHARGE_FLU'  ] = (df.loc[:,['BRD_CHARGE_11'  , 'BRD_CHARGE_10'  , 'BRD_CHARGE_09'  ]].max(axis=1) - df.loc[:,['BRD_CHARGE_11'  , 'BRD_CHARGE_10'  , 'BRD_CHARGE_09'  ]].min(axis=1))/df['BRD_CHARGE_AVG'  ]
df['BRD_MBL_FLUX_FLU'] = (df.loc[:,['BRD_MBL_FLUX_11', 'BRD_MBL_FLUX_10', 'BRD_MBL_FLUX_09']].max(axis=1) - df.loc[:,['BRD_MBL_FLUX_11', 'BRD_MBL_FLUX_10', 'BRD_MBL_FLUX_09']].min(axis=1))/df['BRD_MBL_FLUX_AVG']
df['BRD_CNT_FLU'     ] = (df.loc[:,['BRD_CNT_11'     , 'BRD_CNT_10'     , 'BRD_CNT_09'     ]].max(axis=1) - df.loc[:,['BRD_CNT_11'     , 'BRD_CNT_10'     , 'BRD_CNT_09'     ]].min(axis=1))/df['BRD_CNT_AVG'     ]
df['ACTIVE_DAY_FLU'  ] = (df.loc[:,['ACTIVE_DAY_11'  , 'ACTIVE_DAY_10'  , 'ACTIVE_DAY_09'  ]].max(axis=1) - df.loc[:,['ACTIVE_DAY_11'  , 'ACTIVE_DAY_10'  , 'ACTIVE_DAY_09'  ]].min(axis=1))/df['ACTIVE_DAY_AVG'  ]


# In[36]:


df = df.fillna(0)


# ### 4、特征加工

# In[37]:


from sklearn import preprocessing

# 将除flag字段之外的数据读入X：
X = df[ [col for col in df.columns if col not in ['FLAG']] ]
X = pd.DataFrame(preprocessing.StandardScaler().fit_transform(X), columns=X.columns, index=X.index)
y = df['FLAG']


# ### 5、特征选择

# In[38]:


# 采用基于方差筛选的方法
from sklearn.feature_selection import VarianceThreshold
threshhold_var = 0.3   # 事先设置阈值
Arr_X_Cols_Var  = VarianceThreshold().fit(X).variances_        # ndarray，每个变量的方差
idx_Cols_Var_d  = [ i for i in range(len(Arr_X_Cols_Var)) if Arr_X_Cols_Var[i] < threshhold_var]
Cols_Var_Filter = X.iloc[:,idx_Cols_Var_d].columns    # 这些字段名要过滤掉

print('待删除字段为：', Cols_Var_Filter)
X = X.drop(Cols_Var_Filter, axis=1)


# In[39]:


# 各月数据原始值可直接删除
X = X.drop(['BRD_CHARGE_11'  , 'BRD_CHARGE_10'  , 'BRD_CHARGE_09'  ], axis=1)
X = X.drop(['BRD_MBL_FLUX_11', 'BRD_MBL_FLUX_10', 'BRD_MBL_FLUX_09'], axis=1)
X = X.drop(['BRD_CNT_11'     , 'BRD_CNT_10'     , 'BRD_CNT_09'     ], axis=1)
X = X.drop(['ACTIVE_DAY_11' , 'ACTIVE_DAY_10'   , 'ACTIVE_DAY_09'  ], axis=1)


# ### 6、模型创建

# In[40]:


from sklearn import tree

model = tree.DecisionTreeClassifier(criterion='entropy',    # 划分标准：gini（default）、entropy
                                    max_leaf_nodes=50,      # 最大叶节点数
                                    max_depth=16,           # 最大树深度
                                    min_samples_split=50,   # 分裂所需最小样本数
                                    min_samples_leaf=20,    # 叶节点最小样本数
                                    max_features=20,        # 分裂时考虑的最大特征数
                                    class_weight={0:0.2}    # 样本均衡
                                   )
clf = model.fit(X, y)
# print(clf)


# ### 7、模型评估

# In[54]:


# 用模型对训练数据进行评估
print('训练集效果如下：----------------------------------------------')
from sklearn import metrics
print('↓混淆矩阵\n', metrics.confusion_matrix(y, clf.predict(X)))

# 命中率、覆盖率、F1测度
precision = metrics.precision_score(y, clf.predict(X), average=None)[1]*100
recall    = metrics.recall_score(y, clf.predict(X), average=None)[1]*100
f1        = metrics.f1_score(y, clf.predict(X))*100

print('precision: %4.2f%% \t recall: %4.2f%% \t f1_score: %4.2f%%' % ( precision, recall, f1))


# # 二、预测过程

# ### 1、预测集数据整理（过程与前面保持一致）

# In[42]:


import pandas as pd
X_pre = pd.read_csv( dir_base + 'kd_pre.txt', sep=',', header=0, index_col='SERV_ID')
print(X_pre.shape)

X_pre = X_pre.replace({'PROD_INST_STATE':'2HA'},1)

X_pre = X_pre.replace({'RM_MODE_CD':'FTTH接入'},1)
X_pre = X_pre.replace({'RM_MODE_CD':'普通接入'},2)
X_pre = X_pre.replace({'RM_MODE_CD':'LAN接入'},3)
X_pre = X_pre.replace({'RM_MODE_CD':'VDSL接入'},4)

X_pre = X_pre.replace({'OFFER_NAME_LEV1':'宽带'},1)
X_pre = X_pre.replace({'OFFER_NAME_LEV1':'一般套餐'},2)
X_pre.head()

X_pre = X_pre.fillna({
    'GENDER':0, 
    'AGE':X_pre['AGE'].mean(),
    'STAR_LEVEL':0,
    'RM_MODE_CD':0,
    'OFFER_NAME_LEV1':0,
    'ZHU_OFFER_VALUE':X_pre['ZHU_OFFER_VALUE'].mean()
})

# 衍生字段 - 均值
X_pre['BRD_CHARGE_AVG'  ] = X_pre.loc[:,['BRD_CHARGE_11'  , 'BRD_CHARGE_10'  , 'BRD_CHARGE_09'  ]].mean(axis=1)
X_pre['BRD_MBL_FLUX_AVG'] = X_pre.loc[:,['BRD_MBL_FLUX_11', 'BRD_MBL_FLUX_10', 'BRD_MBL_FLUX_09']].mean(axis=1)
X_pre['BRD_CNT_AVG'     ] = X_pre.loc[:,['BRD_CNT_11'     , 'BRD_CNT_10'     , 'BRD_CNT_09'     ]].mean(axis=1)
X_pre['ACTIVE_DAY_AVG'  ] = X_pre.loc[:,['ACTIVE_DAY_11'  , 'ACTIVE_DAY_10'  , 'ACTIVE_DAY_09'  ]].mean(axis=1)
# 衍生字段 - 趋势
X_pre['BRD_CHARGE_TRD'  ] = (X_pre['BRD_CHARGE_11'  ] - X_pre['BRD_CHARGE_09'  ])/X_pre['BRD_CHARGE_AVG'  ]
X_pre['BRD_MBL_FLUX_TRD'] = (X_pre['BRD_MBL_FLUX_11'] - X_pre['BRD_MBL_FLUX_09'])/X_pre['BRD_MBL_FLUX_AVG']
X_pre['BRD_CNT_TRD'     ] = (X_pre['BRD_CNT_11'     ] - X_pre['BRD_CNT_09'     ])/X_pre['BRD_CNT_AVG'     ]
X_pre['ACTIVE_DAY_TRD'  ] = (X_pre['ACTIVE_DAY_11'  ] - X_pre['ACTIVE_DAY_09'  ])/X_pre['ACTIVE_DAY_AVG'  ]
# 衍生字段 - 波动
X_pre['BRD_CHARGE_FLU'  ] = (X_pre.loc[:,['BRD_CHARGE_11'  , 'BRD_CHARGE_10'  , 'BRD_CHARGE_09'  ]].max(axis=1) - X_pre.loc[:,['BRD_CHARGE_11'  , 'BRD_CHARGE_10'  , 'BRD_CHARGE_09'  ]].min(axis=1))/X_pre['BRD_CHARGE_AVG'  ]
X_pre['BRD_MBL_FLUX_FLU'] = (X_pre.loc[:,['BRD_MBL_FLUX_11', 'BRD_MBL_FLUX_10', 'BRD_MBL_FLUX_09']].max(axis=1) - X_pre.loc[:,['BRD_MBL_FLUX_11', 'BRD_MBL_FLUX_10', 'BRD_MBL_FLUX_09']].min(axis=1))/X_pre['BRD_MBL_FLUX_AVG']
X_pre['BRD_CNT_FLU'     ] = (X_pre.loc[:,['BRD_CNT_11'     , 'BRD_CNT_10'     , 'BRD_CNT_09'     ]].max(axis=1) - X_pre.loc[:,['BRD_CNT_11'     , 'BRD_CNT_10'     , 'BRD_CNT_09'     ]].min(axis=1))/X_pre['BRD_CNT_AVG'     ]
X_pre['ACTIVE_DAY_FLU'  ] = (X_pre.loc[:,['ACTIVE_DAY_11'  , 'ACTIVE_DAY_10'  , 'ACTIVE_DAY_09'  ]].max(axis=1) - X_pre.loc[:,['ACTIVE_DAY_11'  , 'ACTIVE_DAY_10'  , 'ACTIVE_DAY_09'  ]].min(axis=1))/X_pre['ACTIVE_DAY_AVG'  ]
X_pre = X_pre.fillna(0)


# In[43]:


X_pre = pd.DataFrame(preprocessing.StandardScaler().fit_transform(X_pre), columns=X_pre.columns, index=X_pre.index)


# In[44]:


X_pre = X_pre.drop(['BRD_CHARGE_11'  , 'BRD_CHARGE_10'  , 'BRD_CHARGE_09'  ], axis=1)
X_pre = X_pre.drop(['BRD_MBL_FLUX_11', 'BRD_MBL_FLUX_10', 'BRD_MBL_FLUX_09'], axis=1)
X_pre = X_pre.drop(['BRD_CNT_11'     , 'BRD_CNT_10'     , 'BRD_CNT_09'     ], axis=1)
X_pre = X_pre.drop(['ACTIVE_DAY_11' , 'ACTIVE_DAY_10'   , 'ACTIVE_DAY_09'  ], axis=1)
X_pre = X_pre.drop(['PROD_INST_STATE'], axis=1)


# ### 2、生成预测结果

# In[45]:


y_pre = clf.predict(X_pre)
type(y_pre)


# In[46]:


df_pre = pd.DataFrame(y_pre, index=X_pre.index, columns=['FLAG'])


# In[55]:


print('将预测结果导出至 kd_pre_result ：----------------------------------------------')

import numpy as np
# 只需要将预测为流失的SERV_ID保存下来提交：
np.savetxt(dir_base + 'kd_pre_result.txt', list(df_pre[df_pre['FLAG']==1].index), fmt='%s');

print('导出完毕！')


# ### 3、预测结果评估

# In[49]:


# 导入标准答案
import numpy as np
list_ans = np.loadtxt(dir_base + 'Answer/kd_ans.txt')
len(list_ans)


# In[50]:


# 导入预测结果
list_pre = np.loadtxt(dir_base + 'kd_pre_result.txt')
len(list_pre)


# In[56]:


print('预测集效果如下：----------------------------------------------')

tp = len(set(list_ans).intersection(set(list_pre))) # 预测名单中，确实为流失用户的数量

# # 命中率、覆盖率、F1测度
precision = tp / len(list_pre)*100
recall    = tp / len(list_ans)*100
f1        = 2*precision*recall/(precision+recall)

print('precision: %4.2f%% \t recall: %4.2f%% \t f1_score: %4.2f%%' % ( precision, recall, f1))

