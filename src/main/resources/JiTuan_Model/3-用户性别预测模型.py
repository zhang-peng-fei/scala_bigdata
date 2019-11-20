
# coding: utf-8

# # 一、训练过程

# ### 1、读入数据

# In[38]:


import pandas as pd
dir_base = 'C:\\Users\\yym\\Desktop\\JiTuan_Model\\JiTuan_Model/'
df = pd.read_csv( dir_base + 'xb_train.txt', sep=',', header=0, index_col='SERV_ID')
# print(df.shape)


# ### 2、空值填充

# In[39]:


df = df.fillna(0)


# ### 3、特征加工

# In[40]:


from sklearn import preprocessing

# 将除flag字段之外的数据读入X：
X = df[ [col for col in df.columns if col not in ['FLAG']] ]
X = pd.DataFrame(preprocessing.StandardScaler().fit_transform(X), columns=X.columns, index=X.index)
y = df['FLAG']


# ### 4、特征选择

# In[41]:


# 采用基于方差筛选的方法
from sklearn.feature_selection import VarianceThreshold
threshhold_var = 0.3   # 事先设置阈值
Arr_X_Cols_Var  = VarianceThreshold().fit(X).variances_        # ndarray，每个变量的方差
idx_Cols_Var_d  = [ i for i in range(len(Arr_X_Cols_Var)) if Arr_X_Cols_Var[i] < threshhold_var]
Cols_Var_Filter = X.iloc[:,idx_Cols_Var_d].columns    # 这些字段名要过滤掉

print('待删除字段为：', Cols_Var_Filter)
X = X.drop(Cols_Var_Filter, axis=1)


# ### 5、模型创建

# In[42]:


from sklearn import tree

model = tree.DecisionTreeClassifier(criterion='entropy',    # 划分标准：gini（default）、entropy
                                    max_leaf_nodes=100,      # 最大叶节点数
                                    max_depth=16,           # 最大树深度
                                    min_samples_split=20,   # 分裂所需最小样本数
                                    min_samples_leaf=10,    # 叶节点最小样本数
                                    max_features=20,        # 分裂时考虑的最大特征数
                                   )
clf = model.fit(X, y)
# print(clf)


# ### 6、模型评估

# In[43]:


# 用模型对训练数据进行评估
print('训练集效果如下：----------------------------------------------')
from sklearn import metrics

con_mat = metrics.confusion_matrix(       y, clf.predict(X)  )
cla_rep = metrics.classification_report(  y, clf.predict(X)  )

print(con_mat)
print('\n')
print(cla_rep)

print('总体准确率为：%.2f%%' % ((con_mat[0][0]+con_mat[1][1])*100/con_mat.sum()) )


# # 二、预测过程

# ### 1、预测集数据整理（过程与前面保持一致）

# In[44]:


import pandas as pd
X_pre = pd.read_csv( dir_base + 'xb_pre.txt', sep=',', header=0, index_col='SERV_ID')
print(X_pre.shape)

X_pre = X_pre.fillna(0)
X_pre = pd.DataFrame(preprocessing.StandardScaler().fit_transform(X_pre), columns=X_pre.columns, index=X_pre.index)
print(X_pre.shape)


# ### 2、生成预测结果

# In[45]:


y_pre = clf.predict(X_pre)
type(y_pre)


# In[46]:


df_pre = pd.DataFrame(y_pre, index=X_pre.index, columns=['FLAG'])


# In[47]:


df_pre.head()


# In[48]:


print('将预测结果导出至 xb_pre_result ：----------------------------------------------')
import numpy as np
# 将预测的SERV_ID,FLAG保存下来提交：
df_pre.to_csv( dir_base + 'xb_pre_result.txt', header=False)


# ### 3、预测结果评估

# In[50]:


# 导入标准答案
df_ans = pd.read_csv( dir_base + 'Answer/xb_ans.txt', sep=',', names=['SERV_ID', 'FLAG'], header=None)
df_ans = df_ans.sort_values('SERV_ID')
df_ans.head()


# In[51]:


# 导入预测结果
df_pre = pd.read_csv( dir_base + 'xb_pre_result.txt', sep=',', names=['SERV_ID', 'FLAG'], header=None)
df_pre = df_pre.sort_values('SERV_ID')
df_pre.head()


# In[52]:


print('预测集效果如下：----------------------------------------------')
# 用模型对训练数据进行评估
from sklearn import metrics

con_mat = metrics.confusion_matrix(       list(df_ans['FLAG']),  list(df_pre['FLAG'])  )
cla_rep = metrics.classification_report(  list(df_ans['FLAG']),  list(df_pre['FLAG'])  )

print(con_mat)
print('\n')
print(cla_rep)

print('总体准确率为：%.2f%%' % ((con_mat[0][0]+con_mat[1][1])*100/con_mat.sum()) )

