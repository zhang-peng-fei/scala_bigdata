
# coding: utf-8

# # 一、训练过程

# ### 1、读入数据

# In[1]:


import pandas as pd
dir_base = 'C:\\Users\\yym\\Desktop\\JiTuan_Model\\JiTuan_Model/'
df = pd.read_csv( dir_base + 'zp_train.txt', sep=',', header=0, index_col='SERV_ID')
# print(df.shape)


# ### 2、空值填充
df = df.fillna(0)
# ### 3、特征加工
from sklearn import preprocessing
# 将除flag字段之外的数据读入X：
X = df[ [col for col in df.columns if col not in ['FLAG']] ]
X = pd.DataFrame(preprocessing.StandardScaler().fit_transform(X), columns=X.columns, index=X.index)
y = df['FLAG']
# ### 4、特征选择
# 采用基于方差筛选的方法
from sklearn.feature_selection import VarianceThreshold
threshhold_var = 0.3   # 事先设置阈值
Arr_X_Cols_Var  = VarianceThreshold().fit(X).variances_        # ndarray，每个变量的方差
idx_Cols_Var_d  = [ i for i in range(len(Arr_X_Cols_Var)) if Arr_X_Cols_Var[i] < threshhold_var]
Cols_Var_Filter = X.iloc[:,idx_Cols_Var_d].columns    # 这些字段名要过滤掉
print('待删除字段为：', Cols_Var_Filter)
X = X.drop(Cols_Var_Filter, axis=1)
# ### 5、模型创建
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
# ### 6、模型评估
# 用模型对训练数据进行评估
print('训练集效果如下：----------------------------------------------')
from sklearn import metrics
print('↓混淆矩阵\n', metrics.confusion_matrix(y, clf.predict(X)))
print('\n')

# 命中率、覆盖率、F1测度
precision = metrics.precision_score(y, clf.predict(X), average=None)[1]*100
recall    = metrics.recall_score(y, clf.predict(X), average=None)[1]*100
f1        = metrics.f1_score(y, clf.predict(X))*100

print('precision: %4.2f%% \t recall: %4.2f%% \t f1_score: %4.2f%%' % ( precision, recall, f1))


# # 二、预测过程

# ### 1、预测集数据整理（过程与前面保持一致）

# In[7]:


import pandas as pd
X_pre = pd.read_csv( dir_base + 'zp_pre.txt', sep=',', header=0, index_col='SERV_ID')
print(X_pre.shape)

X_pre = X_pre.fillna(0)
X_pre = pd.DataFrame(preprocessing.StandardScaler().fit_transform(X_pre), columns=X_pre.columns, index=X_pre.index)
X_pre = X_pre.drop(['PAY_MODE'], axis=1)
print(X_pre.shape)

print (X_pre.head())
# ### 2、生成预测结果

# In[8]:
zp_flag=clf.predict(X_pre)
zp_pred_proba=clf.predict_proba(X_pre)[:,1].reshape(-1,1)
lw_rf_out=zp_flag
lw_rf_out['pre']=zp_pred_proba
print (lw_rf_out.head())



print('将预测结果导出至 zp_pre_result ：----------------------------------------------')
import numpy as np
# 只需要将预测为诈骗的SERV_ID保存下来提交：
np.savetxt( dir_base + 'zp_pre_result.txt', list(df_pre[df_pre['FLAG']==1].index), fmt='%s');


# ### 3、预测结果评估


'''
# 导入标准答案
import numpy as np
list_ans = np.loadtxt( dir_base + 'Answer/zp_ans.txt')
len(list_ans)


# In[15]:


# 导入预测结果
list_pre = np.loadtxt( dir_base + 'zp_pre_result.txt')
len(list_pre)


# In[13]:


print('预测集效果如下：----------------------------------------------')

tp = len(set(list_ans).intersection(set(list_pre))) # 预测名单中，确实为流失用户的数量

# # 命中率、覆盖率、F1测度
precision = tp / len(list_pre)*100
recall    = tp / len(list_ans)*100
f1        = 2*precision*recall/(precision+recall)

print('precision: %4.2f%% \t recall: %4.2f%% \t f1_score: %4.2f%%' % ( precision, recall, f1))
'''
