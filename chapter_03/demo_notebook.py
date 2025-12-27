# Databricks notebook source
# In[ ]:


print("Hello World")


# In[ ]:


get_ipython().run_line_magic('sql', '')
SELECT 'Hello World' AS col


# In[ ]:





# In[ ]:


get_ipython().run_line_magic('run', './setup_notebook')


# In[ ]:


print(test_variable)


# In[ ]:


dbutils.help()


# In[ ]:


print('fs help:')
dbutils.fs.help()

print('notebook help:')
dbutils.notebook.help()

print('widgets help:')
dbutils.widgets.help()


# In[ ]:


available_datasets = dbutils.fs.ls("/databricks-datasets")


# In[ ]:


display(available_datasets)

