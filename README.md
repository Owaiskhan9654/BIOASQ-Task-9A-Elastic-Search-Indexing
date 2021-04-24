This Live Notebook is created by Owais Ahmad, contact and questions: [owaiskhan9654.github.io](https://owaiskhan9654.github.io/)


This notebook is bit lengthy but I have explained everthing very precisely
# ElasticSearch Basics
- [ElasticSearch installation](https://www.elastic.co/guide/en/elasticsearch/reference/current/install-elasticsearch.html)
- [ElasticSearch python client](https://elasticsearch-py.readthedocs.io/en/master/)
- BIOASQ TASK 9a Dataset [Link](http://participants-area.bioasq.org/Tasks/9a/trainingDataset/raw/allMeSH/)

## Installing python client

```!pip install elasticsearch```

### Importing packages


```python
# Import packages
import numpy as np
import pandas as pd
import re
import ijson
from pprint import pprint
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning) 
from elasticsearch import Elasticsearch
```

### Configurating ElasticSearch


```python
# Elastic search configuation

es = Elasticsearch(HOST="http://localhost", PORT=9200)
```

### In BIOASQ TASK 9A total  Number of articles present are 15,559,157 which is around 25.6 GB in size and Total Number of MeSH Covered in These articles are 29,369

### Loading BioAsq Task 8a Dataset

In this notebook I am only working with sample size of 10,000 


## DataSet [Link](http://participants-area.bioasq.org/Tasks/9a/trainingDataset/raw/allMeSH/)


```python
%%time

abstractText=[]
meshMajor=[]
pmid=[]
title=[]
journal=[]
year=[]
count=0 #you may increase the value of count in order to 
f = open(r"D:/Lab Backup by Sushil/OWAIS/BIO ASQ DATASET TASK A/allMeSH_2021.json")
objects = ijson.items(f, 'articles.item')
for obj in tqdm(objects):
    abstractText.append(obj["abstractText"].strip())
    meshMajor.append(obj["meshMajor"])
    pmid.append(obj["pmid"])
    title.append(obj['title'])
    journal.append(obj['journal'])
    year.append(obj['year'])
    count =count +1
    if count==10000:
        break

data = pd.DataFrame({'abstractText': abstractText, 'journal':journal,'meshMajor': meshMajor,'pmid':pmid,'title':title,'year':year})
data=data.to_dict(orient='records')
```

    9999it [00:00, 75381.85it/s]

    Wall time: 202 ms
    

    
    


```python
pprint(data[509]) #Random dataset at index 509
```

    {'abstractText': 'Resin acids in pulp and paper mills wastewater are '
                     'potentially partitioned in the solids in post-primary '
                     'clarification due to higher hydrophobicity with log Kow '
                     '?1.74-5.80. They are known to adversely affect anaerobic '
                     'digestion (AD) process, although the effect has not been '
                     'quantified deterministically in control studies. The '
                     'objective of the present work was to determine the effect of '
                     'untreated and ozonated spiked resin acids on AD of primary '
                     'sludge. Batch adsorption tests were conducted to determine '
                     'the solid-liquid partition coefficient (Kd) of resin acids '
                     'on the primary sludge. Higher Kd was obtained at pH 4; '
                     'however, it was decreased by 78-98% at pH 8. Thereafter, '
                     'batch AD of model resin acids in primary sludge using food '
                     'to microorganism ratio (S0/X) of 0.5gtCOD/gVSSindicated only '
                     '15-20% removal of resin acids in the liquid phase '
                     'anaerobically. While, ozonation in pure water using '
                     '0.74-1.48\xa0mg O3/mg tCOD showed >90% reduction of the test '
                     'resin acids, an ozone dose of 0.52\xa0mg O3/mg tCOD reduced '
                     "50-70% spiked resin acids' load to the digester. However, no "
                     'further removal of resin acids occurred during AD over 30 '
                     'days. About 42% reduction in methane production compared to '
                     'the control digestor occurred in the presence of 150\xa0mg/L '
                     'of resin acids. When treated with 0.52\xa0mg O3/mg tCOD, '
                     'methane production improved and was comparable to the '
                     'control digestor, indicating that resin acids may not be '
                     'detrimental to AD at a concentration range of 45-75\xa0mg/L.',
     'journal': 'Chemosphere',
     'meshMajor': ['Abietanes',
                   'Adsorption',
                   'Anaerobiosis',
                   'Diterpenes',
                   'Kinetics',
                   'Methane',
                   'Ozone',
                   'Resins, Plant',
                   'Sewage',
                   'Waste Disposal, Fluid',
                   'Waste Water',
                   'Water Pollutants, Chemical'],
     'pmid': '33182136',
     'title': 'Anaerobic digestibility of resin acids in primary sludge: Effect of '
              'ozone pretreatment.',
     'year': '2021'}
    

### Creating index for each Articles


```python
%%time

i=-1
for a_data in tqdm(data):
    i=i+1
    result=es.index(index='bioasq_task_9a',body=a_data,id=i)
pprint(result)
print('\n\nThis is only showing last inserted element')
```

    100%|███████████████████████████████████████████████████████████████████████████| 10000/10000 [00:35<00:00, 285.22it/s]

    {'_id': '9999',
     '_index': 'bioasq_task_9a',
     '_primary_term': 1,
     '_seq_no': 9999,
     '_shards': {'failed': 0, 'successful': 1, 'total': 2},
     '_type': '_doc',
     '_version': 1,
     'result': 'created'}
    
    
    This is only showing last inserted element
    Wall time: 35.1 s
    

    
    

### Printing the indexed Data for confirmation


```python
for i in tqdm(range(len(data))):
    
    result=es.get(index="bioasq_task_9a",id=i)
pprint(result)
print('\n\nSize of this notebook will become to large so I only printing the element which is last inserted')
```

    100%|██████████████████████████████████████████████████████████████████████████| 10000/10000 [00:08<00:00, 1209.17it/s]

    {'_id': '9999',
     '_index': 'bioasq_task_9a',
     '_primary_term': 1,
     '_seq_no': 9999,
     '_source': {'abstractText': 'The coronavirus disease (COVID-19), while mild '
                                 'in most cases, has nevertheless caused '
                                 'significant mortality. The measures adopted in '
                                 'most countries to contain it have led to '
                                 'colossal social and economic disruptions, which '
                                 'will impact the medium- and long-term health '
                                 'outcomes for many communities. In this paper, we '
                                 'deliberate on the reality and facts surrounding '
                                 'the disease. For comparison, we present data '
                                 'from past pandemics, some of which claimed more '
                                 'lives than COVID-19. Mortality data on road '
                                 'traffic crashes and other non-communicable '
                                 'diseases, which cause more deaths each year than '
                                 'COVID-19 has so far, is also provided. The '
                                 'indirect, serious health and social effects are '
                                 'briefly discussed. We also deliberate on how '
                                 'misinformation, confusion stemming from '
                                 'contrasting expert statements, and lack of '
                                 'international coordination may have influenced '
                                 'the public perception of the illness and '
                                 'increased fear and uncertainty. With pandemics '
                                 'and similar problems likely to re-occur, we call '
                                 'for evidence-based decisions, the restoration of '
                                 'responsible journalism and communication built '
                                 'on a solid scientific foundation.',
                 'journal': 'Epidemiology and infection',
                 'meshMajor': ['Accidents, Traffic',
                               'Betacoronavirus',
                               'COVID-19',
                               'Communication',
                               'Coronavirus Infections',
                               'Disease Outbreaks',
                               'Economic Recession',
                               'Humans',
                               'Influenza Pandemic, 1918-1919',
                               'Mental Health',
                               'Pandemics',
                               'Pneumonia, Viral',
                               'Psychological Distance',
                               'Public Health',
                               'Risk',
                               'SARS-CoV-2'],
                 'pmid': '32958089',
                 'title': 'The COVID-19 pandemic: the public health reality.',
                 'year': '2020'},
     '_type': '_doc',
     '_version': 1,
     'found': True}
    
    
    Size of this notebook will become to large so I only printing the element which is last inserted
    

    
    

### If some mistake occurs while indexing you can execute below command to remove that particular index


```python
#es.indices.delete(index="bioasq_task_9a")
```


```python
print(es.indices.get_alias("*")) #To show how many indices are totally present
```

    {'bioasq_task_9a': {'aliases': {}}}
    

### Defining Elastic_ser  method for matching documents with given query and number of results to be shown


```python
# Creating Query Function

## Match Query 
def Elastic_ser(Query="COVID-19",Result_size=2): #Default Query as COVID-19 and Default Result Size as 2
    body = {
        "from":0,
        "size":Result_size,
        "query": {
            "match": {
                "meshMajor":Query
            }
        }
    }

    results = es.search(index="bioasq_task_9a", body=body)
    return(results)
```


```python
Elastic_ser('SARS-CoV-2',1)
```




    {'took': 4,
     'timed_out': False,
     '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0},
     'hits': {'total': {'value': 2630, 'relation': 'eq'},
      'max_score': 5.066309,
      'hits': [{'_index': 'bioasq_task_9a',
        '_type': '_doc',
        '_id': '2044',
        '_score': 5.066309,
        '_source': {'abstractText': 'OBJECTIVE: To analyze the clinical manifestations of heart, liver and kidney damages in the early stage of COVID-19 to identify the indicators for these damages.METHODS: We analyzed the clinical features, underlying diseases, and indicators of infection in 12 patients with COVID-19 on the second day after their admission to our hospital between January 20 and February 20, 2020.The data including CK-MB, aTnI, BNP, heart rate, changes in ECG, LVEF (%), left ventricular general longitudinal strain (GLS, measured by color Doppler ultrasound) were collected.The changes of liver function biochemical indicators were dynamically reviewed.BUN, UCR, eGFR, Ccr, and UACR and the levels of MA, A1M, IGU, and TRU were recorded.RESULTS: The 12 patients included 2 severe cases, 8 common type cases, and 2 mild cases.Four of the patients presented with sinus tachycardia, ECG changes and abnormal GLS in spite of normal aTNI and LVEF; 1 patient had abnormal CKMB and BNP.On the first and third days following admission, the patients had normal ALT, AST and GGT levels.On day 7, hepatic function damage occurred in the severe cases, manifested by elevated ALT and AST levels.Abnormalities of eGFR, Ccr and UACR occurred in 8, 5 and 5 of the patients, respectively.Abnormal elevations of MA, A1M, IGU and TRU in urine protein were observed in 4, 4, 5, and 2 of the patients, respectively.CONCLUSIONS: In patients with COVID-19, heart damage can be identified early by observing the GLS and new abnormalities on ECG in spite of normal aTNI and LVEF.Early liver injury is not obvious in these patients, but dynamic monitoring of the indicators of should be emplemented, especially in severe cases. In cases with normal CR and BUN, kidney damage can be detected early by calculating eGFR, Ccr and UACR and urine protein tests.',
         'journal': 'Nan fang yi ke da xue xue bao = Journal of Southern Medical University',
         'meshMajor': ['Betacoronavirus',
          'COVID-19',
          'Coronavirus Infections',
          'Humans',
          'Pandemics',
          'Pneumonia, Viral',
          'SARS-CoV-2'],
         'pmid': '33118517',
         'title': '[Clinical analysis of early damage in multiple extra-pulmonary organs in COVID-19].',
         'year': '2020'}}]}}



### Combining queries

#### must, must_not and should



```python
 
body = {
    "from":0,
    "size":1, #change this inorder to increase the result size
    "query": {
        "bool": {
            "must_not": {
                  "match": {
                    "meshMajor":"COVID-19"
                           }
                         },
            "should": {
                "match": {
                    "meshMajor": "Betacoronavirus"
                          }
                      },
            "must": {
                "match": {
                    "meshMajor": "Pneumonia, Viral"
                         },
                    }
                 }
               } 
            }

res = es.search(index="bioasq_task_9a", body=body)
res
```




    {'took': 4,
     'timed_out': False,
     '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0},
     'hits': {'total': {'value': 160, 'relation': 'eq'},
      'max_score': 3.3100905,
      'hits': [{'_index': 'bioasq_task_9a',
        '_type': '_doc',
        '_id': '9700',
        '_score': 3.3100905,
        '_source': {'abstractText': "BACKGROUND: This longitudinal study aimed to examine the changes in psychological distress of the general public from the early to community-transmission phases of the COVID-19 pandemic and to investigate the factors related to these changes.METHODS: An internet-based survey of 2,400 Japanese people was conducted in two phases: early phase (baseline survey: February 25-27, 2020) and community-transmission phase (follow-up survey: April 1-6, 2020). The presence of severe psychological distress (SPD) was measured using the Kessler's Six-scale Psychological Distress Scale. The difference of SPD percentages between the two phases was examined. Mixed-effects ordinal logistic regression analysis was performed to assess the factors associated with the change of SPD status between the two phases.RESULTS: Surveys for both phases had 2,078 valid respondents (49.3% men; average age, 50.3 years). In the two surveys, individuals with SPD were 9.3% and 11.3%, respectively, demonstrating a significant increase between the two phases (P = 0.005). Significantly higher likelihood to develop SPD were observed among those in lower (ie, 18,600-37,200 United States dollars [USD], odds ratio [OR] 1.95; 95% confidence interval [CI], 1.10-3.46) and the lowest income category (ie, <18,600 USD, OR 2.12; 95% CI, 1.16-3.86). Furthermore, those with respiratory diseases were more likely to develop SPD (OR 2.56; 95% CI, 1.51-4.34).CONCLUSIONS: From the early to community-transmission phases of COVID-19, psychological distress increased among the Japanese. Recommendations include implementing mental health measures together with protective measures against COVID-19 infection, prioritizing low-income people and those with underlying diseases.",
         'journal': 'Journal of epidemiology',
         'meshMajor': ['Adolescent',
          'Adult',
          'Aged',
          'Betacoronavirus',
          'Coronavirus Infections',
          'Depression',
          'Female',
          'Health Surveys',
          'Humans',
          'Japan',
          'Longitudinal Studies',
          'Male',
          'Middle Aged',
          'Pandemics',
          'Pneumonia, Viral',
          'Psychological Distress',
          'SARS-CoV-2',
          'Stress, Psychological',
          'Young Adult'],
         'pmid': '32963212',
         'title': 'Changes in Psychological Distress During the COVID-19 Pandemic in Japan: A Longitudinal Study.',
         'year': '2020'}}]}}

More comming Soon


```python

```
