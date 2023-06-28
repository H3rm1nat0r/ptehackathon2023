# pTE Hackathon

Welcome to the Hackathon. The challenge is to predict part consumption better than NEMO. To overcome the first steps, we provide you with a "Framework" that connects our database and has some SQL statements to load parts and material movements as a base for predictions.

# setup

1. Please follow the technical instructions in pAWorld (https://paworld.proalpha.com/pages/viewpage.action?pageId=321333399#pTEHackathon–werfteinenBlickhinterdieKulissenunseresCloudproduktsNEMO-TechnischeInformationen)
1. Clone this repository into your visual studio 
1. create a venv environment (use python 3.11), install all dependecies from "requirements.txt" and activate this environment
1. edit config.ini file and insert database connection credentials
1. do a test run by
``` 
python pteframework.py
```

# Framework

Code is the best documentation. Please search the MAIN-Block at the end of the framework class and see what's happening there. You should implement code in the "do_the_magic_stuff" method.

Quick-Tip:
search for this code and you'll immediately get into the place, where you have to perform

```python

            ##################################
            # magic forecasting algorithm here
            forecast = [random.randint(0, 5) for _ in range(90)]
            ##################################
```

You can run your code with 
```
python pteframework.py
```

You can configure the parts to be predicted in the parts section of config.ini file. You should not change the Connect-section nor the timeline-section
