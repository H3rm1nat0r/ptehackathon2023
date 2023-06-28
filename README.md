# pTE Hackathon

Welcome to the Hackathon. The challenge is to predict part consumption better than NEMO. To overcome the first steps, we provide you with a "Framework" that connects our database and has some SQL statements to load parts and material movements as a base for predictions.

# setup

Please follow the technical instructions in pAWorld (https://paworld.proalpha.com/pages/viewpage.action?pageId=321333399#pTEHackathon–werfteinenBlickhinterdieKulissenunseresCloudproduktsNEMO-TechnischeInformationen)

In Visual Studio then create a venv environment and install all packages from requirements.txt

You also need a config.ini file and the "framework" "pteframework.py" from pAWorld. Download these files into your visual studio project

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
